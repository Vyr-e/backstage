package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProviderConfig configures the Kafka transport.
type KafkaProviderConfig struct {
	Brokers  []string
	ClientID string
	Prefix   string
}

type kafkaPendingEntry struct {
	message   MessageRef
	msg       kafka.Message
	readerKey string
	claimedAt time.Time
}

type kafkaDelayedTask struct {
	id         string
	executeAt  int64
	queue      string
	taskName   string
	payload    interface{}
	attempts   int
	backoff    *BackoffConfig
	timeout    time.Duration
	enqueuedAt int64
}

// KafkaProvider implements Provider with topics per logical queue,
// consumer groups for work sharing, unique groups for broadcast,
// and an in-provider delayed table for scheduling.
type KafkaProvider struct {
	prefix    string
	brokers   []string
	clientID  string
	mu        sync.Mutex
	writers   map[string]*kafka.Writer
	readers   map[string]*kafka.Reader
	pending   map[string]*kafkaPendingEntry
	dedupe    map[string]int64
	delayed   []kafkaDelayedTask
	buffers   map[string][]MessageRef
	bcastBuf  map[string][]MessageRef
}

// NewKafkaProvider creates a Kafka provider.
func NewKafkaProvider(cfg KafkaProviderConfig) *KafkaProvider {
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = WirePrefix
	}
	brokers := cfg.Brokers
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = "backstage"
	}
	return &KafkaProvider{
		prefix:   prefix,
		brokers:  brokers,
		clientID: clientID,
		writers:  make(map[string]*kafka.Writer),
		readers:  make(map[string]*kafka.Reader),
		pending:  make(map[string]*kafkaPendingEntry),
		dedupe:   make(map[string]int64),
		buffers:  make(map[string][]MessageRef),
		bcastBuf: make(map[string][]MessageRef),
	}
}

func (p *KafkaProvider) Name() string { return "kafka" }
func (p *KafkaProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{
		Durable: true, Broadcast: true, Scheduling: true,
		Retries: true, Deduplication: true,
	}
}

func (p *KafkaProvider) topicFor(queue string) string {
	return fmt.Sprintf("%s.%s", p.prefix, queue)
}
func (p *KafkaProvider) dlqTopic(queue string) string {
	return fmt.Sprintf("%s.%s.dead-letter", p.prefix, queue)
}
func (p *KafkaProvider) broadcastTopic() string {
	return fmt.Sprintf("%s.broadcast", p.prefix)
}

func (p *KafkaProvider) writer(topic string) *kafka.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := p.writers[topic]; ok {
		return w
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(p.brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
	p.writers[topic] = w
	return w
}

func (p *KafkaProvider) EnsureQueues(ctx context.Context, queues []string) error {
	_ = ctx
	// Topics are auto-created on first produce when the broker allows it.
	for _, q := range queues {
		_ = p.writer(p.topicFor(q))
		_ = p.writer(p.dlqTopic(q))
	}
	return nil
}

func (p *KafkaProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.readers {
		_ = r.Close()
	}
	p.readers = make(map[string]*kafka.Reader)
	for _, w := range p.writers {
		_ = w.Close()
	}
	p.writers = make(map[string]*kafka.Writer)
	p.pending = make(map[string]*kafkaPendingEntry)
	p.buffers = make(map[string][]MessageRef)
	p.bcastBuf = make(map[string][]MessageRef)
	return nil
}

func (p *KafkaProvider) Publish(ctx context.Context, taskName string, payload interface{}, opts PublishOptions) (string, error) {
	if opts.Dedupe != nil {
		now := time.Now().UnixMilli()
		p.gcDedupe(now)
		p.mu.Lock()
		if exp, ok := p.dedupe[opts.Dedupe.Key]; ok && exp > now {
			p.mu.Unlock()
			return "", nil
		}
		ttl := opts.Dedupe.TTL
		if ttl == 0 {
			ttl = time.Hour
		}
		p.dedupe[opts.Dedupe.Key] = now + ttl.Milliseconds()
		p.mu.Unlock()
	}

	queue := opts.Queue
	if queue == "" {
		if opts.Priority != "" {
			queue = string(opts.Priority)
		} else {
			queue = string(PriorityDefault)
		}
	}
	enqueuedAt := time.Now().UnixMilli()
	id := fmt.Sprintf("%d-%s", enqueuedAt, randString(8))

	if opts.Delay > 0 {
		p.mu.Lock()
		p.delayed = append(p.delayed, kafkaDelayedTask{
			id: id, executeAt: enqueuedAt + opts.Delay.Milliseconds(),
			queue: queue, taskName: taskName, payload: payload,
			attempts: opts.Attempts, backoff: opts.Backoff,
			timeout: opts.Timeout, enqueuedAt: enqueuedAt,
		})
		p.mu.Unlock()
		return fmt.Sprintf("scheduled:%d", enqueuedAt+opts.Delay.Milliseconds()), nil
	}

	body, err := json.Marshal(map[string]interface{}{
		"taskName":   taskName,
		"payload":    payload,
		"enqueuedAt": enqueuedAt,
		"attempts":   opts.Attempts,
		"backoff":    opts.Backoff,
		"timeout":    durationMs(opts.Timeout),
	})
	if err != nil {
		return "", err
	}

	w := p.writer(p.topicFor(queue))
	if err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(taskName),
		Value: body,
		Headers: []kafka.Header{
			{Key: "messageId", Value: []byte(id)},
			{Key: "deliveryCount", Value: []byte("1")},
		},
	}); err != nil {
		return "", err
	}
	return id, nil
}

func (p *KafkaProvider) bufferKey(group, consumerID string) string {
	return group + "::" + consumerID
}

func (p *KafkaProvider) ensureReader(args ConsumeArgs) error {
	key := p.bufferKey(args.ConsumerGroup, args.ConsumerID)
	p.mu.Lock()
	if _, ok := p.readers[key]; ok {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	topics := make([]string, len(args.Queues))
	for i, q := range args.Queues {
		topics[i] = p.topicFor(q)
	}

	// One reader per topic when multiple queues; fan into shared buffer via goroutines.
	for _, topic := range topics {
		rKey := key + "::" + topic
		p.mu.Lock()
		if _, ok := p.readers[rKey]; ok {
			p.mu.Unlock()
			continue
		}
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        p.brokers,
			GroupID:        args.ConsumerGroup,
			Topic:          topic,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0, // manual commit
			StartOffset:    kafka.LastOffset,
		})
		p.readers[rKey] = r
		p.mu.Unlock()

		go p.readLoop(rKey, key, r, topic)
	}

	// Marker so ensureReader is considered done for this group/consumer.
	p.mu.Lock()
	p.readers[key] = nil
	if _, ok := p.buffers[key]; !ok {
		p.buffers[key] = nil
	}
	p.mu.Unlock()
	return nil
}

func (p *KafkaProvider) readLoop(rKey, bufKey string, r *kafka.Reader, topic string) {
	for {
		msg, err := r.FetchMessage(context.Background())
		if err != nil {
			// Reader closed or fatal — exit.
			return
		}
		ref := p.kafkaMsgToRef(msg, topic)
		p.mu.Lock()
		// Bail if reader was removed (Close).
		if _, ok := p.readers[rKey]; !ok {
			p.mu.Unlock()
			return
		}
		p.pending[ref.ID] = &kafkaPendingEntry{
			message: ref, msg: msg, readerKey: rKey, claimedAt: time.Now(),
		}
		p.buffers[bufKey] = append(p.buffers[bufKey], ref)
		p.mu.Unlock()
	}
}

func (p *KafkaProvider) Consume(ctx context.Context, args ConsumeArgs) ([]MessageRef, error) {
	if err := p.ensureReader(args); err != nil {
		return nil, err
	}
	key := p.bufferKey(args.ConsumerGroup, args.ConsumerID)
	deadline := time.Now().Add(time.Duration(args.BlockMs) * time.Millisecond)
	max := int(args.MaxMessages)
	if max <= 0 {
		max = 1
	}

	for {
		p.mu.Lock()
		buf := p.buffers[key]
		if len(buf) > 0 {
			n := max
			if n > len(buf) {
				n = len(buf)
			}
			out := append([]MessageRef{}, buf[:n]...)
			p.buffers[key] = buf[n:]
			p.mu.Unlock()
			return out, nil
		}
		p.mu.Unlock()

		if args.BlockMs <= 0 || time.Now().After(deadline) {
			return nil, nil
		}
		sleep := time.Until(deadline)
		if sleep > 25*time.Millisecond {
			sleep = 25 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleep):
		}
	}
}

func (p *KafkaProvider) Ack(ctx context.Context, messages []MessageRef) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range messages {
		entry, ok := p.pending[m.ID]
		if !ok {
			continue
		}
		r := p.readers[entry.readerKey]
		delete(p.pending, m.ID)
		if r != nil {
			_ = r.CommitMessages(ctx, entry.msg)
		}
	}
	return nil
}

func (p *KafkaProvider) AckAndForget(ctx context.Context, messages []MessageRef) error {
	return p.Ack(ctx, messages)
}

func (p *KafkaProvider) ReclaimIdle(ctx context.Context, args ReclaimIdleArgs) ([]MessageRef, error) {
	now := time.Now()
	maxCount := int(args.MaxCount)
	if maxCount <= 0 {
		maxCount = 10
	}
	idleThreshold := time.Duration(args.IdleMs) * time.Millisecond
	queueSet := make(map[string]struct{}, len(args.Queues))
	for _, q := range args.Queues {
		queueSet[q] = struct{}{}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	var claimed []MessageRef
	for _, entry := range p.pending {
		if len(claimed) >= maxCount {
			break
		}
		if _, ok := queueSet[entry.message.Queue]; !ok {
			continue
		}
		idle := now.Sub(entry.claimedAt)
		if idle < idleThreshold {
			continue
		}
		if entry.message.Backoff != nil {
			wait := time.Duration(calculateBackoff(*entry.message.Backoff, entry.message.DeliveryCount)) * time.Millisecond
			if idle < wait {
				continue
			}
		}
		entry.message.DeliveryCount++
		entry.claimedAt = now
		claimed = append(claimed, entry.message)
	}
	return claimed, nil
}

func (p *KafkaProvider) DeadLetter(ctx context.Context, message MessageRef, meta DeadLetterMeta) error {
	body, _ := json.Marshal(map[string]interface{}{
		"taskName":       message.TaskName,
		"payload":        json.RawMessage(message.Payload),
		"enqueuedAt":     message.EnqueuedAt,
		"originalId":     meta.OriginalID,
		"deliveryCount":  meta.DeliveryCount,
		"deadLetteredAt": time.Now().UnixMilli(),
		"error":          meta.Error,
	})
	w := p.writer(p.dlqTopic(message.Queue))
	if err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(message.TaskName),
		Value: body,
	}); err != nil {
		return err
	}
	return p.Ack(ctx, []MessageRef{message})
}

func (p *KafkaProvider) PromoteDueScheduled(ctx context.Context, nowMs int64) (int64, error) {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	p.mu.Lock()
	var due []kafkaDelayedTask
	var remaining []kafkaDelayedTask
	for _, t := range p.delayed {
		if t.executeAt <= nowMs {
			due = append(due, t)
		} else {
			remaining = append(remaining, t)
		}
	}
	p.delayed = remaining
	p.mu.Unlock()

	for _, t := range due {
		_, err := p.Publish(ctx, t.taskName, t.payload, PublishOptions{
			Queue:    t.queue,
			Attempts: t.attempts,
			Backoff:  t.backoff,
			Timeout:  t.timeout,
		})
		if err != nil {
			return int64(len(due)), err
		}
	}
	return int64(len(due)), nil
}

func (p *KafkaProvider) EnsureBroadcast(ctx context.Context, consumerIdentity string, start BroadcastStart) error {
	groupID := wireBroadcastGroup(consumerIdentity)
	p.mu.Lock()
	if _, ok := p.readers[groupID]; ok {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	offset := kafka.LastOffset
	if start == BroadcastStartBeginning {
		offset = kafka.FirstOffset
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        p.brokers,
		GroupID:        groupID,
		Topic:          p.broadcastTopic(),
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
		StartOffset:    offset,
	})

	p.mu.Lock()
	p.readers[groupID] = r
	p.bcastBuf[consumerIdentity] = nil
	p.mu.Unlock()

	go p.broadcastReadLoop(groupID, consumerIdentity, r)
	return nil
}

func (p *KafkaProvider) broadcastReadLoop(groupID, consumerIdentity string, r *kafka.Reader) {
	for {
		msg, err := r.FetchMessage(context.Background())
		if err != nil {
			return
		}
		ref := p.kafkaMsgToRef(msg, p.broadcastTopic())
		ref.Queue = "broadcast"
		p.mu.Lock()
		if _, ok := p.readers[groupID]; !ok {
			p.mu.Unlock()
			return
		}
		p.pending[ref.ID] = &kafkaPendingEntry{
			message: ref, msg: msg, readerKey: groupID, claimedAt: time.Now(),
		}
		p.bcastBuf[consumerIdentity] = append(p.bcastBuf[consumerIdentity], ref)
		p.mu.Unlock()
	}
}

func (p *KafkaProvider) Broadcast(ctx context.Context, taskName string, payload interface{}) (string, error) {
	id := fmt.Sprintf("%d-%s", time.Now().UnixMilli(), randString(8))
	body, _ := json.Marshal(map[string]interface{}{
		"taskName":   taskName,
		"payload":    payload,
		"enqueuedAt": time.Now().UnixMilli(),
	})
	w := p.writer(p.broadcastTopic())
	if err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(taskName),
		Value: body,
		Headers: []kafka.Header{
			{Key: "messageId", Value: []byte(id)},
		},
	}); err != nil {
		return "", err
	}
	return id, nil
}

func (p *KafkaProvider) ConsumeBroadcast(ctx context.Context, consumerIdentity string, maxMessages int64, blockMs int64) ([]MessageRef, error) {
	if err := p.EnsureBroadcast(ctx, consumerIdentity, BroadcastStartLatest); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(time.Duration(blockMs) * time.Millisecond)
	max := int(maxMessages)
	if max <= 0 {
		max = 1
	}

	for {
		p.mu.Lock()
		buf := p.bcastBuf[consumerIdentity]
		if len(buf) > 0 {
			n := max
			if n > len(buf) {
				n = len(buf)
			}
			out := append([]MessageRef{}, buf[:n]...)
			p.bcastBuf[consumerIdentity] = buf[n:]
			p.mu.Unlock()
			return out, nil
		}
		p.mu.Unlock()

		if blockMs <= 0 || time.Now().After(deadline) {
			return nil, nil
		}
		sleep := time.Until(deadline)
		if sleep > 25*time.Millisecond {
			sleep = 25 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleep):
		}
	}
}

func (p *KafkaProvider) AckBroadcast(ctx context.Context, consumerIdentity string, ids []string) error {
	_ = consumerIdentity
	msgs := make([]MessageRef, len(ids))
	for i, id := range ids {
		msgs[i] = MessageRef{ID: id, Queue: "broadcast", DeliveryCount: 1}
	}
	return p.Ack(ctx, msgs)
}

func (p *KafkaProvider) CleanupBroadcastGhosts(ctx context.Context, idleMs int64) (int64, error) {
	return 0, nil
}

func (p *KafkaProvider) kafkaMsgToRef(msg kafka.Message, topic string) MessageRef {
	var parsed struct {
		TaskName   string          `json:"taskName"`
		Payload    json.RawMessage `json:"payload"`
		EnqueuedAt int64           `json:"enqueuedAt"`
		Attempts   int             `json:"attempts"`
		Backoff    *BackoffConfig  `json:"backoff"`
		Timeout    int64           `json:"timeout"`
	}
	_ = json.Unmarshal(msg.Value, &parsed)

	id := ""
	deliveryCount := 1
	for _, h := range msg.Headers {
		switch h.Key {
		case "messageId":
			id = string(h.Value)
		case "deliveryCount":
			fmt.Sscanf(string(h.Value), "%d", &deliveryCount)
		}
	}
	if id == "" {
		id = fmt.Sprintf("%d-%d", msg.Partition, msg.Offset)
	}

	queue := topic
	prefix := p.prefix + "."
	if strings.HasPrefix(topic, prefix) {
		queue = strings.TrimPrefix(topic, prefix)
		queue = strings.TrimSuffix(queue, ".dead-letter")
	}

	return MessageRef{
		ID: id, Queue: queue, TaskName: parsed.TaskName,
		Payload: parsed.Payload, EnqueuedAt: parsed.EnqueuedAt,
		DeliveryCount: deliveryCount, Attempts: parsed.Attempts,
		Backoff: parsed.Backoff, Timeout: parsed.Timeout,
	}
}

func (p *KafkaProvider) gcDedupe(now int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, exp := range p.dedupe {
		if exp <= now {
			delete(p.dedupe, k)
		}
	}
}
