package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQProviderConfig configures the RabbitMQ transport.
type RabbitMQProviderConfig struct {
	URL      string
	Hostname string
	Port     int
	Username string
	Password string
	VHost    string
	Prefetch int
	Prefix   string
}

type rabbitPendingEntry struct {
	message   MessageRef
	delivery  amqp.Delivery
	claimedAt time.Time
}

// RabbitMQProvider implements Provider with durable queues, TTL+DLX delays,
// fanout broadcast, and in-process pending for reclaim.
type RabbitMQProvider struct {
	config   RabbitMQProviderConfig
	prefix   string
	conn     *amqp.Connection
	channel  *amqp.Channel
	mu       sync.Mutex
	pending  map[string]*rabbitPendingEntry
	dedupe   map[string]int64
	bcastQ   map[string]string
	ensured  map[string]struct{}
}

// NewRabbitMQProvider creates a RabbitMQ provider (lazy connect on first use).
func NewRabbitMQProvider(cfg RabbitMQProviderConfig) *RabbitMQProvider {
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = WirePrefix
	}
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = 50
	}
	return &RabbitMQProvider{
		config:  cfg,
		prefix:  prefix,
		pending: make(map[string]*rabbitPendingEntry),
		dedupe:  make(map[string]int64),
		bcastQ:  make(map[string]string),
		ensured: make(map[string]struct{}),
	}
}

func (p *RabbitMQProvider) Name() string { return "rabbitmq" }
func (p *RabbitMQProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{
		Durable: true, Broadcast: true, Scheduling: true,
		Retries: true, Deduplication: true,
	}
}

func (p *RabbitMQProvider) workQueue(queue string) string {
	return fmt.Sprintf("%s.%s", p.prefix, queue)
}
func (p *RabbitMQProvider) delayedQueue(queue string) string {
	return fmt.Sprintf("%s.%s.delayed", p.prefix, queue)
}
func (p *RabbitMQProvider) dlqName(queue string) string {
	return fmt.Sprintf("%s.%s.dead-letter", p.prefix, queue)
}
func (p *RabbitMQProvider) broadcastExchange() string {
	return fmt.Sprintf("%s.broadcast", p.prefix)
}

func (p *RabbitMQProvider) connect() (*amqp.Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.channel != nil && !p.channel.IsClosed() {
		return p.channel, nil
	}
	url := p.config.URL
	if url == "" {
		user := p.config.Username
		if user == "" {
			user = "guest"
		}
		pass := p.config.Password
		if pass == "" {
			pass = "guest"
		}
		host := p.config.Hostname
		if host == "" {
			host = "localhost"
		}
		port := p.config.Port
		if port == 0 {
			port = 5672
		}
		vhost := p.config.VHost
		if vhost == "" {
			vhost = "/"
		}
		url = fmt.Sprintf("amqp://%s:%s@%s:%d%s", user, pass, host, port, vhost)
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	if err := ch.Qos(p.config.Prefetch, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	p.conn = conn
	p.channel = ch
	return ch, nil
}

func (p *RabbitMQProvider) EnsureQueues(ctx context.Context, queues []string) error {
	ch, err := p.connect()
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, queue := range queues {
		if _, ok := p.ensured[queue]; ok {
			continue
		}
		work, delayed, dlq := p.workQueue(queue), p.delayedQueue(queue), p.dlqName(queue)
		if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
			return err
		}
		if _, err := ch.QueueDeclare(work, true, false, false, false, amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": dlq,
		}); err != nil {
			return err
		}
		if _, err := ch.QueueDeclare(delayed, true, false, false, false, amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": work,
		}); err != nil {
			return err
		}
		p.ensured[queue] = struct{}{}
	}
	return nil
}

func (p *RabbitMQProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.channel != nil {
		_ = p.channel.Close()
		p.channel = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
	p.pending = make(map[string]*rabbitPendingEntry)
	return nil
}

func (p *RabbitMQProvider) Publish(ctx context.Context, taskName string, payload interface{}, opts PublishOptions) (string, error) {
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
	if err := p.EnsureQueues(ctx, []string{queue}); err != nil {
		return "", err
	}
	ch, err := p.connect()
	if err != nil {
		return "", err
	}

	enqueuedAt := time.Now().UnixMilli()
	id := fmt.Sprintf("%d-%s", enqueuedAt, randString(8))
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

	headers := amqp.Table{
		"taskName":      taskName,
		"enqueuedAt":    enqueuedAt,
		"deliveryCount": int32(1),
	}
	if opts.Attempts > 0 {
		headers["attempts"] = int32(opts.Attempts)
	}
	if opts.Backoff != nil {
		b, _ := json.Marshal(opts.Backoff)
		headers["backoff"] = string(b)
	}
	if opts.Timeout > 0 {
		headers["timeout"] = opts.Timeout.Milliseconds()
	}

	pub := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		MessageId:    id,
		ContentType:  "application/json",
		Body:         body,
		Headers:      headers,
	}

	if opts.Delay > 0 {
		pub.Expiration = fmt.Sprintf("%d", opts.Delay.Milliseconds())
		if err := ch.PublishWithContext(ctx, "", p.delayedQueue(queue), false, false, pub); err != nil {
			return "", err
		}
		return fmt.Sprintf("scheduled:%d", enqueuedAt+opts.Delay.Milliseconds()), nil
	}

	if err := ch.PublishWithContext(ctx, "", p.workQueue(queue), false, false, pub); err != nil {
		return "", err
	}
	return id, nil
}

func (p *RabbitMQProvider) Consume(ctx context.Context, args ConsumeArgs) ([]MessageRef, error) {
	ch, err := p.connect()
	if err != nil {
		return nil, err
	}
	if err := p.EnsureQueues(ctx, args.Queues); err != nil {
		return nil, err
	}

	var out []MessageRef
	deadline := time.Now().Add(time.Duration(args.BlockMs) * time.Millisecond)
	max := int(args.MaxMessages)
	if max <= 0 {
		max = 1
	}

	for len(out) < max {
		got := false
		for _, queue := range args.Queues {
			if len(out) >= max {
				break
			}
			del, ok, err := ch.Get(p.workQueue(queue), false)
			if err != nil {
				return out, err
			}
			if !ok {
				continue
			}
			got = true
			ref := p.toMessageRef(queue, del)
			p.mu.Lock()
			p.pending[ref.ID] = &rabbitPendingEntry{message: ref, delivery: del, claimedAt: time.Now()}
			p.mu.Unlock()
			out = append(out, ref)
		}
		if got {
			continue
		}
		if args.BlockMs <= 0 || time.Now().After(deadline) {
			break
		}
		sleep := time.Until(deadline)
		if sleep > 50*time.Millisecond {
			sleep = 50 * time.Millisecond
		}
		if sleep > 0 {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			case <-time.After(sleep):
			}
		}
	}
	return out, nil
}

func (p *RabbitMQProvider) Ack(ctx context.Context, messages []MessageRef) error {
	ch, err := p.connect()
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range messages {
		if entry, ok := p.pending[m.ID]; ok {
			_ = ch.Ack(entry.delivery.DeliveryTag, false)
			delete(p.pending, m.ID)
		}
	}
	return nil
}

func (p *RabbitMQProvider) AckAndForget(ctx context.Context, messages []MessageRef) error {
	return p.Ack(ctx, messages)
}

func (p *RabbitMQProvider) ReclaimIdle(ctx context.Context, args ReclaimIdleArgs) ([]MessageRef, error) {
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

func (p *RabbitMQProvider) DeadLetter(ctx context.Context, message MessageRef, meta DeadLetterMeta) error {
	if err := p.EnsureQueues(ctx, []string{message.Queue}); err != nil {
		return err
	}
	ch, err := p.connect()
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]interface{}{
		"taskName":       message.TaskName,
		"payload":        json.RawMessage(message.Payload),
		"enqueuedAt":     message.EnqueuedAt,
		"originalId":     meta.OriginalID,
		"deliveryCount":  meta.DeliveryCount,
		"deadLetteredAt": time.Now().UnixMilli(),
		"error":          meta.Error,
	})
	if err := ch.PublishWithContext(ctx, "", p.dlqName(message.Queue), false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		MessageId:    "dlq-" + meta.OriginalID,
		ContentType:  "application/json",
		Body:         body,
	}); err != nil {
		return err
	}
	return p.Ack(ctx, []MessageRef{message})
}

func (p *RabbitMQProvider) PromoteDueScheduled(ctx context.Context, nowMs int64) (int64, error) {
	// Delays use per-message TTL + DLX; RabbitMQ promotes automatically.
	return 0, nil
}

func (p *RabbitMQProvider) EnsureBroadcast(ctx context.Context, consumerIdentity string, start BroadcastStart) error {
	_ = start
	ch, err := p.connect()
	if err != nil {
		return err
	}
	ex := p.broadcastExchange()
	if err := ch.ExchangeDeclare(ex, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	qName := fmt.Sprintf("%s.broadcast.%s", p.prefix, consumerIdentity)
	q, err := ch.QueueDeclare(qName, false, true, false, false, nil)
	if err != nil {
		return err
	}
	if err := ch.QueueBind(q.Name, "", ex, false, nil); err != nil {
		return err
	}
	p.mu.Lock()
	p.bcastQ[consumerIdentity] = q.Name
	p.mu.Unlock()
	return nil
}

func (p *RabbitMQProvider) Broadcast(ctx context.Context, taskName string, payload interface{}) (string, error) {
	ch, err := p.connect()
	if err != nil {
		return "", err
	}
	ex := p.broadcastExchange()
	if err := ch.ExchangeDeclare(ex, "fanout", true, false, false, false, nil); err != nil {
		return "", err
	}
	id := fmt.Sprintf("%d-%s", time.Now().UnixMilli(), randString(8))
	body, _ := json.Marshal(map[string]interface{}{
		"taskName":   taskName,
		"payload":    payload,
		"enqueuedAt": time.Now().UnixMilli(),
	})
	if err := ch.PublishWithContext(ctx, ex, "", false, false, amqp.Publishing{
		MessageId:   id,
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		return "", err
	}
	return id, nil
}

func (p *RabbitMQProvider) ConsumeBroadcast(ctx context.Context, consumerIdentity string, maxMessages int64, blockMs int64) ([]MessageRef, error) {
	p.mu.Lock()
	qName, ok := p.bcastQ[consumerIdentity]
	p.mu.Unlock()
	if !ok {
		if err := p.EnsureBroadcast(ctx, consumerIdentity, BroadcastStartLatest); err != nil {
			return nil, err
		}
		p.mu.Lock()
		qName = p.bcastQ[consumerIdentity]
		p.mu.Unlock()
	}
	ch, err := p.connect()
	if err != nil {
		return nil, err
	}

	var out []MessageRef
	deadline := time.Now().Add(time.Duration(blockMs) * time.Millisecond)
	max := int(maxMessages)
	if max <= 0 {
		max = 1
	}

	for len(out) < max {
		del, ok, err := ch.Get(qName, false)
		if err != nil {
			return out, err
		}
		if !ok {
			if blockMs <= 0 || time.Now().After(deadline) {
				break
			}
			sleep := time.Until(deadline)
			if sleep > 50*time.Millisecond {
				sleep = 50 * time.Millisecond
			}
			if sleep > 0 {
				select {
				case <-ctx.Done():
					return out, ctx.Err()
				case <-time.After(sleep):
				}
			}
			continue
		}
		var parsed struct {
			TaskName   string          `json:"taskName"`
			Payload    json.RawMessage `json:"payload"`
			EnqueuedAt int64           `json:"enqueuedAt"`
		}
		_ = json.Unmarshal(del.Body, &parsed)
		id := del.MessageId
		if id == "" {
			id = fmt.Sprintf("%d", time.Now().UnixMilli())
		}
		ref := MessageRef{
			ID: id, Queue: "broadcast", TaskName: parsed.TaskName,
			Payload: parsed.Payload, EnqueuedAt: parsed.EnqueuedAt, DeliveryCount: 1,
		}
		p.mu.Lock()
		p.pending[id] = &rabbitPendingEntry{message: ref, delivery: del, claimedAt: time.Now()}
		p.mu.Unlock()
		out = append(out, ref)
	}
	return out, nil
}

func (p *RabbitMQProvider) AckBroadcast(ctx context.Context, consumerIdentity string, ids []string) error {
	_ = consumerIdentity
	msgs := make([]MessageRef, len(ids))
	for i, id := range ids {
		msgs[i] = MessageRef{ID: id, Queue: "broadcast", DeliveryCount: 1}
	}
	return p.Ack(ctx, msgs)
}

func (p *RabbitMQProvider) CleanupBroadcastGhosts(ctx context.Context, idleMs int64) (int64, error) {
	return 0, nil
}

func (p *RabbitMQProvider) toMessageRef(queue string, del amqp.Delivery) MessageRef {
	var parsed struct {
		TaskName   string          `json:"taskName"`
		Payload    json.RawMessage `json:"payload"`
		EnqueuedAt int64           `json:"enqueuedAt"`
		Attempts   int             `json:"attempts"`
		Backoff    *BackoffConfig  `json:"backoff"`
		Timeout    int64           `json:"timeout"`
	}
	_ = json.Unmarshal(del.Body, &parsed)

	deliveryCount := 1
	if del.Headers != nil {
		if v, ok := del.Headers["deliveryCount"]; ok {
			switch n := v.(type) {
			case int32:
				deliveryCount = int(n)
			case int64:
				deliveryCount = int(n)
			case int:
				deliveryCount = n
			}
		}
	}
	if deliveryCount == 1 && del.Redelivered {
		deliveryCount = 2
	}

	backoff := parsed.Backoff
	if backoff == nil && del.Headers != nil {
		if s, ok := del.Headers["backoff"].(string); ok && s != "" {
			var b BackoffConfig
			if json.Unmarshal([]byte(s), &b) == nil {
				backoff = &b
			}
		}
	}

	id := del.MessageId
	if id == "" {
		id = fmt.Sprintf("%d-%s", time.Now().UnixMilli(), randString(6))
	}

	timeout := parsed.Timeout
	if timeout == 0 && del.Headers != nil {
		if v, ok := asInt64(del.Headers["timeout"]); ok {
			timeout = v
		}
	}

	return MessageRef{
		ID: id, Queue: queue, TaskName: parsed.TaskName,
		Payload: parsed.Payload, EnqueuedAt: parsed.EnqueuedAt,
		DeliveryCount: deliveryCount, Attempts: parsed.Attempts,
		Backoff: backoff, Timeout: timeout,
	}
}

func (p *RabbitMQProvider) gcDedupe(now int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, exp := range p.dedupe {
		if exp <= now {
			delete(p.dedupe, k)
		}
	}
}

func durationMs(d time.Duration) interface{} {
	if d <= 0 {
		return nil
	}
	return d.Milliseconds()
}

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
