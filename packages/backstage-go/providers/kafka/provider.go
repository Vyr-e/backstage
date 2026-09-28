package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/vyr-e/backstage/packages/backstage-go"
)

type Config struct {
	Brokers       []string
	Prefix        string
	MaxDeliveries int
}

type Provider struct {
	name          string
	brokers       []string
	prefix        string
	maxDeliveries  int
	ctx           *backstage.ProviderContext
	jobs          *jobsCap
	topics        *topicsCap
	writer        *kafka.Writer
	closed        atomic.Bool
	mu            sync.Mutex
}

func New(cfg Config) *Provider {
	brokers := cfg.Brokers
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "backstage"
	}
	maxD := cfg.MaxDeliveries
	if maxD == 0 {
		maxD = 5
	}
	p := &Provider{name: "kafka", brokers: brokers, prefix: prefix, maxDeliveries: maxD}
	p.jobs = &jobsCap{p: p}
	p.topics = &topicsCap{p: p}
	return p
}

func (p *Provider) Name() string             { return p.name }
func (p *Provider) Jobs() backstage.Jobs     { return p.jobs }
func (p *Provider) Topics() backstage.Topics { return p.topics }
func (p *Provider) Delays() backstage.Delays { return nil }
func (p *Provider) Dedupe() backstage.Dedupe { return nil }

func (p *Provider) Init(ctx context.Context, pctx backstage.ProviderContext) error {
	p.ctx = &pctx
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writer = &kafka.Writer{
		Addr:         kafka.TCP(p.brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	return nil
}

func (p *Provider) Close() error {
	p.closed.Store(true)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writer != nil {
		err := p.writer.Close()
		p.writer = nil
		return err
	}
	return nil
}

func (p *Provider) queueTopic(q string) string { return p.prefix + "." + q }
func (p *Provider) dlqTopic(q string) string   { return p.prefix + "." + q + ".dead-letter" }

func (p *Provider) getWriter() *kafka.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writer == nil {
		p.writer = &kafka.Writer{
			Addr:         kafka.TCP(p.brokers...),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		}
	}
	return p.writer
}

type jobsCap struct{ p *Provider }

func (j *jobsCap) Name() string { return "kafka" }
func (j *jobsCap) Requires() []backstage.CapabilityName {
	return []backstage.CapabilityName{backstage.CapabilityDelays}
}
func (j *jobsCap) EnsureQueues(ctx context.Context, queues []string) error {
	conn, err := kafka.Dial("tcp", j.p.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cconn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer cconn.Close()
	var topics []kafka.TopicConfig
	for _, q := range queues {
		topics = append(topics,
			kafka.TopicConfig{Topic: j.p.queueTopic(q), NumPartitions: 1, ReplicationFactor: 1},
			kafka.TopicConfig{Topic: j.p.dlqTopic(q), NumPartitions: 1, ReplicationFactor: 1},
		)
	}
	return cconn.CreateTopics(topics...)
}

type wireJob struct {
	TaskName      string            `json:"taskName"`
	Payload       json.RawMessage   `json:"payload"`
	EnqueuedAt    int64             `json:"enqueuedAt"`
	Meta          backstage.JobMeta `json:"meta"`
	DeliveryCount int               `json:"deliveryCount"`
}

func (j *jobsCap) Publish(ctx context.Context, job backstage.OutgoingJob) (string, error) {
	payload, _ := json.Marshal(job.Payload)
	body, _ := json.Marshal(wireJob{
		TaskName: job.TaskName, Payload: payload, EnqueuedAt: job.EnqueuedAt,
		Meta: job.Meta, DeliveryCount: max(1, job.DeliveryCount),
	})
	err := j.p.getWriter().WriteMessages(ctx, kafka.Message{Topic: j.p.queueTopic(job.Queue), Value: body})
	return fmt.Sprintf("kafka-%d", job.EnqueuedAt), err
}

func (j *jobsCap) Consume(ctx context.Context, opts backstage.ConsumeOptions, onDelivery func(context.Context, backstage.JobDelivery) error) (backstage.Subscription, error) {
	stopCtx, cancel := context.WithCancel(ctx)
	var running atomic.Bool
	running.Store(true)
	prefetch := opts.Prefetch
	if prefetch <= 0 {
		prefetch = 1
	}
	var wg sync.WaitGroup
	for _, q := range opts.Queues {
		wg.Add(1)
		queue := q
		go func() {
			defer wg.Done()
			j.consumeQueue(stopCtx, &running, queue, opts, prefetch, onDelivery)
		}()
	}
	return &sub{stop: func() {
		running.Store(false)
		cancel()
		wg.Wait()
	}}, nil
}

func (j *jobsCap) consumeQueue(ctx context.Context, running *atomic.Bool, queue string, opts backstage.ConsumeOptions, prefetch int, onDelivery func(context.Context, backstage.JobDelivery) error) {
	backoff := time.Second
	for running.Load() && !j.p.closed.Load() {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: j.p.brokers, GroupID: opts.Group, Topic: j.p.queueTopic(queue),
			MinBytes: 1, MaxBytes: 10e6, StartOffset: kafka.FirstOffset,
			MaxWait: 500 * time.Millisecond,
		})
		trackers := map[int]*contiguousTracker{}
		sem := make(chan struct{}, prefetch)
		var inFlight sync.WaitGroup
		errCh := make(chan error, 1)

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			for running.Load() {
				m, err := r.FetchMessage(ctx)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				tr := trackers[m.Partition]
				if tr == nil {
					tr = &contiguousTracker{}
					trackers[m.Partition] = tr
				}
				off := m.Offset
				tr.markUnsettled(off)
				var body wireJob
				_ = json.Unmarshal(m.Value, &body)
				d := &kDelivery{
					r: r, m: m, queue: queue, body: body, count: max(1, body.DeliveryCount),
					tr: tr, p: j.p,
				}
				sem <- struct{}{}
				inFlight.Add(1)
				go func(delivery backstage.JobDelivery) {
					defer func() { <-sem; inFlight.Done() }()
					_ = onDelivery(ctx, delivery)
				}(d)
			}
		}()

		select {
		case <-ctx.Done():
			_ = r.Close()
			<-readDone
			inFlight.Wait()
			return
		case err := <-errCh:
			_ = r.Close()
			<-readDone
			inFlight.Wait()
			if !running.Load() || j.p.closed.Load() {
				return
			}
			if j.p.ctx != nil && j.p.ctx.Logger != nil {
				j.p.ctx.Logger.Warn("kafka consumer disconnected; reconnecting", "queue", queue, "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
	}
}

type kDelivery struct {
	r     *kafka.Reader
	m     kafka.Message
	queue string
	body  wireJob
	count int
	tr    *contiguousTracker
	p     *Provider
	once  sync.Once
}

func (d *kDelivery) settle(ctx context.Context) error {
	var err error
	d.once.Do(func() {
		d.tr.markSettled(d.m.Offset)
		commitTo := d.tr.contiguous()
		if commitTo >= 0 {
			msg := d.m
			msg.Offset = commitTo
			err = d.r.CommitMessages(ctx, msg)
		}
	})
	return err
}

func (d *kDelivery) ID() string               { return fmt.Sprintf("%s:%d:%d", d.m.Topic, d.m.Partition, d.m.Offset) }
func (d *kDelivery) Queue() string            { return d.queue }
func (d *kDelivery) TaskName() string         { return d.body.TaskName }
func (d *kDelivery) Payload() json.RawMessage { return d.body.Payload }
func (d *kDelivery) EnqueuedAt() int64        { return d.body.EnqueuedAt }
func (d *kDelivery) DeliveryCount() int       { return d.count }
func (d *kDelivery) Meta() backstage.JobMeta  { return d.body.Meta }
func (d *kDelivery) Ack(ctx context.Context) error { return d.settle(ctx) }
func (d *kDelivery) Retry(ctx context.Context, opts backstage.RetryOpts) error {
	delays := d.p.ctx.Capabilities.Delays
	if delays == nil {
		return fmt.Errorf("kafka retry requires delays")
	}
	_, err := delays.Schedule(ctx, backstage.OutgoingJob{
		Queue: d.queue, TaskName: d.body.TaskName, Payload: d.body.Payload,
		EnqueuedAt: d.body.EnqueuedAt, Meta: d.body.Meta, DeliveryCount: d.count + 1,
	}, time.Now().UnixMilli()+opts.DelayMs)
	if err != nil {
		return err
	}
	return d.settle(ctx)
}
func (d *kDelivery) DeadLetter(ctx context.Context, opts backstage.DeadLetterOpts) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"taskName": d.body.TaskName, "payload": d.body.Payload, "error": opts.Error,
		"originalId": d.ID(), "deliveryCount": d.count, "deadLetteredAt": time.Now().UnixMilli(),
	})
	if err := d.p.getWriter().WriteMessages(ctx, kafka.Message{Topic: d.p.dlqTopic(d.queue), Value: payload}); err != nil {
		return err
	}
	return d.settle(ctx)
}

type topicsCap struct{ p *Provider }

func (t *topicsCap) Name() string { return "kafka" }
func (t *topicsCap) Publish(ctx context.Context, topic string, payload interface{}) (string, error) {
	body, _ := json.Marshal(map[string]interface{}{"payload": payload, "publishedAt": time.Now().UnixMilli(), "deliveryCount": 1})
	err := t.p.getWriter().WriteMessages(ctx, kafka.Message{Topic: t.p.prefix + ".topic." + topic, Value: body})
	return strconv.FormatInt(time.Now().UnixMilli(), 10), err
}
func (t *topicsCap) Subscribe(ctx context.Context, opts backstage.TopicSubscribeOptions, onMessage func(context.Context, backstage.TopicDelivery) error) (backstage.Subscription, error) {
	stopCtx, cancel := context.WithCancel(ctx)
	var running atomic.Bool
	running.Store(true)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.subscribeLoop(stopCtx, &running, opts, onMessage)
	}()
	return &sub{stop: func() {
		running.Store(false)
		cancel()
		wg.Wait()
	}}, nil
}

func (t *topicsCap) subscribeLoop(ctx context.Context, running *atomic.Bool, opts backstage.TopicSubscribeOptions, onMessage func(context.Context, backstage.TopicDelivery) error) {
	group := t.p.prefix + ".sub." + opts.ConsumerID
	if opts.Group != "" {
		group = t.p.prefix + ".grp." + opts.Group
	}
	start := kafka.LastOffset
	if opts.From == backstage.TopicFromEarliest {
		start = kafka.FirstOffset
	}
	topic := t.p.prefix + ".topic." + opts.Topic
	backoff := time.Second
	for running.Load() && !t.p.closed.Load() {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: t.p.brokers, GroupID: group, Topic: topic,
			StartOffset: start, MinBytes: 1, MaxBytes: 10e6, MaxWait: 500 * time.Millisecond,
		})
		for running.Load() {
			m, err := r.FetchMessage(ctx)
			if err != nil {
				_ = r.Close()
				if !running.Load() || t.p.closed.Load() {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
				break
			}
			backoff = time.Second
			var body map[string]json.RawMessage
			_ = json.Unmarshal(m.Value, &body)
			var publishedAt int64
			_ = json.Unmarshal(body["publishedAt"], &publishedAt)
			count := 1
			if raw, ok := body["deliveryCount"]; ok {
				_ = json.Unmarshal(raw, &count)
			}
			td := &topicDel{id: strconv.FormatInt(m.Offset, 10), topic: opts.Topic, payload: body["payload"], publishedAt: publishedAt, count: count}
			if err := onMessage(ctx, td); err != nil {
				if count >= t.p.maxDeliveries {
					if t.p.ctx != nil && t.p.ctx.Logger != nil {
						t.p.ctx.Logger.Error("Topic handler failed after max deliveries; dropping",
							"topic", opts.Topic, "error", err)
					}
					_ = r.CommitMessages(ctx, m)
					continue
				}
				next, _ := json.Marshal(map[string]interface{}{
					"payload": json.RawMessage(body["payload"]), "publishedAt": publishedAt, "deliveryCount": count + 1,
				})
				if pubErr := t.p.getWriter().WriteMessages(ctx, kafka.Message{Topic: topic, Value: next}); pubErr != nil {
					// do not commit — redeliver after reconnect/rebalance
					continue
				}
				_ = r.CommitMessages(ctx, m)
				continue
			}
			_ = r.CommitMessages(ctx, m)
		}
	}
}

type topicDel struct {
	id, topic   string
	payload     json.RawMessage
	publishedAt int64
	count       int
}

func (t *topicDel) ID() string                       { return t.id }
func (t *topicDel) Topic() string                    { return t.topic }
func (t *topicDel) Payload() json.RawMessage         { return t.payload }
func (t *topicDel) PublishedAt() int64               { return t.publishedAt }
func (t *topicDel) DeliveryCount() int               { return t.count }
func (t *topicDel) Ack(ctx context.Context) error    { return nil }

type sub struct{ stop func() }

func (s *sub) Stop(ctx context.Context) error { s.stop(); return nil }

type contiguousTracker struct {
	unsettled map[int64]struct{}
	settled   map[int64]struct{}
	highest   int64
	mu        sync.Mutex
}

func (t *contiguousTracker) markUnsettled(off int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.unsettled == nil {
		t.unsettled = map[int64]struct{}{}
		t.settled = map[int64]struct{}{}
		t.highest = -1
	}
	t.unsettled[off] = struct{}{}
}
func (t *contiguousTracker) markSettled(off int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.unsettled, off)
	t.settled[off] = struct{}{}
	for {
		next := t.highest + 1
		if t.highest < 0 {
			minS := int64(-1)
			for s := range t.settled {
				if minS < 0 || s < minS {
					minS = s
				}
			}
			if minS < 0 {
				return
			}
			blocked := false
			for u := range t.unsettled {
				if u < minS {
					blocked = true
					break
				}
			}
			if blocked {
				return
			}
			t.highest = minS
			delete(t.settled, minS)
			continue
		}
		if _, ok := t.settled[next]; ok {
			t.highest = next
			delete(t.settled, next)
			continue
		}
		return
	}
}
func (t *contiguousTracker) contiguous() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.highest
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
