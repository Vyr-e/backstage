package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/vyr-e/backstage/packages/backstage-go"
)

type Config struct {
	Brokers []string
	Prefix  string
}

type Provider struct {
	name    string
	brokers []string
	prefix  string
	ctx     *backstage.ProviderContext
	jobs    *jobsCap
	topics  *topicsCap
	writer  *kafka.Writer
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
	p := &Provider{name: "kafka", brokers: brokers, prefix: prefix}
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
	p.writer = &kafka.Writer{
		Addr:         kafka.TCP(p.brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	return nil
}

func (p *Provider) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}

func (p *Provider) queueTopic(q string) string { return p.prefix + "." + q }
func (p *Provider) dlqTopic(q string) string   { return p.prefix + "." + q + ".dead-letter" }

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
	err := j.p.writer.WriteMessages(ctx, kafka.Message{Topic: j.p.queueTopic(job.Queue), Value: body})
	return fmt.Sprintf("kafka-%d", job.EnqueuedAt), err
}

func (j *jobsCap) Consume(ctx context.Context, opts backstage.ConsumeOptions, onDelivery func(context.Context, backstage.JobDelivery) error) (backstage.Subscription, error) {
	readers := make([]*kafka.Reader, 0, len(opts.Queues))
	for _, q := range opts.Queues {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: j.p.brokers, GroupID: opts.Group, Topic: j.p.queueTopic(q),
			MinBytes: 1, MaxBytes: 10e6, StartOffset: kafka.FirstOffset,
		})
		readers = append(readers, r)
	}
	stopCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i, r := range readers {
		wg.Add(1)
		queue := opts.Queues[i]
		go func(r *kafka.Reader, queue string) {
			defer wg.Done()
			trackers := map[int]*contiguousTracker{}
			for {
				m, err := r.FetchMessage(stopCtx)
				if err != nil {
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
				_ = onDelivery(stopCtx, d)
			}
		}(r, queue)
	}
	return &sub{stop: func() {
		cancel()
		for _, r := range readers {
			_ = r.Close()
		}
		wg.Wait()
	}}, nil
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
	if err := d.p.writer.WriteMessages(ctx, kafka.Message{Topic: d.p.dlqTopic(d.queue), Value: payload}); err != nil {
		return err
	}
	return d.settle(ctx)
}

type topicsCap struct{ p *Provider }

func (t *topicsCap) Name() string { return "kafka" }
func (t *topicsCap) Publish(ctx context.Context, topic string, payload interface{}) (string, error) {
	body, _ := json.Marshal(map[string]interface{}{"payload": payload, "publishedAt": time.Now().UnixMilli()})
	err := t.p.writer.WriteMessages(ctx, kafka.Message{Topic: t.p.prefix + ".topic." + topic, Value: body})
	return strconv.FormatInt(time.Now().UnixMilli(), 10), err
}
func (t *topicsCap) Subscribe(ctx context.Context, opts backstage.TopicSubscribeOptions, onMessage func(context.Context, backstage.TopicDelivery) error) (backstage.Subscription, error) {
	group := t.p.prefix + ".sub." + opts.ConsumerID
	if opts.Group != "" {
		group = t.p.prefix + ".grp." + opts.Group
	}
	start := kafka.LastOffset
	if opts.From == backstage.TopicFromEarliest {
		start = kafka.FirstOffset
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: t.p.brokers, GroupID: group, Topic: t.p.prefix + ".topic." + opts.Topic,
		StartOffset: start, MinBytes: 1, MaxBytes: 10e6,
	})
	stopCtx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			m, err := r.ReadMessage(stopCtx)
			if err != nil {
				return
			}
			var body map[string]json.RawMessage
			_ = json.Unmarshal(m.Value, &body)
			var publishedAt int64
			_ = json.Unmarshal(body["publishedAt"], &publishedAt)
			td := &topicDel{id: strconv.FormatInt(m.Offset, 10), topic: opts.Topic, payload: body["payload"], publishedAt: publishedAt}
			_ = onMessage(stopCtx, td)
		}
	}()
	return &sub{stop: func() { cancel(); _ = r.Close() }}, nil
}

type topicDel struct {
	id, topic   string
	payload     json.RawMessage
	publishedAt int64
}

func (t *topicDel) ID() string                       { return t.id }
func (t *topicDel) Topic() string                    { return t.topic }
func (t *topicDel) Payload() json.RawMessage         { return t.payload }
func (t *topicDel) PublishedAt() int64               { return t.publishedAt }
func (t *topicDel) DeliveryCount() int               { return 1 }
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
			// find min settled with no lower unsettled
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
