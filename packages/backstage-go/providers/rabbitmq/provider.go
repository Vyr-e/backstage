// Package rabbitmq implements BackstageProvider over AMQP 0-9-1.
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vyr-e/backstage/packages/backstage-go"
)

type Config struct {
	URL    string
	Prefix string
}

type Provider struct {
	name   string
	url    string
	prefix string
	conn   *amqp.Connection
	pub    *amqp.Channel
	ctx    *backstage.ProviderContext
	jobs   *jobsCap
	topics *topicsCap
	delays backstage.Delays
}

func New(cfg Config) *Provider {
	url := cfg.URL
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "backstage"
	}
	p := &Provider{name: "rabbitmq", url: url, prefix: prefix}
	p.jobs = &jobsCap{p: p}
	p.topics = &topicsCap{p: p}
	return p
}

func (p *Provider) Name() string              { return p.name }
func (p *Provider) Jobs() backstage.Jobs      { return p.jobs }
func (p *Provider) Topics() backstage.Topics  { return p.topics }
func (p *Provider) Delays() backstage.Delays  { return p.delays }
func (p *Provider) Dedupe() backstage.Dedupe  { return nil }

func (p *Provider) Init(ctx context.Context, pctx backstage.ProviderContext) error {
	p.ctx = &pctx
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return err
	}
	p.conn = conn
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	p.pub = ch
	// Detect delayed exchange plugin
	err = ch.ExchangeDeclare(p.prefix+".delayed", "x-delayed-message", true, false, false, false, amqp.Table{"x-delayed-type": "direct"})
	if err != nil {
		if pctx.Capabilities.Delays == nil {
			return fmt.Errorf("rabbitmq jobs require delays: enable delayed-message plugin or pass Capabilities.Delays")
		}
		p.delays = pctx.Capabilities.Delays
	} else {
		p.delays = &delayedDelays{p: p}
		if pctx.Capabilities.Delays != nil {
			p.delays = pctx.Capabilities.Delays
		}
	}
	return nil
}

func (p *Provider) Close() error {
	if p.pub != nil {
		_ = p.pub.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

func (p *Provider) q(name string) string   { return p.prefix + "." + name }
func (p *Provider) dlq(name string) string { return p.prefix + "." + name + ".dead-letter" }

type jobsCap struct{ p *Provider }

func (j *jobsCap) Name() string { return "rabbitmq" }
func (j *jobsCap) Requires() []backstage.CapabilityName {
	return []backstage.CapabilityName{backstage.CapabilityDelays}
}
func (j *jobsCap) EnsureQueues(ctx context.Context, queues []string) error {
	for _, q := range queues {
		if _, err := j.p.pub.QueueDeclare(j.p.q(q), true, false, false, false, nil); err != nil {
			return err
		}
		if _, err := j.p.pub.QueueDeclare(j.p.dlq(q), true, false, false, false, nil); err != nil {
			return err
		}
	}
	return nil
}

type wireJob struct {
	Queue         string                 `json:"queue"`
	TaskName      string                 `json:"taskName"`
	Payload       json.RawMessage        `json:"payload"`
	EnqueuedAt    int64                  `json:"enqueuedAt"`
	Meta          backstage.JobMeta      `json:"meta"`
	DeliveryCount int                    `json:"deliveryCount"`
}

func (j *jobsCap) Publish(ctx context.Context, job backstage.OutgoingJob) (string, error) {
	payload, _ := json.Marshal(job.Payload)
	body, _ := json.Marshal(wireJob{
		Queue: job.Queue, TaskName: job.TaskName, Payload: payload,
		EnqueuedAt: job.EnqueuedAt, Meta: job.Meta, DeliveryCount: max(1, job.DeliveryCount),
	})
	err := j.p.pub.PublishWithContext(ctx, "", j.p.q(job.Queue), false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, Body: body,
	})
	return fmt.Sprintf("rabbit-%d", job.EnqueuedAt), err
}

func (j *jobsCap) Consume(ctx context.Context, opts backstage.ConsumeOptions, onDelivery func(context.Context, backstage.JobDelivery) error) (backstage.Subscription, error) {
	ch, err := j.p.conn.Channel()
	if err != nil {
		return nil, err
	}
	_ = ch.Qos(opts.Prefetch, 0, false)
	var running atomic.Bool
	running.Store(true)
	for _, q := range opts.Queues {
		_, _ = ch.QueueDeclare(j.p.q(q), true, false, false, false, nil)
		deliveries, err := ch.Consume(j.p.q(q), opts.ConsumerID+"-"+q, false, false, false, false, nil)
		if err != nil {
			_ = ch.Close()
			return nil, err
		}
		go func(queue string, ds <-chan amqp.Delivery) {
			for d := range ds {
				if !running.Load() {
					return
				}
				delivery := j.toDelivery(ch, queue, d)
				_ = onDelivery(ctx, delivery)
			}
		}(q, deliveries)
	}
	return &sub{stop: func() { running.Store(false); _ = ch.Close() }}, nil
}

func (j *jobsCap) toDelivery(ch *amqp.Channel, queue string, d amqp.Delivery) backstage.JobDelivery {
	var body wireJob
	_ = json.Unmarshal(d.Body, &body)
	count := body.DeliveryCount
	if count < 1 {
		count = 1
	}
	if d.Redelivered {
		count++
	}
	p := j.p
	return &rmqDelivery{ch: ch, d: d, queue: queue, body: body, count: count, p: p}
}

type rmqDelivery struct {
	ch    *amqp.Channel
	d     amqp.Delivery
	queue string
	body  wireJob
	count int
	p     *Provider
}

func (r *rmqDelivery) ID() string                      { return fmt.Sprintf("%d", r.d.DeliveryTag) }
func (r *rmqDelivery) Queue() string                   { return r.queue }
func (r *rmqDelivery) TaskName() string                { return r.body.TaskName }
func (r *rmqDelivery) Payload() json.RawMessage        { return r.body.Payload }
func (r *rmqDelivery) EnqueuedAt() int64               { return r.body.EnqueuedAt }
func (r *rmqDelivery) DeliveryCount() int              { return r.count }
func (r *rmqDelivery) Meta() backstage.JobMeta         { return r.body.Meta }
func (r *rmqDelivery) Ack(ctx context.Context) error   { return r.d.Ack(false) }
func (r *rmqDelivery) Retry(ctx context.Context, opts backstage.RetryOpts) error {
	next := r.body
	next.DeliveryCount = r.count + 1
	delays := r.p.delays
	if delays == nil && r.p.ctx != nil {
		delays = r.p.ctx.Capabilities.Delays
	}
	if delays == nil {
		return fmt.Errorf("rabbitmq retry requires delays")
	}
	job := backstage.OutgoingJob{
		Queue: next.Queue, TaskName: next.TaskName, Payload: next.Payload,
		EnqueuedAt: next.EnqueuedAt, Meta: next.Meta, DeliveryCount: next.DeliveryCount,
	}
	if opts.DelayMs <= 0 {
		_, err := r.p.jobs.Publish(ctx, job)
		if err != nil {
			return err
		}
	} else {
		if _, err := delays.Schedule(ctx, job, time.Now().UnixMilli()+opts.DelayMs); err != nil {
			return err
		}
	}
	return r.d.Ack(false)
}
func (r *rmqDelivery) DeadLetter(ctx context.Context, opts backstage.DeadLetterOpts) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"taskName": r.body.TaskName, "payload": r.body.Payload, "error": opts.Error,
		"originalId": r.ID(), "deliveryCount": r.count, "deadLetteredAt": time.Now().UnixMilli(),
	})
	_ = r.p.pub.PublishWithContext(ctx, "", r.p.dlq(r.queue), false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, Body: payload,
	})
	return r.d.Ack(false)
}

type delayedDelays struct{ p *Provider }

func (d *delayedDelays) Name() string { return "rabbitmq-delayed" }
func (d *delayedDelays) Schedule(ctx context.Context, job backstage.OutgoingJob, runAt int64) (string, error) {
	delay := runAt - time.Now().UnixMilli()
	if delay < 0 {
		delay = 0
	}
	payload, _ := json.Marshal(job.Payload)
	body, _ := json.Marshal(wireJob{
		Queue: job.Queue, TaskName: job.TaskName, Payload: payload,
		EnqueuedAt: job.EnqueuedAt, Meta: job.Meta, DeliveryCount: max(1, job.DeliveryCount),
	})
	_, _ = d.p.pub.QueueDeclare(d.p.q(job.Queue), true, false, false, false, nil)
	_ = d.p.pub.QueueBind(d.p.q(job.Queue), job.Queue, d.p.prefix+".delayed", false, nil)
	err := d.p.pub.PublishWithContext(ctx, d.p.prefix+".delayed", job.Queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, Body: body, Headers: amqp.Table{"x-delay": delay},
	})
	return fmt.Sprintf("scheduled:%d", runAt), err
}

type topicsCap struct{ p *Provider }

func (t *topicsCap) Name() string { return "rabbitmq" }
func (t *topicsCap) Publish(ctx context.Context, topic string, payload interface{}) (string, error) {
	ex := t.p.prefix + ".topics"
	_ = t.p.pub.ExchangeDeclare(ex, "topic", true, false, false, false, nil)
	body, _ := json.Marshal(map[string]interface{}{"payload": payload, "publishedAt": time.Now().UnixMilli()})
	err := t.p.pub.PublishWithContext(ctx, ex, topic, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: body})
	return fmt.Sprintf("topic-%d", time.Now().UnixMilli()), err
}
func (t *topicsCap) Subscribe(ctx context.Context, opts backstage.TopicSubscribeOptions, onMessage func(context.Context, backstage.TopicDelivery) error) (backstage.Subscription, error) {
	ch, err := t.p.conn.Channel()
	if err != nil {
		return nil, err
	}
	ex := t.p.prefix + ".topics"
	_ = ch.ExchangeDeclare(ex, "topic", true, false, false, false, nil)
	var q amqp.Queue
	if opts.Group != "" {
		q, err = ch.QueueDeclare(fmt.Sprintf("%s.topic.%s.%s", t.p.prefix, opts.Topic, opts.Group), true, false, false, false, nil)
	} else {
		q, err = ch.QueueDeclare("", false, true, true, false, nil)
	}
	if err != nil {
		return nil, err
	}
	_ = ch.QueueBind(q.Name, opts.Topic, ex, false, nil)
	ds, err := ch.Consume(q.Name, opts.ConsumerID, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	var running atomic.Bool
	running.Store(true)
	go func() {
		for d := range ds {
			if !running.Load() {
				return
			}
			var body map[string]json.RawMessage
			_ = json.Unmarshal(d.Body, &body)
			payload := body["payload"]
			var publishedAt int64
			_ = json.Unmarshal(body["publishedAt"], &publishedAt)
			td := &topicDel{d: d, topic: opts.Topic, payload: payload, publishedAt: publishedAt}
			if err := onMessage(ctx, td); err != nil {
				_ = d.Nack(false, true)
				continue
			}
			_ = d.Ack(false)
		}
	}()
	return &sub{stop: func() { running.Store(false); _ = ch.Close() }}, nil
}

type topicDel struct {
	d           amqp.Delivery
	topic       string
	payload     json.RawMessage
	publishedAt int64
}

func (t *topicDel) ID() string               { return fmt.Sprintf("%d", t.d.DeliveryTag) }
func (t *topicDel) Topic() string            { return t.topic }
func (t *topicDel) Payload() json.RawMessage { return t.payload }
func (t *topicDel) PublishedAt() int64       { return t.publishedAt }
func (t *topicDel) DeliveryCount() int       { return 1 }
func (t *topicDel) Ack(ctx context.Context) error { return t.d.Ack(false) }

type sub struct{ stop func() }

func (s *sub) Stop(ctx context.Context) error { s.stop(); return nil }

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
