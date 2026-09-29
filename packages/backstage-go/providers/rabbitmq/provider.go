// Package rabbitmq implements BackstageProvider over AMQP 0-9-1.
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vyr-e/backstage/packages/backstage-go"
)

type Config struct {
	URL           string
	Prefix        string
	MaxDeliveries int
}

type Provider struct {
	name          string
	url           string
	prefix        string
	maxDeliveries int
	mu            sync.Mutex
	pubMu         sync.Mutex // brief lock around PublishWithDeferredConfirm only
	conn          *amqp.Connection
	pub           *amqp.Channel
	ctx           *backstage.ProviderContext
	jobs          *jobsCap
	topics        *topicsCap
	delays        backstage.Delays
	delayedReady  bool
	boundQueues   map[string]struct{}
	closed        atomic.Bool
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
	maxD := cfg.MaxDeliveries
	if maxD == 0 {
		maxD = 5
	}
	p := &Provider{
		name: "rabbitmq", url: url, prefix: prefix, maxDeliveries: maxD,
		boundQueues: make(map[string]struct{}),
	}
	p.jobs = &jobsCap{p: p}
	p.topics = &topicsCap{p: p}
	return p
}

func (p *Provider) Name() string             { return p.name }
func (p *Provider) Jobs() backstage.Jobs     { return p.jobs }
func (p *Provider) Topics() backstage.Topics { return p.topics }
func (p *Provider) Delays() backstage.Delays { return p.delays }
func (p *Provider) Dedupe() backstage.Dedupe { return nil }

func (p *Provider) Init(ctx context.Context, pctx backstage.ProviderContext) error {
	p.ctx = &pctx
	if err := p.connect(); err != nil {
		return err
	}
	p.mu.Lock()
	ch := p.pub
	p.mu.Unlock()
	err := ch.ExchangeDeclare(p.prefix+".delayed", "x-delayed-message", true, false, false, false, amqp.Table{"x-delayed-type": "direct"})
	if err != nil {
		if pctx.Capabilities.Delays == nil {
			return fmt.Errorf("rabbitmq jobs require delays: enable delayed-message plugin or pass Capabilities.Delays")
		}
		p.delays = pctx.Capabilities.Delays
		p.delayedReady = false
	} else {
		p.delayedReady = true
		p.delays = &delayedDelays{p: p}
		if pctx.Capabilities.Delays != nil {
			p.delays = pctx.Capabilities.Delays
		}
	}
	return nil
}

// connect dials (or reuses) the shared AMQP connection and publisher channel.
// Always closes any previous connection before replacing it to avoid leaks.
func (p *Provider) connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.connectLocked()
}

func (p *Provider) connectLocked() error {
	if p.conn != nil && !p.conn.IsClosed() && p.pub != nil {
		return nil
	}
	// Close leftovers before dialing a fresh connection (no leaked conns).
	if p.pub != nil {
		_ = p.pub.Close()
		p.pub = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	p.conn = conn
	p.pub = ch
	return nil
}

// recreatePubChannel recreates only the publisher channel on the existing
// connection. Does not tear down the connection or other consumer channels.
func (p *Provider) recreatePubChannel() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil || p.conn.IsClosed() {
		return p.connectLocked()
	}
	if p.pub != nil {
		_ = p.pub.Close()
		p.pub = nil
	}
	ch, err := p.conn.Channel()
	if err != nil {
		// Connection itself may be dead — full reconnect.
		_ = p.conn.Close()
		p.conn = nil
		return p.connectLocked()
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	p.pub = ch
	return nil
}

func (p *Provider) ensurePub() (*amqp.Channel, error) {
	if err := p.connect(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pub, nil
}

// publishConfirmed uses deferred confirms so callers are not serialized behind
// one mutex while waiting for each broker ack. The mutex only covers the
// Publish call itself (delivery-tag assignment); WaitContext runs unlocked.
func (p *Provider) publishConfirmed(ctx context.Context, exchange, key string, msg amqp.Publishing) error {
	dc, err := p.publishDeferred(ctx, exchange, key, msg)
	if err != nil {
		// Channel/connection may be dead — recreate pub channel (or conn) once.
		if rerr := p.recreatePubChannel(); rerr != nil {
			return err
		}
		dc, err = p.publishDeferred(ctx, exchange, key, msg)
		if err != nil {
			return err
		}
	}
	if dc == nil {
		return fmt.Errorf("publisher confirms not enabled")
	}
	waitCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	acked, err := dc.WaitContext(waitCtx)
	if err != nil {
		return err
	}
	if !acked {
		return fmt.Errorf("broker nacked publish to %s/%s", exchange, key)
	}
	return nil
}

func (p *Provider) publishDeferred(ctx context.Context, exchange, key string, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	ch, err := p.ensurePub()
	if err != nil {
		return nil, err
	}
	p.pubMu.Lock()
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, false, false, msg)
	p.pubMu.Unlock()
	return dc, err
}

func (p *Provider) Close() error {
	p.closed.Store(true)
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	if p.pub != nil {
		err = p.pub.Close()
		p.pub = nil
	}
	if p.conn != nil {
		if e := p.conn.Close(); e != nil && err == nil {
			err = e
		}
		p.conn = nil
	}
	return err
}

func (p *Provider) q(name string) string   { return p.prefix + "." + name }
func (p *Provider) dlq(name string) string { return p.prefix + "." + name + ".dead-letter" }

type jobsCap struct{ p *Provider }

func (j *jobsCap) Name() string { return "rabbitmq" }
func (j *jobsCap) Requires() []backstage.CapabilityName {
	return []backstage.CapabilityName{backstage.CapabilityDelays}
}

func (j *jobsCap) EnsureQueues(ctx context.Context, queues []string) error {
	ch, err := j.p.ensurePub()
	if err != nil {
		return err
	}
	for _, q := range queues {
		if _, err := ch.QueueDeclare(j.p.q(q), true, false, false, false, nil); err != nil {
			return err
		}
		if _, err := ch.QueueDeclare(j.p.dlq(q), true, false, false, false, nil); err != nil {
			return err
		}
		if j.p.delayedReady {
			if err := ch.QueueBind(j.p.q(q), q, j.p.prefix+".delayed", false, nil); err != nil {
				return err
			}
			j.p.mu.Lock()
			j.p.boundQueues[q] = struct{}{}
			j.p.mu.Unlock()
		}
	}
	return nil
}

type wireJob struct {
	Queue         string            `json:"queue"`
	TaskName      string            `json:"taskName"`
	Payload       json.RawMessage   `json:"payload"`
	EnqueuedAt    int64             `json:"enqueuedAt"`
	Meta          backstage.JobMeta `json:"meta"`
	DeliveryCount int               `json:"deliveryCount"`
}

func (j *jobsCap) Publish(ctx context.Context, job backstage.OutgoingJob) (string, error) {
	payload, err := backstage.EncodePayload(job.Payload)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(wireJob{
		Queue: job.Queue, TaskName: job.TaskName, Payload: payload,
		EnqueuedAt: job.EnqueuedAt, Meta: job.Meta, DeliveryCount: max(1, job.DeliveryCount),
	})
	if err != nil {
		return "", err
	}
	err = j.p.publishConfirmed(ctx, "", j.p.q(job.Queue), amqp.Publishing{
		DeliveryMode: amqp.Persistent, Body: body,
	})
	return fmt.Sprintf("rabbit-%d", job.EnqueuedAt), err
}

func (j *jobsCap) Consume(ctx context.Context, opts backstage.ConsumeOptions, onDelivery func(context.Context, backstage.JobDelivery) error) (backstage.Subscription, error) {
	stopCtx, cancel := context.WithCancel(ctx)
	var running atomic.Bool
	running.Store(true)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		j.consumeLoop(stopCtx, &running, opts, onDelivery)
	}()
	return &sub{stop: func() {
		running.Store(false)
		cancel()
		wg.Wait()
	}}, nil
}

func (j *jobsCap) consumeLoop(ctx context.Context, running *atomic.Bool, opts backstage.ConsumeOptions, onDelivery func(context.Context, backstage.JobDelivery) error) {
	backoff := time.Second
	prefetch := opts.Prefetch
	if prefetch <= 0 {
		prefetch = 1
	}
	for running.Load() && !j.p.closed.Load() {
		if err := j.p.connect(); err != nil {
			if j.p.ctx != nil && j.p.ctx.Logger != nil {
				j.p.ctx.Logger.Warn("rabbitmq reconnect failed", "error", err)
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
		backoff = time.Second
		j.p.mu.Lock()
		conn := j.p.conn
		j.p.mu.Unlock()
		if conn == nil || conn.IsClosed() {
			continue
		}
		ch, err := conn.Channel()
		if err != nil {
			if conn.IsClosed() {
				j.p.clearConnIf(conn)
			}
			continue
		}
		_ = ch.Qos(prefetch, 0, false)
		closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
		chanClose := ch.NotifyClose(make(chan *amqp.Error, 1))

		type tagged struct {
			queue string
			ds    <-chan amqp.Delivery
		}
		var feeds []tagged
		ok := true
		for _, q := range opts.Queues {
			_, _ = ch.QueueDeclare(j.p.q(q), true, false, false, false, nil)
			ds, err := ch.Consume(j.p.q(q), opts.ConsumerID+"-"+q, false, false, false, false, nil)
			if err != nil {
				ok = false
				break
			}
			feeds = append(feeds, tagged{queue: q, ds: ds})
		}
		if !ok {
			_ = ch.Close()
			if conn.IsClosed() {
				j.p.clearConnIf(conn)
			}
			continue
		}

		sem := make(chan struct{}, prefetch)
		var feedWg sync.WaitGroup
		for _, f := range feeds {
			feedWg.Add(1)
			go func(queue string, ds <-chan amqp.Delivery) {
				defer feedWg.Done()
				for d := range ds {
					if !running.Load() {
						return
					}
					sem <- struct{}{}
					go func(d amqp.Delivery) {
						defer func() { <-sem }()
						delivery := j.toDelivery(ch, queue, d)
						_ = onDelivery(ctx, delivery)
					}(d)
				}
			}(f.queue, f.ds)
		}

		connDied := false
		select {
		case <-ctx.Done():
			_ = ch.Close()
			feedWg.Wait()
			// Drain in-flight handlers
			for i := 0; i < prefetch; i++ {
				sem <- struct{}{}
			}
			return
		case <-closeCh:
			connDied = true
		case <-chanClose:
			// Channel-only failure: leave shared connection alone so other
			// consumers/publishers keep working.
		}
		_ = ch.Close()
		feedWg.Wait()
		for i := 0; i < prefetch; i++ {
			sem <- struct{}{}
		}
		if connDied {
			j.p.clearConnIf(conn)
			if running.Load() && j.p.ctx != nil && j.p.ctx.Logger != nil {
				j.p.ctx.Logger.Warn("rabbitmq connection lost; reconnecting")
			}
		} else if running.Load() && j.p.ctx != nil && j.p.ctx.Logger != nil {
			j.p.ctx.Logger.Warn("rabbitmq consumer channel lost; recreating channel")
		}
	}
}

// clearConnIf drops the shared connection only if it is still the one that died,
// closing it first so it is never leaked.
func (p *Provider) clearConnIf(conn *amqp.Connection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != conn {
		return
	}
	if p.pub != nil {
		_ = p.pub.Close()
		p.pub = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
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
	return &rmqDelivery{ch: ch, d: d, queue: queue, body: body, count: count, p: j.p}
}

type rmqDelivery struct {
	ch    *amqp.Channel
	d     amqp.Delivery
	queue string
	body  wireJob
	count int
	p     *Provider
}

func (r *rmqDelivery) ID() string               { return fmt.Sprintf("%d", r.d.DeliveryTag) }
func (r *rmqDelivery) Queue() string            { return r.queue }
func (r *rmqDelivery) TaskName() string         { return r.body.TaskName }
func (r *rmqDelivery) Payload() json.RawMessage { return r.body.Payload }
func (r *rmqDelivery) EnqueuedAt() int64        { return r.body.EnqueuedAt }
func (r *rmqDelivery) DeliveryCount() int       { return r.count }
func (r *rmqDelivery) Meta() backstage.JobMeta  { return r.body.Meta }
func (r *rmqDelivery) Ack(ctx context.Context) error {
	return r.d.Ack(false)
}
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
		if _, err := r.p.jobs.Publish(ctx, job); err != nil {
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
	if err := r.p.publishConfirmed(ctx, "", r.p.dlq(r.queue), amqp.Publishing{
		DeliveryMode: amqp.Persistent, Body: payload,
	}); err != nil {
		return err
	}
	return r.d.Ack(false)
}

type delayedDelays struct{ p *Provider }

func (d *delayedDelays) Name() string { return "rabbitmq-delayed" }
func (d *delayedDelays) Schedule(ctx context.Context, job backstage.OutgoingJob, runAt int64) (string, error) {
	delay := runAt - time.Now().UnixMilli()
	if delay < 0 {
		delay = 0
	}
	payload, err := backstage.EncodePayload(job.Payload)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(wireJob{
		Queue: job.Queue, TaskName: job.TaskName, Payload: payload,
		EnqueuedAt: job.EnqueuedAt, Meta: job.Meta, DeliveryCount: max(1, job.DeliveryCount),
	})
	if err != nil {
		return "", err
	}
	// Bind once via ensureQueues; schedule only publishes.
	d.p.mu.Lock()
	_, bound := d.p.boundQueues[job.Queue]
	d.p.mu.Unlock()
	if !bound {
		ch, err := d.p.ensurePub()
		if err != nil {
			return "", err
		}
		if _, err := ch.QueueDeclare(d.p.q(job.Queue), true, false, false, false, nil); err != nil {
			return "", err
		}
		if err := ch.QueueBind(d.p.q(job.Queue), job.Queue, d.p.prefix+".delayed", false, nil); err != nil {
			return "", err
		}
		d.p.mu.Lock()
		d.p.boundQueues[job.Queue] = struct{}{}
		d.p.mu.Unlock()
	}
	err = d.p.publishConfirmed(ctx, d.p.prefix+".delayed", job.Queue, amqp.Publishing{
		DeliveryMode: amqp.Persistent, Body: body, Headers: amqp.Table{"x-delay": delay},
	})
	return fmt.Sprintf("scheduled:%d", runAt), err
}

type topicsCap struct{ p *Provider }

func (t *topicsCap) Name() string { return "rabbitmq" }
func (t *topicsCap) Publish(ctx context.Context, topic string, payload interface{}) (string, error) {
	ex := t.p.prefix + ".topics"
	ch, err := t.p.ensurePub()
	if err != nil {
		return "", err
	}
	if err := ch.ExchangeDeclare(ex, "topic", true, false, false, false, nil); err != nil {
		return "", err
	}
	encoded, err := backstage.EncodePayload(payload)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(map[string]interface{}{"payload": json.RawMessage(encoded), "publishedAt": time.Now().UnixMilli(), "deliveryCount": 1})
	if err != nil {
		return "", err
	}
	err = t.p.publishConfirmed(ctx, ex, topic, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: body})
	return fmt.Sprintf("topic-%d", time.Now().UnixMilli()), err
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
	backoff := time.Second
	const prefetch = 10
	for running.Load() && !t.p.closed.Load() {
		if err := t.p.connect(); err != nil {
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
		backoff = time.Second
		t.p.mu.Lock()
		conn := t.p.conn
		t.p.mu.Unlock()
		if conn == nil || conn.IsClosed() {
			continue
		}
		ch, err := conn.Channel()
		if err != nil {
			if conn.IsClosed() {
				t.p.clearConnIf(conn)
			}
			continue
		}
		_ = ch.Qos(prefetch, 0, false)
		ex := t.p.prefix + ".topics"
		_ = ch.ExchangeDeclare(ex, "topic", true, false, false, false, nil)
		var q amqp.Queue
		if opts.Group != "" {
			q, err = ch.QueueDeclare(fmt.Sprintf("%s.topic.%s.%s", t.p.prefix, opts.Topic, opts.Group), true, false, false, false, nil)
		} else {
			q, err = ch.QueueDeclare("", false, true, true, false, nil)
		}
		if err != nil {
			_ = ch.Close()
			continue
		}
		_ = ch.QueueBind(q.Name, opts.Topic, ex, false, nil)
		ds, err := ch.Consume(q.Name, opts.ConsumerID, false, false, false, false, nil)
		if err != nil {
			_ = ch.Close()
			continue
		}
		closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
		chanClose := ch.NotifyClose(make(chan *amqp.Error, 1))
		sem := make(chan struct{}, prefetch)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for d := range ds {
				if !running.Load() {
					return
				}
				sem <- struct{}{}
				go func(d amqp.Delivery) {
					defer func() { <-sem }()
					t.handleTopicDelivery(ctx, q.Name, opts, d, onMessage)
				}(d)
			}
		}()
		connDied := false
		select {
		case <-ctx.Done():
			_ = ch.Close()
			<-done
			for i := 0; i < prefetch; i++ {
				sem <- struct{}{}
			}
			return
		case <-closeCh:
			connDied = true
		case <-chanClose:
		}
		_ = ch.Close()
		<-done
		for i := 0; i < prefetch; i++ {
			sem <- struct{}{}
		}
		if connDied {
			t.p.clearConnIf(conn)
		}
	}
}

func (t *topicsCap) handleTopicDelivery(ctx context.Context, queueName string, opts backstage.TopicSubscribeOptions, d amqp.Delivery, onMessage func(context.Context, backstage.TopicDelivery) error) {
	var body map[string]json.RawMessage
	_ = json.Unmarshal(d.Body, &body)
	payload := body["payload"]
	var publishedAt int64
	_ = json.Unmarshal(body["publishedAt"], &publishedAt)
	count := 1
	if raw, ok := body["deliveryCount"]; ok {
		_ = json.Unmarshal(raw, &count)
	}
	if count < 1 {
		count = 1
	}
	td := &topicDel{d: d, topic: opts.Topic, payload: payload, publishedAt: publishedAt, count: count}
	if err := onMessage(ctx, td); err != nil {
		if count >= t.p.maxDeliveries {
			if t.p.ctx != nil && t.p.ctx.Logger != nil {
				t.p.ctx.Logger.Error("Topic handler failed after max deliveries; dropping",
					"topic", opts.Topic, "error", err)
			}
			_ = d.Ack(false)
			return
		}
		// Republish to this subscriber's queue only (default exchange), not the
		// topic exchange — other fan-out subscribers must not get the retry.
		next, _ := json.Marshal(map[string]interface{}{
			"payload": json.RawMessage(payload), "publishedAt": publishedAt, "deliveryCount": count + 1,
		})
		if pubErr := t.p.publishConfirmed(ctx, "", queueName, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: next}); pubErr != nil {
			// leave unacked for broker redelivery after reconnect
			return
		}
		_ = d.Ack(false)
		return
	}
	_ = d.Ack(false)
}

type topicDel struct {
	d           amqp.Delivery
	topic       string
	payload     json.RawMessage
	publishedAt int64
	count       int
}

func (t *topicDel) ID() string                    { return fmt.Sprintf("%d", t.d.DeliveryTag) }
func (t *topicDel) Topic() string                 { return t.topic }
func (t *topicDel) Payload() json.RawMessage      { return t.payload }
func (t *topicDel) PublishedAt() int64            { return t.publishedAt }
func (t *topicDel) DeliveryCount() int            { return t.count }
func (t *topicDel) Ack(ctx context.Context) error { return t.d.Ack(false) }

type sub struct{ stop func() }

func (s *sub) Stop(ctx context.Context) error { s.stop(); return nil }

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
