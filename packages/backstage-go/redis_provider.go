package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	ackBatchSize       = 100
	ackFlushIntervalMs = 50
)

type RedisStreamsProviderConfig struct {
	Host             string
	Port             int
	Password         string
	DB               int
	Redis            *redis.Client
	Prefix           string
	TopicMaxLen      int64
	TopicGroupIdleMs int64
	DeleteOnAck      bool
	BlockTimeout     time.Duration
	ReclaimInterval  time.Duration
	MaxDeliveries    int
}

type RedisStreamsProvider struct {
	name             string
	redis            *redis.Client
	scripts          *ScriptRegistry
	prefix           string
	topicMaxLen      int64
	topicGroupIdleMs int64
	deleteOnAck      bool
	blockTimeout     time.Duration
	reclaimInterval  time.Duration
	maxDeliveries    int
	ownsClient       bool
	pctx             *ProviderContext
	jobs             *redisJobs
	topics           *redisTopics
	delays           *redisDelays
	dedupe           *redisDedupe
}

func NewRedisStreamsProvider(cfg RedisStreamsProviderConfig) *RedisStreamsProvider {
	owns := cfg.Redis == nil
	rdb := cfg.Redis
	if rdb == nil {
		host, port := cfg.Host, cfg.Port
		if host == "" {
			host = "localhost"
		}
		if port == 0 {
			port = 6379
		}
		rdb = redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%d", host, port), Password: cfg.Password, DB: cfg.DB})
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = StreamPrefix
	}
	tmax := cfg.TopicMaxLen
	if tmax == 0 {
		tmax = 10_000
	}
	tidle := cfg.TopicGroupIdleMs
	if tidle == 0 {
		tidle = 3_600_000
	}
	block := cfg.BlockTimeout
	if block == 0 {
		block = 5 * time.Second
	}
	reclaim := cfg.ReclaimInterval
	if reclaim == 0 {
		reclaim = 30 * time.Second
	}
	maxD := cfg.MaxDeliveries
	if maxD == 0 {
		maxD = 5
	}
	p := &RedisStreamsProvider{
		name: "redis-streams", redis: rdb, scripts: NewScriptRegistry(rdb), prefix: prefix,
		topicMaxLen: tmax, topicGroupIdleMs: tidle, deleteOnAck: cfg.DeleteOnAck,
		blockTimeout: block, reclaimInterval: reclaim, maxDeliveries: maxD, ownsClient: owns,
	}
	p.jobs = &redisJobs{p: p}
	p.topics = &redisTopics{p: p}
	p.delays = &redisDelays{p: p}
	p.dedupe = &redisDedupe{p: p}
	return p
}

func (p *RedisStreamsProvider) Name() string            { return p.name }
func (p *RedisStreamsProvider) Jobs() Jobs              { return p.jobs }
func (p *RedisStreamsProvider) Topics() Topics          { return p.topics }
func (p *RedisStreamsProvider) Delays() Delays          { return p.delays }
func (p *RedisStreamsProvider) Dedupe() Dedupe          { return p.dedupe }
func (p *RedisStreamsProvider) Redis() *redis.Client    { return p.redis }
func (p *RedisStreamsProvider) Scripts() *ScriptRegistry { return p.scripts }
func (p *RedisStreamsProvider) Prefix() string          { return p.prefix }

func (p *RedisStreamsProvider) Init(ctx context.Context, pctx ProviderContext) error {
	p.pctx = &pctx
	return nil
}

func (p *RedisStreamsProvider) Close() error {
	if p.ownsClient {
		return p.redis.Close()
	}
	return nil
}

func (p *RedisStreamsProvider) ensureGroup(ctx context.Context, key, group, start string) error {
	err := p.redis.XGroupCreateMkStream(ctx, key, group, start).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func payloadJSON(v interface{}) (string, error) {
	switch x := v.(type) {
	case json.RawMessage:
		if len(x) == 0 {
			return "null", nil
		}
		return string(x), nil
	case nil:
		return "null", nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}

// --- ack batcher ---

type ackBatcher struct {
	redis       *redis.Client
	group       string
	deleteOnAck bool
	mu          sync.Mutex
	pending     map[string][]string
	closed      bool
	stop        chan struct{}
	wg          sync.WaitGroup
}

func newAckBatcher(rdb *redis.Client, group string, deleteOnAck bool) *ackBatcher {
	b := &ackBatcher{
		redis: rdb, group: group, deleteOnAck: deleteOnAck,
		pending: make(map[string][]string), stop: make(chan struct{}),
	}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		t := time.NewTicker(ackFlushIntervalMs * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_ = b.flush(context.Background())
			case <-b.stop:
				_ = b.flush(context.Background())
				return
			}
		}
	}()
	return b
}

func (b *ackBatcher) queue(stream, id string) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = b.redis.XAck(context.Background(), stream, b.group, id).Err()
		return
	}
	b.pending[stream] = append(b.pending[stream], id)
	flushNow := len(b.pending[stream]) >= ackBatchSize
	var ids []string
	if flushNow {
		ids = b.pending[stream]
		b.pending[stream] = nil
	}
	b.mu.Unlock()
	if flushNow {
		b.flushStream(context.Background(), stream, ids)
	}
}

func (b *ackBatcher) flush(ctx context.Context) error {
	b.mu.Lock()
	snap := b.pending
	b.pending = make(map[string][]string)
	b.mu.Unlock()
	for s, ids := range snap {
		b.flushStream(ctx, s, ids)
	}
	return nil
}

func (b *ackBatcher) flushStream(ctx context.Context, stream string, ids []string) {
	if len(ids) == 0 {
		return
	}
	_ = b.redis.XAck(ctx, stream, b.group, ids...).Err()
	if b.deleteOnAck {
		_ = b.redis.XDel(ctx, stream, ids...).Err()
	}
}

func (b *ackBatcher) close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		close(b.stop)
	}
	b.mu.Unlock()
	b.wg.Wait()
}

// --- jobs ---

type redisJobs struct{ p *RedisStreamsProvider }

func (j *redisJobs) Name() string               { return "redis-streams" }
func (j *redisJobs) Requires() []CapabilityName { return nil }
func (j *redisJobs) EnsureQueues(ctx context.Context, queues []string) error {
	// XGROUP CREATE MKSTREAM happens in Consume — never XADD MAXLEN~0.
	return nil
}

func (j *redisJobs) Publish(ctx context.Context, job OutgoingJob) (string, error) {
	key := StreamKey(j.p.prefix, job.Queue)
	payload, err := payloadJSON(job.Payload)
	if err != nil {
		return "", err
	}
	values := map[string]interface{}{
		"taskName": job.TaskName, "payload": payload, "enqueuedAt": job.EnqueuedAt,
	}
	if job.Meta.Attempts > 0 {
		values["attempts"] = job.Meta.Attempts
	}
	if job.Meta.Backoff != nil {
		b, _ := json.Marshal(job.Meta.Backoff)
		values["backoff"] = string(b)
	}
	if job.Meta.Timeout > 0 {
		values["timeout"] = job.Meta.Timeout
	}
	return j.p.redis.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: values}).Result()
}

type redisSubscription struct {
	stopOnce sync.Once
	stopFn   func()
}

func (s *redisSubscription) Stop(ctx context.Context) error {
	s.stopOnce.Do(s.stopFn)
	return nil
}

func (j *redisJobs) Consume(ctx context.Context, opts ConsumeOptions, onDelivery func(context.Context, JobDelivery) error) (Subscription, error) {
	for _, q := range opts.Queues {
		if err := j.p.ensureGroup(ctx, StreamKey(j.p.prefix, q), opts.Group, "0"); err != nil {
			return nil, err
		}
	}
	subCtx, cancel := context.WithCancel(ctx)
	var running atomic.Bool
	running.Store(true)
	prefetch := opts.Prefetch
	if prefetch <= 0 {
		prefetch = 1
	}
	sem := make(chan struct{}, prefetch)
	var wg sync.WaitGroup
	acks := newAckBatcher(j.p.redis, opts.Group, j.p.deleteOnAck)

	go func() {
		t := time.NewTicker(j.p.reclaimInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if running.Load() {
					j.p.reclaimLoop(subCtx, opts, onDelivery, sem, &wg, acks)
				}
			case <-subCtx.Done():
				return
			}
		}
	}()

	go func() {
		streams := make([]string, 0, len(opts.Queues)*2)
		for _, q := range opts.Queues {
			streams = append(streams, StreamKey(j.p.prefix, q))
		}
		for range opts.Queues {
			streams = append(streams, ">")
		}
		for running.Load() {
			avail := cap(sem) - len(sem)
			if avail <= 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			result, err := j.p.redis.XReadGroup(subCtx, &redis.XReadGroupArgs{
				Group: opts.Group, Consumer: opts.ConsumerID, Streams: streams,
				Count: int64(avail), Block: j.p.blockTimeout,
			}).Result()
			if err == redis.Nil || err == context.Canceled {
				continue
			}
			if err != nil {
				if running.Load() {
					if strings.Contains(err.Error(), "NOGROUP") {
						for _, q := range opts.Queues {
							_ = j.p.ensureGroup(subCtx, StreamKey(j.p.prefix, q), opts.Group, "0")
						}
					}
					time.Sleep(time.Second)
				}
				continue
			}
			for _, stream := range result {
				queue := QueueFromStreamKey(j.p.prefix, stream.Stream)
				for _, msg := range stream.Messages {
					d := j.p.toJobDelivery(queue, msg, 1, opts, acks)
					if d == nil {
						continue
					}
					sem <- struct{}{}
					wg.Add(1)
					go func(delivery JobDelivery) {
						defer func() { <-sem; wg.Done() }()
						_ = onDelivery(subCtx, delivery)
					}(d)
				}
			}
		}
	}()

	return &redisSubscription{stopFn: func() {
		running.Store(false)
		cancel()
		// Do not wait on in-flight handlers — Client applies gracePeriod.
		time.Sleep(50 * time.Millisecond)
		acks.close()
	}}, nil
}

func (p *RedisStreamsProvider) reclaimLoop(ctx context.Context, opts ConsumeOptions, onDelivery func(context.Context, JobDelivery) error, sem chan struct{}, wg *sync.WaitGroup, acks *ackBatcher) {
	idle := time.Duration(opts.IdleTimeout) * time.Millisecond
	if idle <= 0 {
		idle = time.Minute
	}
	for _, q := range opts.Queues {
		sKey := StreamKey(p.prefix, q)
		pending, err := p.redis.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: sKey, Group: opts.Group, Idle: idle, Start: "-", End: "+", Count: 10,
		}).Result()
		if err != nil {
			continue
		}
		for _, entry := range pending {
			full, err := p.redis.XRange(ctx, sKey, entry.ID, entry.ID).Result()
			if err != nil || len(full) == 0 {
				continue
			}
			if bj, ok := full[0].Values["backoff"].(string); ok && bj != "" {
				var bc BackoffConfig
				if json.Unmarshal([]byte(bj), &bc) == nil {
					req := ComputeBackoff(bc, int(entry.RetryCount))
					if entry.Idle < time.Duration(req)*time.Millisecond {
						continue
					}
				}
			}
			claimed, err := p.redis.XClaim(ctx, &redis.XClaimArgs{
				Stream: sKey, Group: opts.Group, Consumer: opts.ConsumerID, MinIdle: idle, Messages: []string{entry.ID},
			}).Result()
			if err != nil || len(claimed) == 0 {
				continue
			}
			d := p.toJobDelivery(q, claimed[0], int(entry.RetryCount)+1, opts, acks)
			if d == nil {
				continue
			}
			select {
			case sem <- struct{}{}:
			default:
				continue
			}
			wg.Add(1)
			go func(delivery JobDelivery) {
				defer func() { <-sem; wg.Done() }()
				_ = onDelivery(ctx, delivery)
			}(d)
		}
	}
}

type redisJobDelivery struct {
	p             *RedisStreamsProvider
	id, queue, taskName string
	payload       json.RawMessage
	enqueuedAt    int64
	deliveryCount int
	meta          JobMeta
	streamKey     string
	group         string
	acks          *ackBatcher
	raw           redis.XMessage
}

func (p *RedisStreamsProvider) toJobDelivery(queue string, msg redis.XMessage, deliveryCount int, opts ConsumeOptions, acks *ackBatcher) JobDelivery {
	if _, init := msg.Values["_init"]; init {
		return nil
	}
	taskName, _ := msg.Values["taskName"].(string)
	payloadStr, _ := msg.Values["payload"].(string)
	enqueuedAt, _ := asInt64(msg.Values["enqueuedAt"])
	meta := JobMeta{}
	if a, ok := asInt64(msg.Values["attempts"]); ok {
		meta.Attempts = int(a)
	}
	if b, ok := msg.Values["backoff"].(string); ok && b != "" {
		var bc BackoffConfig
		if json.Unmarshal([]byte(b), &bc) == nil {
			meta.Backoff = &bc
		}
	}
	if to, ok := asInt64(msg.Values["timeout"]); ok {
		meta.Timeout = to
	}
	return &redisJobDelivery{
		p: p, id: msg.ID, queue: queue, taskName: taskName, payload: json.RawMessage(payloadStr),
		enqueuedAt: enqueuedAt, deliveryCount: deliveryCount, meta: meta,
		streamKey: StreamKey(p.prefix, queue), group: opts.Group, acks: acks, raw: msg,
	}
}

func (d *redisJobDelivery) ID() string               { return d.id }
func (d *redisJobDelivery) Queue() string            { return d.queue }
func (d *redisJobDelivery) TaskName() string         { return d.taskName }
func (d *redisJobDelivery) Payload() json.RawMessage { return d.payload }
func (d *redisJobDelivery) EnqueuedAt() int64        { return d.enqueuedAt }
func (d *redisJobDelivery) DeliveryCount() int       { return d.deliveryCount }
func (d *redisJobDelivery) Meta() JobMeta            { return d.meta }

func (d *redisJobDelivery) Ack(ctx context.Context) error {
	d.acks.queue(d.streamKey, d.id)
	return nil
}

func (d *redisJobDelivery) Retry(ctx context.Context, opts RetryOpts) error {
	if opts.Error != "" {
		d.p.redis.Set(ctx, ErrorKey(d.p.prefix, d.id), opts.Error, time.Hour)
	}
	return nil
}

func (d *redisJobDelivery) DeadLetter(ctx context.Context, opts DeadLetterOpts) error {
	dlq := DeadLetterKey(d.p.prefix, d.queue)
	errMsg := opts.Error
	if errMsg == "" {
		if stored, err := d.p.redis.Get(ctx, ErrorKey(d.p.prefix, d.id)).Result(); err == nil {
			errMsg = stored
		}
	}
	values := map[string]interface{}{
		"taskName": d.raw.Values["taskName"], "payload": d.raw.Values["payload"],
		"enqueuedAt": d.raw.Values["enqueuedAt"], "originalId": d.id,
		"deliveryCount": d.deliveryCount, "deadLetteredAt": time.Now().UnixMilli(),
	}
	if errMsg != "" {
		values["error"] = errMsg
	}
	if err := d.p.redis.XAdd(ctx, &redis.XAddArgs{Stream: dlq, Values: values}).Err(); err != nil {
		return err
	}
	// ACK original stream (fixes custom-queue wrong-stream ACK bug)
	if err := d.p.redis.XAck(ctx, d.streamKey, d.group, d.id).Err(); err != nil {
		return err
	}
	d.p.redis.Del(ctx, ErrorKey(d.p.prefix, d.id))
	return nil
}

// PromoteCrossProvider promotes due delayed jobs. Called by Client Start loop only.
func (p *RedisStreamsProvider) PromoteCrossProvider(ctx context.Context) (int64, error) {
	if p.pctx == nil {
		return 0, nil
	}
	jobs := p.pctx.Capabilities.Jobs
	if jobs == nil || jobs.Name() == "redis-streams" {
		result, err := p.redis.Eval(ctx, ProcessScheduledLua, []string{ScheduledKey(p.prefix)},
			time.Now().UnixMilli(), p.prefix, string(PriorityDefault)).Result()
		if err != nil {
			return 0, err
		}
		n, _ := result.(int64)
		return n, nil
	}
	// Retry stuck claimed first
	stuck, _ := p.redis.ZRangeByScore(ctx, ScheduledClaimedKey(p.prefix), &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
	var n int64
	for _, raw := range stuck {
		if p.publishClaimed(ctx, jobs, raw) {
			n++
		}
	}
	claimed, err := p.redis.Eval(ctx, ClaimScheduledLua, []string{ScheduledKey(p.prefix), ScheduledClaimedKey(p.prefix)}, time.Now().UnixMilli()).StringSlice()
	if err != nil && err != redis.Nil {
		return n, err
	}
	for _, raw := range claimed {
		if p.publishClaimed(ctx, jobs, raw) {
			n++
		}
	}
	return n, nil
}

func (p *RedisStreamsProvider) publishClaimed(ctx context.Context, jobs Jobs, raw string) bool {
	var task map[string]interface{}
	if json.Unmarshal([]byte(raw), &task) != nil {
		return false
	}
	queue := string(PriorityDefault)
	if sk, ok := task["streamKey"].(string); ok && sk != "" {
		queue = QueueFromStreamKey(p.prefix, sk)
	} else if pr, ok := task["priority"].(string); ok && pr != "" {
		queue = pr
	}
	payload := json.RawMessage("null")
	if ps, ok := task["payload"].(string); ok {
		payload = json.RawMessage(ps)
	}
	enqueuedAt, _ := asInt64(task["enqueuedAt"])
	meta := JobMeta{}
	if a, ok := asInt64(task["attempts"]); ok {
		meta.Attempts = int(a)
	}
	if b, ok := task["backoff"].(string); ok && b != "" {
		var bc BackoffConfig
		if json.Unmarshal([]byte(b), &bc) == nil {
			meta.Backoff = &bc
		}
	}
	if to, ok := asInt64(task["timeout"]); ok {
		meta.Timeout = to
	}
	taskName, _ := task["taskName"].(string)
	if _, err := jobs.Publish(ctx, OutgoingJob{Queue: queue, TaskName: taskName, Payload: payload, EnqueuedAt: enqueuedAt, Meta: meta}); err != nil {
		return false
	}
	p.redis.ZRem(ctx, ScheduledClaimedKey(p.prefix), raw)
	return true
}
