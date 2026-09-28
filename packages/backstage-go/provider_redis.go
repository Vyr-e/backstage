package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStreamsProviderConfig configures the Redis Streams transport.
type RedisStreamsProviderConfig struct {
	Redis         *redis.Client // optional; when set, provider does not own Close
	Host          string
	Port          int
	Password      string
	DB            int
	ConsumerGroup string
	Prefix        string
	DefaultPriority Priority
}

// RedisStreamsProvider implements Provider using Redis Streams + consumer groups.
type RedisStreamsProvider struct {
	name          string
	caps          ProviderCapabilities
	redis         *redis.Client
	ownsClient    bool
	consumerGroup string
	prefix        string
	defaultPrio   Priority
	ensured       sync.Map // queue name -> struct{}
}

// NewRedisStreamsProvider creates a Redis Streams provider.
func NewRedisStreamsProvider(cfg RedisStreamsProviderConfig) *RedisStreamsProvider {
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = WirePrefix
	}
	group := cfg.ConsumerGroup
	if group == "" {
		group = WireDefaultConsumerGroup
	}
	prio := cfg.DefaultPriority
	if prio == "" {
		prio = PriorityDefault
	}

	var rdb *redis.Client
	owns := false
	if cfg.Redis != nil {
		rdb = cfg.Redis
	} else {
		host := cfg.Host
		if host == "" {
			host = "localhost"
		}
		port := cfg.Port
		if port == 0 {
			port = 6379
		}
		rdb = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", host, port),
			Password: cfg.Password,
			DB:       cfg.DB,
		})
		owns = true
	}

	return &RedisStreamsProvider{
		name: "redis-streams",
		caps: ProviderCapabilities{
			Durable: true, Broadcast: true, Scheduling: true,
			Retries: true, Deduplication: true,
		},
		redis:         rdb,
		ownsClient:    owns,
		consumerGroup: group,
		prefix:        prefix,
		defaultPrio:   prio,
	}
}

func (p *RedisStreamsProvider) Name() string                     { return p.name }
func (p *RedisStreamsProvider) Capabilities() ProviderCapabilities { return p.caps }

// Client returns the underlying Redis client (for Inspect/ScriptRegistry/tests).
func (p *RedisStreamsProvider) Client() *redis.Client { return p.redis }

// SetConsumerGroup overrides the consumer group used for ack (matches Worker config).
func (p *RedisStreamsProvider) SetConsumerGroup(group string) {
	if group != "" {
		p.consumerGroup = group
	}
}

func (p *RedisStreamsProvider) Prefix() string { return p.prefix }

func (p *RedisStreamsProvider) EnsureQueues(ctx context.Context, queues []string) error {
	for _, q := range queues {
		if err := p.createConsumerGroup(ctx, wireStreamKey(p.prefix, q)); err != nil {
			return err
		}
		p.ensured.Store(q, struct{}{})
	}
	return nil
}

func (p *RedisStreamsProvider) createConsumerGroup(ctx context.Context, streamKey string) error {
	err := p.redis.XGroupCreateMkStream(ctx, streamKey, p.consumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("XGroupCreate for %s: %w", streamKey, err)
	}
	return nil
}

func (p *RedisStreamsProvider) Close() error {
	if p.ownsClient && p.redis != nil {
		return p.redis.Close()
	}
	return nil
}

func (p *RedisStreamsProvider) Publish(ctx context.Context, taskName string, payload interface{}, opts PublishOptions) (string, error) {
	if opts.Dedupe != nil {
		dedupeKey := wireDedupeKey(p.prefix, opts.Dedupe.Key)
		ttl := opts.Dedupe.TTL
		if ttl == 0 {
			ttl = time.Hour
		}
		set, err := p.redis.SetNX(ctx, dedupeKey, "1", ttl).Result()
		if err != nil {
			return "", fmt.Errorf("dedupe setnx: %w", err)
		}
		if !set {
			return "", nil
		}
	}

	queue := opts.Queue
	if queue == "" {
		if opts.Priority != "" {
			queue = string(opts.Priority)
		} else {
			queue = string(p.defaultPrio)
		}
	}
	streamKey := wireStreamKey(p.prefix, queue)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	enqueuedAt := time.Now().UnixMilli()

	values := map[string]interface{}{
		WireFieldTaskName:   taskName,
		WireFieldPayload:    string(payloadBytes),
		WireFieldEnqueuedAt: enqueuedAt,
	}
	if opts.Attempts > 0 {
		values[WireFieldAttempts] = opts.Attempts
	}
	if opts.Backoff != nil {
		b, _ := json.Marshal(opts.Backoff)
		values[WireFieldBackoff] = string(b)
	}
	if opts.Timeout > 0 {
		values[WireFieldTimeout] = opts.Timeout.Milliseconds()
	}

	if opts.Delay > 0 {
		executeAt := float64(time.Now().Add(opts.Delay).UnixMilli())
		scheduledData := map[string]interface{}{
			"taskName":   taskName,
			"payload":    string(payloadBytes),
			"enqueuedAt": enqueuedAt,
			"streamKey":  streamKey,
			"priority":   queue,
		}
		if opts.Attempts > 0 {
			scheduledData["attempts"] = opts.Attempts
		}
		if opts.Backoff != nil {
			b, _ := json.Marshal(opts.Backoff)
			scheduledData["backoff"] = string(b)
		}
		if opts.Timeout > 0 {
			scheduledData["timeout"] = opts.Timeout.Milliseconds()
		}
		data, _ := json.Marshal(scheduledData)
		if err := p.redis.ZAdd(ctx, wireScheduledKey(p.prefix), redis.Z{
			Score: executeAt, Member: string(data),
		}).Err(); err != nil {
			return "", fmt.Errorf("zadd scheduled: %w", err)
		}
		return fmt.Sprintf("scheduled:%d", int64(executeAt)), nil
	}

	id, err := p.redis.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: values}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd: %w", err)
	}
	return id, nil
}

func (p *RedisStreamsProvider) Consume(ctx context.Context, args ConsumeArgs) ([]MessageRef, error) {
	if len(args.Queues) == 0 {
		return nil, nil
	}
	streamKeys := make([]string, len(args.Queues))
	for i, q := range args.Queues {
		streamKeys[i] = wireStreamKey(p.prefix, q)
	}
	streams := append(append([]string{}, streamKeys...), make([]string, len(streamKeys))...)
	for i := range streamKeys {
		streams[len(streamKeys)+i] = ">"
	}

	block := time.Duration(args.BlockMs) * time.Millisecond
	result, err := p.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    args.ConsumerGroup,
		Consumer: args.ConsumerID,
		Streams:  streams,
		Count:    args.MaxMessages,
		Block:    block,
	}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var out []MessageRef
	for _, stream := range result {
		for _, msg := range stream.Messages {
			ref := xMessageToRef(msg, stream.Stream, p.prefix, 1)
			if ref != nil {
				out = append(out, *ref)
			}
		}
	}
	return out, nil
}

func (p *RedisStreamsProvider) Ack(ctx context.Context, messages []MessageRef) error {
	if len(messages) == 0 {
		return nil
	}
	byQueue := groupRefsByQueue(messages)
	for queue, msgs := range byQueue {
		ids := make([]string, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		if err := p.redis.XAck(ctx, wireStreamKey(p.prefix, queue), p.consumerGroup, ids...).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (p *RedisStreamsProvider) AckAndForget(ctx context.Context, messages []MessageRef) error {
	if len(messages) == 0 {
		return nil
	}
	if err := p.Ack(ctx, messages); err != nil {
		return err
	}
	byQueue := groupRefsByQueue(messages)
	for queue, msgs := range byQueue {
		ids := make([]string, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		p.redis.XDel(ctx, wireStreamKey(p.prefix, queue), ids...)
	}
	return nil
}

func (p *RedisStreamsProvider) ReclaimIdle(ctx context.Context, args ReclaimIdleArgs) ([]MessageRef, error) {
	maxCount := args.MaxCount
	if maxCount <= 0 {
		maxCount = 10
	}
	idle := time.Duration(args.IdleMs) * time.Millisecond
	var claimed []MessageRef

	for _, queue := range args.Queues {
		streamKey := wireStreamKey(p.prefix, queue)
		pending, err := p.redis.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamKey,
			Group:  args.ConsumerGroup,
			Idle:   idle,
			Start:  "-",
			End:    "+",
			Count:  maxCount,
		}).Result()
		if err != nil {
			continue
		}
		for _, pe := range pending {
			full, err := p.redis.XRange(ctx, streamKey, pe.ID, pe.ID).Result()
			if err != nil || len(full) == 0 {
				continue
			}
			peek := xMessageToRef(full[0], streamKey, p.prefix, int(pe.RetryCount))
			if peek == nil {
				continue
			}
			if peek.Backoff != nil {
				required := calculateBackoff(*peek.Backoff, int(pe.RetryCount))
				if pe.Idle < time.Duration(required)*time.Millisecond {
					continue
				}
			}
			claimedMsgs, err := p.redis.XClaim(ctx, &redis.XClaimArgs{
				Stream:   streamKey,
				Group:    args.ConsumerGroup,
				Consumer: args.ConsumerID,
				MinIdle:  idle,
				Messages: []string{pe.ID},
			}).Result()
			if err != nil || len(claimedMsgs) == 0 {
				continue
			}
			ref := xMessageToRef(claimedMsgs[0], streamKey, p.prefix, int(pe.RetryCount))
			if ref != nil {
				claimed = append(claimed, *ref)
			}
		}
	}
	return claimed, nil
}

func (p *RedisStreamsProvider) DeadLetter(ctx context.Context, message MessageRef, meta DeadLetterMeta) error {
	dlKey := wireDeadLetterKey(p.prefix, message.Queue)
	values := map[string]interface{}{
		WireFieldTaskName:       message.TaskName,
		WireFieldPayload:        string(message.Payload),
		WireFieldEnqueuedAt:     message.EnqueuedAt,
		WireFieldOriginalID:     meta.OriginalID,
		WireFieldDeliveryCount:  meta.DeliveryCount,
		WireFieldDeadLetteredAt: time.Now().UnixMilli(),
	}
	if meta.Error != "" {
		values[WireFieldError] = meta.Error
	} else {
		// Fallback: read last handler error recorded by the client.
		if errStr, err := p.redis.Get(ctx, wireErrorKey(p.prefix, message.ID)).Result(); err == nil && errStr != "" {
			values[WireFieldError] = errStr
		}
	}
	if err := p.redis.XAdd(ctx, &redis.XAddArgs{Stream: dlKey, Values: values}).Err(); err != nil {
		return err
	}
	p.redis.Del(ctx, wireErrorKey(p.prefix, message.ID))
	return p.Ack(ctx, []MessageRef{message})
}

// RecordError stores the last handler error for a message so DeadLetter can attach it.
func (p *RedisStreamsProvider) RecordError(ctx context.Context, messageID, errMsg string) {
	p.redis.Set(ctx, wireErrorKey(p.prefix, messageID), errMsg, time.Hour)
}

const processScheduledLua = `
local zsetKey = KEYS[1]
local cutoff = tonumber(ARGV[1])
local prefix = ARGV[2]
local defaultPriority = ARGV[3]

local tasks = redis.call('ZRANGEBYSCORE', zsetKey, '-inf', cutoff)
local processed = 0

for _, taskData in ipairs(tasks) do
    local ok, task = pcall(cjson.decode, taskData)
    if ok and task then
        local streamKey = task.streamKey or (prefix .. ':' .. (task.priority or defaultPriority))

        local args = {streamKey, '*', 'taskName', task.taskName or '', 'payload', task.payload or '{}', 'enqueuedAt', tostring(task.enqueuedAt or 0)}

        if task.attempts then
            table.insert(args, 'attempts')
            table.insert(args, tostring(task.attempts))
        end
        if task.backoff then
            table.insert(args, 'backoff')
            table.insert(args, task.backoff)
        end
        if task.timeout then
            table.insert(args, 'timeout')
            table.insert(args, tostring(task.timeout))
        end

        redis.call('XADD', unpack(args))
        redis.call('ZREM', zsetKey, taskData)
        processed = processed + 1
    end
end

return processed
`

func (p *RedisStreamsProvider) PromoteDueScheduled(ctx context.Context, nowMs int64) (int64, error) {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	result, err := p.redis.Eval(ctx, processScheduledLua, []string{wireScheduledKey(p.prefix)},
		nowMs, p.prefix, string(p.defaultPrio),
	).Result()
	if err != nil {
		return 0, err
	}
	count, _ := result.(int64)
	return count, nil
}

func (p *RedisStreamsProvider) EnsureBroadcast(ctx context.Context, consumerIdentity string, start BroadcastStart) error {
	group := wireBroadcastGroup(consumerIdentity)
	startID := "$"
	if start == BroadcastStartBeginning {
		startID = "0"
	}
	err := p.redis.XGroupCreateMkStream(ctx, wireBroadcastStream(p.prefix), group, startID).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (p *RedisStreamsProvider) Broadcast(ctx context.Context, taskName string, payload interface{}) (string, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	id, err := p.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: wireBroadcastStream(p.prefix),
		Values: map[string]interface{}{
			WireFieldTaskName:   taskName,
			WireFieldPayload:    string(payloadBytes),
			WireFieldEnqueuedAt: time.Now().UnixMilli(),
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd broadcast: %w", err)
	}
	return id, nil
}

func (p *RedisStreamsProvider) ConsumeBroadcast(ctx context.Context, consumerIdentity string, maxMessages int64, blockMs int64) ([]MessageRef, error) {
	group := wireBroadcastGroup(consumerIdentity)
	result, err := p.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumerIdentity,
		Streams:  []string{wireBroadcastStream(p.prefix), ">"},
		Count:    maxMessages,
		Block:    time.Duration(blockMs) * time.Millisecond,
	}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, nil // match TS: swallow and return empty
	}
	var out []MessageRef
	for _, stream := range result {
		for _, msg := range stream.Messages {
			ref := xMessageToRef(msg, stream.Stream, p.prefix, 1)
			if ref != nil {
				ref.Queue = "broadcast"
				out = append(out, *ref)
			}
		}
	}
	return out, nil
}

func (p *RedisStreamsProvider) AckBroadcast(ctx context.Context, consumerIdentity string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	group := wireBroadcastGroup(consumerIdentity)
	return p.redis.XAck(ctx, wireBroadcastStream(p.prefix), group, ids...).Err()
}

func (p *RedisStreamsProvider) CleanupBroadcastGhosts(ctx context.Context, idleMs int64) (int64, error) {
	stream := wireBroadcastStream(p.prefix)
	groups, err := p.redis.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return 0, nil
	}
	threshold := time.Duration(idleMs) * time.Millisecond
	var deleted int64
	for _, g := range groups {
		if !strings.HasPrefix(g.Name, "broadcast-") {
			continue
		}
		if p.isBroadcastGroupIdle(ctx, stream, g.Name, threshold) {
			if err := p.redis.XGroupDestroy(ctx, stream, g.Name).Err(); err == nil {
				deleted++
			}
		}
	}
	return deleted, nil
}

func (p *RedisStreamsProvider) isBroadcastGroupIdle(ctx context.Context, stream, groupName string, threshold time.Duration) bool {
	consumers, err := p.redis.XInfoConsumers(ctx, stream, groupName).Result()
	if err != nil {
		return false
	}
	if len(consumers) == 0 {
		return true
	}
	for _, c := range consumers {
		if c.Idle < threshold {
			return false
		}
	}
	return true
}

func xMessageToRef(msg redis.XMessage, streamKey, prefix string, deliveryCount int) *MessageRef {
	taskName, _ := msg.Values[WireFieldTaskName].(string)
	payloadStr, _ := msg.Values[WireFieldPayload].(string)
	enqueuedAt, _ := asInt64(msg.Values[WireFieldEnqueuedAt])
	attempts, _ := asInt64(msg.Values[WireFieldAttempts])
	timeout, _ := asInt64(msg.Values[WireFieldTimeout])

	var backoff *BackoffConfig
	if s, ok := msg.Values[WireFieldBackoff].(string); ok && s != "" {
		var b BackoffConfig
		if json.Unmarshal([]byte(s), &b) == nil {
			backoff = &b
		}
	}

	return &MessageRef{
		ID:            msg.ID,
		Queue:         queueFromStreamKey(prefix, streamKey),
		TaskName:      taskName,
		Payload:       json.RawMessage(payloadStr),
		EnqueuedAt:    enqueuedAt,
		DeliveryCount: deliveryCount,
		Attempts:      int(attempts),
		Backoff:       backoff,
		Timeout:       timeout,
	}
}

func groupRefsByQueue(messages []MessageRef) map[string][]MessageRef {
	m := make(map[string][]MessageRef)
	for _, msg := range messages {
		m[msg.Queue] = append(m[msg.Queue], msg)
	}
	return m
}

func calculateBackoff(config BackoffConfig, deliveryCount int) int64 {
	retries := deliveryCount - 1
	if retries < 0 {
		retries = 0
	}
	if config.Type == BackoffFixed {
		return config.Delay
	}
	if config.Type == BackoffExponential {
		power := retries - 1
		if power < 0 {
			power = 0
		}
		delay := config.Delay * int64(1<<uint(power))
		maxDelay := config.MaxDelay
		if maxDelay <= 0 {
			maxDelay = 3600000
		}
		if delay > maxDelay {
			return maxDelay
		}
		return delay
	}
	return 0
}
