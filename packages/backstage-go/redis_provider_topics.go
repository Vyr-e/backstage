package backstage

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisTopics struct{ p *RedisStreamsProvider }

func (t *redisTopics) Name() string { return "redis-streams" }

func (t *redisTopics) Publish(ctx context.Context, topic string, payload interface{}) (string, error) {
	key := TopicStreamKey(t.p.prefix, topic)
	body, err := payloadJSON(payload)
	if err != nil {
		return "", err
	}
	return t.p.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: key, MaxLen: t.p.topicMaxLen, Approx: true,
		Values: map[string]interface{}{"payload": body, "publishedAt": time.Now().UnixMilli()},
	}).Result()
}

func (t *redisTopics) Subscribe(ctx context.Context, opts TopicSubscribeOptions, onMessage func(context.Context, TopicDelivery) error) (Subscription, error) {
	key := TopicStreamKey(t.p.prefix, opts.Topic)
	group := TopicFanoutGroup(opts.ConsumerID)
	if opts.Group != "" {
		group = TopicNamedGroup(opts.Group)
	}
	start := "$"
	if opts.From == TopicFromEarliest {
		start = "0"
	}
	if err := t.p.ensureGroup(ctx, key, group, start); err != nil {
		return nil, err
	}
	subCtx, cancel := context.WithCancel(ctx)
	var running atomic.Bool
	running.Store(true)

	go func() {
		tck := time.NewTicker(t.p.reclaimInterval)
		defer tck.Stop()
		for {
			select {
			case <-tck.C:
				if running.Load() {
					t.p.reclaimTopics(subCtx, key, group, opts, onMessage)
				}
			case <-subCtx.Done():
				return
			}
		}
	}()

	if opts.Group == "" {
		go func() {
			tck := time.NewTicker(time.Duration(t.p.topicGroupIdleMs) * time.Millisecond)
			defer tck.Stop()
			for {
				select {
				case <-tck.C:
					_, _ = t.p.cleanupIdleTopicGroups(subCtx, key)
				case <-subCtx.Done():
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for running.Load() {
			result, err := t.p.redis.XReadGroup(subCtx, &redis.XReadGroupArgs{
				Group: group, Consumer: opts.ConsumerID, Streams: []string{key, ">"},
				Count: 10, Block: t.p.blockTimeout,
			}).Result()
			if err == redis.Nil || err == context.Canceled {
				continue
			}
			if err != nil {
				if running.Load() {
					time.Sleep(500 * time.Millisecond)
				}
				continue
			}
			for _, stream := range result {
				for _, msg := range stream.Messages {
					t.p.handleTopicMsg(subCtx, key, group, opts.Topic, msg, 1, onMessage)
				}
			}
		}
	}()

	var once sync.Once
	return &redisSubscription{stopFn: func() {
		once.Do(func() {
			running.Store(false)
			cancel()
			select {
			case <-done:
			case <-time.After(100 * time.Millisecond):
			}
		})
	}}, nil
}

func (p *RedisStreamsProvider) handleTopicMsg(ctx context.Context, key, group, topic string, msg redis.XMessage, deliveryCount int, onMessage func(context.Context, TopicDelivery) error) {
	payloadStr, _ := msg.Values["payload"].(string)
	publishedAt, _ := asInt64(msg.Values["publishedAt"])
	d := &redisTopicDelivery{
		p: p, id: msg.ID, topic: topic, payload: json.RawMessage(payloadStr),
		publishedAt: publishedAt, deliveryCount: deliveryCount, streamKey: key, group: group,
	}
	if err := onMessage(ctx, d); err != nil {
		if deliveryCount >= p.maxDeliveries {
			if p.pctx != nil && p.pctx.Logger != nil {
				p.pctx.Logger.Error("Topic handler failed after max deliveries; dropping",
					"topic", topic, "id", msg.ID, "error", err)
			}
			_ = d.Ack(ctx)
		}
		return
	}
	_ = d.Ack(ctx)
}

func (p *RedisStreamsProvider) reclaimTopics(ctx context.Context, key, group string, opts TopicSubscribeOptions, onMessage func(context.Context, TopicDelivery) error) {
	idle := p.idleTimeout
	if idle <= 0 {
		idle = 60 * time.Second
	}
	pending, err := p.redis.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: key, Group: group, Idle: idle, Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		return
	}
	for _, entry := range pending {
		claimed, err := p.redis.XClaim(ctx, &redis.XClaimArgs{
			Stream: key, Group: group, Consumer: opts.ConsumerID, MinIdle: idle, Messages: []string{entry.ID},
		}).Result()
		if err != nil || len(claimed) == 0 {
			continue
		}
		p.handleTopicMsg(ctx, key, group, opts.Topic, claimed[0], int(entry.RetryCount)+1, onMessage)
	}
}

type redisTopicDelivery struct {
	p             *RedisStreamsProvider
	id, topic     string
	payload       json.RawMessage
	publishedAt   int64
	deliveryCount int
	streamKey     string
	group         string
}

func (d *redisTopicDelivery) ID() string               { return d.id }
func (d *redisTopicDelivery) Topic() string            { return d.topic }
func (d *redisTopicDelivery) Payload() json.RawMessage { return d.payload }
func (d *redisTopicDelivery) PublishedAt() int64       { return d.publishedAt }
func (d *redisTopicDelivery) DeliveryCount() int       { return d.deliveryCount }
func (d *redisTopicDelivery) Ack(ctx context.Context) error {
	return d.p.redis.XAck(ctx, d.streamKey, d.group, d.id).Err()
}

func (p *RedisStreamsProvider) cleanupIdleTopicGroups(ctx context.Context, stream string) (int, error) {
	groups, err := p.redis.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return 0, err
	}
	deleted := 0
	thresh := time.Duration(p.topicGroupIdleMs) * time.Millisecond
	for _, g := range groups {
		if len(g.Name) < 4 || g.Name[:4] != "sub:" {
			continue
		}
		consumers, err := p.redis.XInfoConsumers(ctx, stream, g.Name).Result()
		if err != nil {
			continue
		}
		allIdle := len(consumers) == 0
		if !allIdle {
			allIdle = true
			for _, c := range consumers {
				if c.Idle < thresh {
					allIdle = false
					break
				}
			}
		}
		if allIdle {
			if p.redis.XGroupDestroy(ctx, stream, g.Name).Err() == nil {
				deleted++
			}
		}
	}
	return deleted, nil
}
