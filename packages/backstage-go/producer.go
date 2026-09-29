package backstage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type BackoffType string

const (
	BackoffFixed       BackoffType = "fixed"
	BackoffExponential BackoffType = "exponential"
)

type BackoffConfig struct {
	Type     BackoffType `json:"type"`
	Delay    int64       `json:"delay"`
	MaxDelay int64       `json:"maxDelay"`
}

type DedupeConfig struct {
	Key string
	TTL time.Duration
}

type EnqueueOptions struct {
	Priority Priority
	Queue    string
	Delay    time.Duration
	Dedupe   *DedupeConfig
	Attempts int
	Backoff  *BackoffConfig
	Timeout  time.Duration
}

func (c *Client) Enqueue(ctx context.Context, taskName string, payload interface{}, opts ...EnqueueOptions) (string, error) {
	var opt EnqueueOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	if opt.Dedupe != nil {
		dedupe, err := RequireDedupe(c.provider.Name(), c.resolved)
		if err != nil {
			return "", err
		}
		ttl := opt.Dedupe.TTL
		if ttl == 0 {
			ttl = time.Hour
		}
		claimed, err := dedupe.Claim(ctx, opt.Dedupe.Key, ttl.Milliseconds())
		if err != nil {
			return "", fmt.Errorf("dedupe claim: %w", err)
		}
		if !claimed {
			return "", nil
		}
	}

	queue := string(PriorityDefault)
	if opt.Queue != "" {
		queue = opt.Queue
	} else if opt.Priority != "" {
		queue = string(opt.Priority)
	}

	meta := JobMeta{}
	if opt.Attempts > 0 {
		meta.Attempts = opt.Attempts
	}
	if opt.Backoff != nil {
		meta.Backoff = opt.Backoff
	}
	if opt.Timeout > 0 {
		meta.Timeout = opt.Timeout.Milliseconds()
	}

	job := OutgoingJob{
		Queue: queue, TaskName: taskName, Payload: payload,
		EnqueuedAt: time.Now().UnixMilli(), Meta: meta,
	}

	if opt.Delay > 0 {
		delays, err := RequireDelays(c.provider.Name(), c.resolved)
		if err != nil {
			return "", err
		}
		return delays.Schedule(ctx, job, time.Now().Add(opt.Delay).UnixMilli())
	}

	jobs, err := RequireJobs(c.provider.Name(), c.resolved)
	if err != nil {
		return "", err
	}
	return jobs.Publish(ctx, job)
}

func (c *Client) Schedule(ctx context.Context, taskName string, payload interface{}, delay time.Duration, opts ...EnqueueOptions) (string, error) {
	var opt EnqueueOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	opt.Delay = delay
	return c.Enqueue(ctx, taskName, payload, opt)
}

func (c *Client) Publish(ctx context.Context, topic string, payload interface{}) (string, error) {
	topics, err := RequireTopics(c.provider.Name(), c.resolved)
	if err != nil {
		return "", err
	}
	return topics.Publish(ctx, topic, payload)
}

func (c *Client) Subscribe(topic string, handler TopicHandler, opts ...SubscribeOption) {
	sub := pendingTopicSub{topic: topic, handler: handler, from: TopicFromLatest}
	for _, o := range opts {
		o(&sub)
	}
	c.subMu.Lock()
	c.pendingTopicSubs = append(c.pendingTopicSubs, sub)
	running := c.running.Load()
	c.subMu.Unlock()
	if running {
		go func() {
			if err := c.startTopicSub(context.Background(), sub); err != nil {
				c.logger.Error("Failed to start topic subscription", "error", err)
			}
		}()
	}
}

// Broadcast sends via the legacy Redis broadcast stream (not topics).
// Deprecated: prefer Publish/Subscribe.
func (c *Client) Broadcast(ctx context.Context, taskName string, payload interface{}) (string, error) {
	if c.redis == nil {
		return "", fmt.Errorf("Broadcast is Redis-only; use Publish/Subscribe topics")
	}
	payloadBytes, err := EncodePayload(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	return c.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: BroadcastStreamKey(c.config.Prefix),
		Values: map[string]interface{}{
			"taskName": taskName, "payload": string(payloadBytes), "enqueuedAt": time.Now().UnixMilli(),
		},
	}).Result()
}
