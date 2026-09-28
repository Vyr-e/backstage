package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

type ConsumerConfig struct {
	BlockTimeout      time.Duration
	ReclaimerInterval time.Duration
	IdleTimeout       time.Duration
	MaxDeliveries     int
	GracePeriod       time.Duration
	Prefetch          int64
	Concurrency       int
}

func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		BlockTimeout: 5 * time.Second, ReclaimerInterval: 30 * time.Second,
		IdleTimeout: 60 * time.Second, MaxDeliveries: 5, GracePeriod: 30 * time.Second,
		Prefetch: 10, Concurrency: 50,
	}
}

func (c *Client) On(taskName string, handler Handler) {
	c.handlers[taskName] = handler
}

func (c *Client) Start(ctx context.Context, cfg ConsumerConfig) error {
	if c.running {
		return fmt.Errorf("worker is already running")
	}
	c.running = true
	c.consumerCfg = cfg

	if len(c.pendingTopicSubs) > 0 {
		if _, err := RequireTopics(c.provider.Name(), c.resolved); err != nil {
			c.running = false
			return err
		}
	}
	for _, req := range c.resolved.Jobs.Requires() {
		switch req {
		case CapabilityDelays:
			if _, err := RequireDelays(c.provider.Name(), c.resolved); err != nil {
				c.running = false
				return err
			}
		case CapabilityDedupe:
			if _, err := RequireDedupe(c.provider.Name(), c.resolved); err != nil {
				c.running = false
				return err
			}
		case CapabilityTopics:
			if _, err := RequireTopics(c.provider.Name(), c.resolved); err != nil {
				c.running = false
				return err
			}
		}
	}

	c.logger.Info("Worker starting", "id", c.config.WorkerID)
	c.logger.Info("\n" + FormatCapabilityReport(c.Capabilities()))

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		log.Println("[Backstage] Shutting down...")
		c.Stop()
	}()

	queues := c.queueNames()
	_ = c.resolved.Jobs.EnsureQueues(ctx, queues)

	prefetch := int(cfg.Prefetch)
	if int(cfg.Concurrency) > prefetch {
		prefetch = cfg.Concurrency
	}

	sub, err := c.resolved.Jobs.Consume(ctx, ConsumeOptions{
		Queues: queues, Group: c.config.ConsumerGroup, ConsumerID: c.config.WorkerID,
		Prefetch: prefetch, IdleTimeout: cfg.IdleTimeout.Milliseconds(),
	}, c.onJobDelivery)
	if err != nil {
		c.running = false
		return err
	}
	c.jobSub = sub

	for _, tsub := range c.pendingTopicSubs {
		if err := c.startTopicSub(ctx, tsub); err != nil {
			c.logger.Error("topic subscribe failed", "error", err)
		}
	}

	// Single promote loop in the worker only
	if c.resolved.Delays != nil && c.redisProvider != nil {
		c.promoteStop = make(chan struct{})
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					_, _ = c.redisProvider.PromoteCrossProvider(context.Background())
				case <-c.promoteStop:
					return
				}
			}
		}()
	}

	// Block until stopped (master behavior)
	for c.running {
		select {
		case <-ctx.Done():
			c.Stop()
		case <-time.After(100 * time.Millisecond):
		}
	}

	if c.jobSub != nil {
		_ = c.jobSub.Stop(context.Background())
		c.jobSub = nil
	}
	for _, s := range c.topicSubs {
		_ = s.Stop(context.Background())
	}
	c.topicSubs = nil

	return nil
}

func (c *Client) startTopicSub(ctx context.Context, sub pendingTopicSub) error {
	topics, err := RequireTopics(c.provider.Name(), c.resolved)
	if err != nil {
		return err
	}
	s, err := topics.Subscribe(ctx, TopicSubscribeOptions{
		Topic: sub.topic, Group: sub.group, ConsumerID: c.config.WorkerID, From: sub.from,
	}, func(ctx context.Context, m TopicDelivery) error {
		return sub.handler(ctx, m.Payload(), TopicMessage{
			ID: m.ID(), Topic: m.Topic(), PublishedAt: m.PublishedAt(),
		})
	})
	if err != nil {
		return err
	}
	c.subMu.Lock()
	c.topicSubs = append(c.topicSubs, s)
	c.subMu.Unlock()
	return nil
}

func (c *Client) onJobDelivery(ctx context.Context, d JobDelivery) error {
	handler, ok := c.handlers[d.TaskName()]
	if !ok {
		log.Printf("[Backstage] Unknown task: %s", d.TaskName())
		return d.Ack(ctx)
	}

	taskCtx := ctx
	var cancel context.CancelFunc
	if d.Meta().Timeout > 0 {
		taskCtx, cancel = context.WithTimeout(ctx, time.Duration(d.Meta().Timeout)*time.Millisecond)
		defer cancel()
	}

	result, err := handler(taskCtx, d.Payload())
	if err != nil {
		log.Printf("[Backstage] Task failed: %s - %v", d.TaskName(), err)
		max := c.consumerCfg.MaxDeliveries
		if d.Meta().Attempts > 0 {
			max = d.Meta().Attempts
		}
		if d.DeliveryCount() > max {
			return d.DeadLetter(ctx, DeadLetterOpts{Error: err.Error()})
		}
		delayMs := c.consumerCfg.IdleTimeout.Milliseconds()
		if d.Meta().Backoff != nil {
			delayMs = ComputeBackoff(*d.Meta().Backoff, d.DeliveryCount())
		}
		return d.Retry(ctx, RetryOpts{DelayMs: delayMs, Error: err.Error()})
	}

	if result != nil {
		if result.Delay > 0 {
			_, _ = c.Schedule(ctx, result.Next, result.Payload, time.Duration(result.Delay)*time.Millisecond)
		} else {
			_, _ = c.Enqueue(ctx, result.Next, result.Payload)
		}
	}
	return d.Ack(ctx)
}

func (c *Client) Stop() {
	if !c.running {
		return
	}
	c.running = false
	if c.promoteStop != nil {
		select {
		case <-c.promoteStop:
		default:
			close(c.promoteStop)
		}
	}
	// Grace period is applied by callers waiting after Stop, matching prior behavior
	// where processLoop waited GracePeriod for in-flight work.
	if c.ackChan != nil {
		select {
		case <-c.ackChan:
		default:
			// may already be closed in tests
		}
	}
}

// --- legacy helpers kept for existing tests ---

func (c *Client) runAckFlusher(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case req, ok := <-c.ackChan:
			if !ok {
				c.flushAllAcks(ctx)
				return
			}
			c.ackMu.Lock()
			c.pendingAcks[req.stream] = append(c.pendingAcks[req.stream], req.id)
			if len(c.pendingAcks[req.stream]) >= 100 {
				ids := c.pendingAcks[req.stream]
				c.pendingAcks[req.stream] = nil
				c.ackMu.Unlock()
				c.ackAndMaybeDelete(ctx, req.stream, ids)
			} else {
				c.ackMu.Unlock()
			}
		case <-ticker.C:
			c.flushAllAcks(ctx)
		case <-ctx.Done():
			c.flushAllAcks(context.Background())
			return
		}
	}
}

func (c *Client) flushAllAcks(ctx context.Context) {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	for stream, ids := range c.pendingAcks {
		if len(ids) > 0 {
			c.ackAndMaybeDelete(ctx, stream, ids)
			c.pendingAcks[stream] = nil
		}
	}
}

func (c *Client) ackAndMaybeDelete(ctx context.Context, stream string, ids []string) {
	if len(ids) == 0 || c.redis == nil {
		return
	}
	c.redis.XAck(ctx, stream, c.config.ConsumerGroup, ids...)
	if c.config.DeleteOnAck {
		c.redis.XDel(ctx, stream, ids...)
	}
}

func (c *Client) queueAck(stream, id string) {
	select {
	case c.ackChan <- ackRequest{stream: stream, id: id}:
	default:
		if c.redis != nil {
			c.redis.XAck(context.Background(), stream, c.config.ConsumerGroup, id)
		}
	}
}

func (c *Client) initConsumerGroups(ctx context.Context) error {
	for _, key := range c.getQueues() {
		err := c.redis.XGroupCreateMkStream(ctx, key, c.config.ConsumerGroup, "0").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("XGroupCreate for %s: %w", key, err)
		}
	}
	return nil
}

const lastErrorTTL = time.Hour

// handleMessage is retained for existing tests that drive Redis messages directly.
func (c *Client) handleMessage(ctx context.Context, streamKey string, msg redis.XMessage) {
	taskName, _ := msg.Values["taskName"].(string)
	payloadStr, _ := msg.Values["payload"].(string)
	timeoutMs, _ := asInt64(msg.Values["timeout"])

	handler, ok := c.handlers[taskName]
	if !ok {
		log.Printf("[Backstage] Unknown task: %s", taskName)
		c.queueAck(streamKey, msg.ID)
		return
	}

	taskCtx := ctx
	if timeoutMs > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()
	}

	result, err := handler(taskCtx, json.RawMessage(payloadStr))
	if err != nil {
		log.Printf("[Backstage] Task failed: %s - %v", taskName, err)
		c.redis.Set(ctx, c.errorKey(msg.ID), err.Error(), lastErrorTTL)
		return
	}
	if result != nil {
		if result.Delay > 0 {
			c.Schedule(ctx, result.Next, result.Payload, time.Duration(result.Delay)*time.Millisecond)
		} else {
			c.Enqueue(ctx, result.Next, result.Payload)
		}
	}
	c.queueAck(streamKey, msg.ID)
}

func (c *Client) ack(ctx context.Context, stream, id string) {
	c.queueAck(stream, id)
}

func (c *Client) runReclaimer(ctx context.Context, cfg ConsumerConfig) {
	ticker := time.NewTicker(cfg.ReclaimerInterval)
	defer ticker.Stop()
	for c.running {
		select {
		case <-ticker.C:
			c.reclaimIdleMessages(ctx, cfg)
		case <-ctx.Done():
			return
		}
	}
}

// reclaimIdleMessages kept for TestBackoffReclaimer; uses ComputeBackoff and
// ACKs the original stream on dead-letter (custom-queue bug fix).
func (c *Client) reclaimIdleMessages(ctx context.Context, cfg ConsumerConfig) {
	if c.redis == nil {
		return
	}
	for _, key := range c.getQueues() {
		pending, err := c.redis.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: key, Group: c.config.ConsumerGroup, Idle: cfg.IdleTimeout,
			Start: "-", End: "+", Count: 10,
		}).Result()
		if err != nil {
			continue
		}
		for _, msg := range pending {
			fullMsg, err := c.redis.XRange(ctx, key, msg.ID, msg.ID).Result()
			if err != nil || len(fullMsg) == 0 {
				continue
			}
			redisMsg := fullMsg[0]
			backoffJSON, _ := redisMsg.Values["backoff"].(string)
			if backoffJSON != "" {
				var backoff BackoffConfig
				if err := json.Unmarshal([]byte(backoffJSON), &backoff); err == nil {
					requiredWait := ComputeBackoff(backoff, int(msg.RetryCount))
					if msg.Idle < time.Duration(requiredWait)*time.Millisecond {
						continue
					}
				}
			}
			claimed, err := c.redis.XClaim(ctx, &redis.XClaimArgs{
				Stream: key, Group: c.config.ConsumerGroup, Consumer: c.config.WorkerID,
				MinIdle: cfg.IdleTimeout, Messages: []string{msg.ID},
			}).Result()
			if err != nil || len(claimed) == 0 {
				continue
			}
			if msg.RetryCount > int64(cfg.MaxDeliveries) {
				c.moveToDeadLetterStream(ctx, key, claimed[0], int(msg.RetryCount))
			} else {
				c.handleMessage(ctx, key, claimed[0])
			}
		}
	}
}

func (c *Client) calculateBackoff(config BackoffConfig, attempts int) int64 {
	return ComputeBackoff(config, attempts)
}

func (c *Client) moveToDeadLetter(ctx context.Context, priority Priority, msg redis.XMessage) {
	c.moveToDeadLetterStream(ctx, c.streamKey(priority), msg, 0)
}

func (c *Client) moveToDeadLetterStream(ctx context.Context, streamKey string, msg redis.XMessage, deliveryCount int) {
	queue := QueueFromStreamKey(c.config.Prefix, streamKey)
	dlKey := DeadLetterKey(c.config.Prefix, queue)
	errKey := c.errorKey(msg.ID)
	lastError, _ := c.redis.Get(ctx, errKey).Result()
	values := map[string]interface{}{
		"taskName": msg.Values["taskName"], "payload": msg.Values["payload"],
		"enqueuedAt": msg.Values["enqueuedAt"], "originalId": msg.ID,
		"deadLetteredAt": time.Now().UnixMilli(), "error": lastError,
	}
	if deliveryCount > 0 {
		values["deliveryCount"] = deliveryCount
	}
	c.redis.XAdd(ctx, &redis.XAddArgs{Stream: dlKey, Values: values})
	c.redis.Del(ctx, errKey)
	// ACK the ORIGINAL stream (custom-queue bug fix)
	c.redis.XAck(ctx, streamKey, c.config.ConsumerGroup, msg.ID)
}

func (c *Client) processScheduled(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for c.running {
		select {
		case <-ticker.C:
			if c.redisProvider != nil {
				_, _ = c.redisProvider.PromoteCrossProvider(ctx)
			} else if c.redis != nil {
				c.redis.Eval(ctx, ProcessScheduledLua, []string{c.scheduledKey()},
					time.Now().UnixMilli(), c.config.Prefix, string(PriorityDefault))
			}
		case <-ctx.Done():
			return
		}
	}
}

// silence unused imports used by legacy paths
var _ = strings.Contains
var _ = sync.WaitGroup{}
