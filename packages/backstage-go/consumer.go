package backstage

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
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
	if c.initErr != nil {
		return c.initErr
	}
	if c.running {
		return fmt.Errorf("worker is already running")
	}
	c.running = true
	c.consumerCfg = cfg
	c.activeWg = sync.WaitGroup{}

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
	c.activeWg.Add(1)
	defer c.activeWg.Done()

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
	if err == nil && result != nil {
		if result.Delay > 0 {
			_, err = c.Schedule(ctx, result.Next, result.Payload, time.Duration(result.Delay)*time.Millisecond)
		} else {
			_, err = c.Enqueue(ctx, result.Next, result.Payload)
		}
		if err != nil {
			log.Printf("[Backstage] Chaining failed: %s - %v", d.TaskName(), err)
		}
	}
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

	grace := c.consumerCfg.GracePeriod
	if grace <= 0 {
		grace = 30 * time.Second
	}
	done := make(chan struct{})
	go func() {
		c.activeWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
		c.logger.Warn("Force exiting with unfinished tasks after grace period",
			"grace", grace.String())
	}
}
