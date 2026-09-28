package backstage

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ConsumerConfig for the worker.
type ConsumerConfig struct {
	BlockTimeout      time.Duration
	ReclaimerInterval time.Duration
	IdleTimeout       time.Duration
	MaxDeliveries     int
	GracePeriod       time.Duration
	Prefetch          int64
	Concurrency       int
}

// DefaultConsumerConfig returns sensible defaults.
func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		BlockTimeout:      5 * time.Second,
		ReclaimerInterval: 30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxDeliveries:     5,
		GracePeriod:       30 * time.Second,
		Prefetch:          10,
		Concurrency:       50,
	}
}

// On registers a task handler.
func (c *Client) On(taskName string, handler Handler) {
	c.handlers[taskName] = handler
}

// Start begins processing tasks through the Provider.
func (c *Client) Start(ctx context.Context, cfg ConsumerConfig) error {
	c.running = true

	if err := c.initConsumerGroups(ctx); err != nil {
		return fmt.Errorf("init consumer groups: %w", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		log.Println("[Backstage] Shutting down...")
		c.running = false
	}()

	go c.runAckFlusher(ctx)
	go c.runReclaimer(ctx, cfg)
	go c.processScheduled(ctx)

	return c.processLoop(ctx, cfg)
}

func (c *Client) runAckFlusher(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-c.ackChan:
			if !ok {
				c.flushAllAcks(ctx)
				return
			}
			c.ackMu.Lock()
			c.pendingAcks = append(c.pendingAcks, msg)
			flush := len(c.pendingAcks) >= 100
			var batch []MessageRef
			if flush {
				batch = c.pendingAcks
				c.pendingAcks = nil
			}
			c.ackMu.Unlock()
			if flush {
				c.flushAckBatch(ctx, batch)
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
	batch := c.pendingAcks
	c.pendingAcks = nil
	c.ackMu.Unlock()
	c.flushAckBatch(ctx, batch)
}

func (c *Client) flushAckBatch(ctx context.Context, messages []MessageRef) {
	if len(messages) == 0 {
		return
	}
	var err error
	if c.config.DeleteOnAck {
		err = c.provider.AckAndForget(ctx, messages)
	} else {
		err = c.provider.Ack(ctx, messages)
	}
	if err != nil {
		log.Printf("[Backstage] ACK failed: %v", err)
	}
}

func (c *Client) queueAck(msg MessageRef) {
	select {
	case c.ackChan <- msg:
	default:
		// Channel full — ack synchronously to avoid unbounded growth.
		c.flushAckBatch(context.Background(), []MessageRef{msg})
	}
}

// Stop stops the worker.
func (c *Client) Stop() {
	c.running = false
	select {
	case <-c.ackChan:
	default:
	}
	// Close ack channel once; recover if already closed.
	func() {
		defer func() { recover() }()
		close(c.ackChan)
	}()
}

func (c *Client) initConsumerGroups(ctx context.Context) error {
	if rp, ok := c.provider.(*RedisStreamsProvider); ok {
		rp.SetConsumerGroup(c.config.ConsumerGroup)
	}
	return c.provider.EnsureQueues(ctx, c.getQueueNames())
}

func (c *Client) processLoop(ctx context.Context, cfg ConsumerConfig) error {
	queues := c.getQueueNames()
	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup

	for c.running {
		available := cfg.Concurrency - len(sem)
		if available <= 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		count := cfg.Prefetch
		if int64(available) < count {
			count = int64(available)
		}

		messages, err := c.provider.Consume(ctx, ConsumeArgs{
			Queues:        queues,
			ConsumerGroup: c.config.ConsumerGroup,
			ConsumerID:    c.config.WorkerID,
			MaxMessages:   count,
			BlockMs:       cfg.BlockTimeout.Milliseconds(),
		})
		if err != nil {
			if c.running {
				if strings.Contains(err.Error(), "NOGROUP") {
					log.Printf("[Backstage] Consumer group missing, recreating: %v", err)
					if rerr := c.initConsumerGroups(ctx); rerr != nil {
						log.Printf("[Backstage] Failed to recreate consumer groups: %v", rerr)
						time.Sleep(time.Second)
					}
					continue
				}
				log.Printf("[Backstage] Read error: %v", err)
				time.Sleep(time.Second)
			}
			continue
		}
		if len(messages) == 0 {
			continue
		}

		for _, msg := range messages {
			sem <- struct{}{}
			wg.Add(1)
			go func(m MessageRef) {
				defer func() {
					<-sem
					wg.Done()
				}()
				c.handleMessage(ctx, m)
			}(msg)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(cfg.GracePeriod):
		log.Printf("[Backstage] Grace period expired, forcing shutdown")
	}
	return nil
}

func (c *Client) handleMessage(ctx context.Context, msg MessageRef) {
	handler, ok := c.handlers[msg.TaskName]
	if !ok {
		log.Printf("[Backstage] Unknown task: %s", msg.TaskName)
		c.queueAck(msg)
		return
	}

	taskCtx := ctx
	if msg.Timeout > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(ctx, time.Duration(msg.Timeout)*time.Millisecond)
		defer cancel()
	}

	result, err := handler(taskCtx, msg.Payload)
	if err != nil {
		log.Printf("[Backstage] Task failed: %s - %v", msg.TaskName, err)
		c.lastErrors.Store(msg.ID, err.Error())
		if rp, ok := c.provider.(*RedisStreamsProvider); ok {
			rp.RecordError(ctx, msg.ID, err.Error())
		}
		return
	}

	if result != nil {
		if result.Delay > 0 {
			c.Schedule(ctx, result.Next, result.Payload, time.Duration(result.Delay)*time.Millisecond)
		} else {
			c.Enqueue(ctx, result.Next, result.Payload)
		}
	}

	c.queueAck(msg)
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

func (c *Client) reclaimIdleMessages(ctx context.Context, cfg ConsumerConfig) {
	claimed, err := c.provider.ReclaimIdle(ctx, ReclaimIdleArgs{
		Queues:        c.getQueueNames(),
		ConsumerGroup: c.config.ConsumerGroup,
		ConsumerID:    c.config.WorkerID,
		IdleMs:        cfg.IdleTimeout.Milliseconds(),
		MaxCount:      10,
	})
	if err != nil {
		return
	}

	for _, msg := range claimed {
		if msg.DeliveryCount > cfg.MaxDeliveries {
			errStr := ""
			if v, ok := c.lastErrors.Load(msg.ID); ok {
				errStr, _ = v.(string)
			}
			_ = c.provider.DeadLetter(ctx, msg, DeadLetterMeta{
				OriginalID:    msg.ID,
				DeliveryCount: msg.DeliveryCount,
				Error:         errStr,
			})
			c.lastErrors.Delete(msg.ID)
		} else {
			c.handleMessage(ctx, msg)
		}
	}
}

func (c *Client) processScheduled(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for c.running {
		select {
		case <-ticker.C:
			_, _ = c.provider.PromoteDueScheduled(ctx, 0)
		case <-ctx.Done():
			return
		}
	}
}
