// Package backstage consumer implementation.
// Handles worker pool management, job processing, and reliability features (reclaiming, dead-letter).
package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConsumerConfig for the worker.
type ConsumerConfig struct {
	BlockTimeout      time.Duration
	ReclaimerInterval time.Duration
	IdleTimeout       time.Duration
	MaxDeliveries     int
	GracePeriod       time.Duration
	Prefetch          int64 // Max messages per XREADGROUP (backpressure)
	Concurrency       int   // Max concurrent tasks (backpressure)
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

// Start begins processing tasks.
func (c *Client) Start(ctx context.Context, cfg ConsumerConfig) error {
	c.running = true

	// Create consumer groups
	if err := c.initConsumerGroups(ctx); err != nil {
		return fmt.Errorf("init consumer groups: %w", err)
	}

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		log.Println("[Backstage] Shutting down...")
		c.running = false
	}()

	// Start ACK flusher
	go c.runAckFlusher(ctx)

	// Start reclaimer
	go c.runReclaimer(ctx, cfg)

	// Start scheduled task processor
	go c.processScheduled(ctx)

	// Main loop
	return c.processLoop(ctx, cfg)
}

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
				c.redis.XAck(ctx, req.stream, c.config.ConsumerGroup, ids...)
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
			c.redis.XAck(ctx, stream, c.config.ConsumerGroup, ids...)
			c.pendingAcks[stream] = nil
		}
	}
}

func (c *Client) queueAck(stream, id string) {
	c.ackChan <- ackRequest{stream: stream, id: id}
}

// Stop stops the worker.
func (c *Client) Stop() {
	c.running = false
	close(c.ackChan)
}

func (c *Client) initConsumerGroups(ctx context.Context) error {
	var streams []string
	
	// Default priority queues
	priorities := []Priority{PriorityUrgent, PriorityDefault, PriorityLow}
	for _, p := range priorities {
		streams = append(streams, c.streamKey(p))
	}

	// Custom registered queues
	c.queuesMu.RLock()
	for _, q := range c.customQueues {
		streams = append(streams, fmt.Sprintf("%s:%s", c.config.Prefix, q))
	}
	c.queuesMu.RUnlock()

	for _, key := range streams {
		err := c.redis.XGroupCreateMkStream(ctx, key, c.config.ConsumerGroup, "0").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("XGroupCreate for %s: %w", key, err)
		}
	}

	return nil
}


func (c *Client) processLoop(ctx context.Context, cfg ConsumerConfig) error {
	var streams []string
	
	// Default priority queues
	priorities := []Priority{PriorityUrgent, PriorityDefault, PriorityLow}
	for _, p := range priorities {
		streams = append(streams, c.streamKey(p))
	}

	// Custom registered queues
	c.queuesMu.RLock()
	for _, q := range c.customQueues {
		streams = append(streams, fmt.Sprintf("%s:%s", c.config.Prefix, q))
	}
	c.queuesMu.RUnlock()

	// Append ">" for each stream to read new messages
	for range streams {
		streams = append(streams, ">")
	}

	// Semaphore for concurrency control (backpressure)
	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup

	for c.running {
		// Calculate available capacity
		available := cfg.Concurrency - len(sem)
		if available <= 0 {
			// At capacity - wait for a slot
			time.Sleep(10 * time.Millisecond)
			continue
		}

		count := cfg.Prefetch
		if int64(available) < count {
			count = int64(available)
		}

		result, err := c.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.config.ConsumerGroup,
			Consumer: c.config.WorkerID,
			Streams:  streams,
			Count:    count,
			Block:    cfg.BlockTimeout,
		}).Result()

		if err == redis.Nil {
			continue
		}
		if err != nil {
			if c.running {
				log.Printf("[Backstage] Read error: %v", err)
				time.Sleep(time.Second)
			}
			continue
		}

		for _, stream := range result {
			for _, msg := range stream.Messages {
				// Acquire semaphore slot
				sem <- struct{}{}
				wg.Add(1)

				// Process concurrently
				go func(streamKey string, m redis.XMessage) {
					defer func() {
						<-sem // Release slot
						wg.Done()
					}()
					c.handleMessage(ctx, streamKey, m)
				}(stream.Stream, msg)
			}
		}
	}

	// Wait for in-flight tasks
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

	// Create a context for the task
	taskCtx := ctx
	if timeoutMs > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()
	}

	result, err := handler(taskCtx, json.RawMessage(payloadStr))
	if err != nil {
		log.Printf("[Backstage] Task failed: %s - %v", taskName, err)
		return // Don't ACK - let reclaimer handle
	}

	// Handle workflow chaining
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

func (c *Client) reclaimIdleMessages(ctx context.Context, cfg ConsumerConfig) {
	var streams []string
	
	// Default priority queues
	priorities := []Priority{PriorityUrgent, PriorityDefault, PriorityLow}
	for _, p := range priorities {
		streams = append(streams, c.streamKey(p))
	}

	// Custom registered queues
	c.queuesMu.RLock()
	for _, q := range c.customQueues {
		streams = append(streams, fmt.Sprintf("%s:%s", c.config.Prefix, q))
	}
	c.queuesMu.RUnlock()

	for _, key := range streams {
		pending, err := c.redis.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: key,
			Group:  c.config.ConsumerGroup,
			Idle:   cfg.IdleTimeout,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()

		if err != nil {
			continue
		}

		for _, msg := range pending {
			// Fetch full message to check backoff
			fullMsg, err := c.redis.XRange(ctx, key, msg.ID, msg.ID).Result()
			if err != nil || len(fullMsg) == 0 {
				continue
			}
			redisMsg := fullMsg[0]

			// Check backoff
			backoffJSON, _ := redisMsg.Values["backoff"].(string)
			if backoffJSON != "" {
				var backoff BackoffConfig
				if err := json.Unmarshal([]byte(backoffJSON), &backoff); err == nil {
					requiredWait := c.calculateBackoff(backoff, int(msg.RetryCount))
					if msg.Idle < time.Duration(requiredWait)*time.Millisecond {
						continue // Not ready yet
					}
				}
			}

			claimed, err := c.redis.XClaim(ctx, &redis.XClaimArgs{
				Stream:   key,
				Group:    c.config.ConsumerGroup,
				Consumer: c.config.WorkerID,
				MinIdle:  cfg.IdleTimeout,
				Messages: []string{msg.ID},
			}).Result()

			if err != nil || len(claimed) == 0 {
				continue
			}

			if msg.RetryCount > int64(cfg.MaxDeliveries) {
				// Determine priority for DLQ
				priority := PriorityDefault
				if key == c.streamKey(PriorityUrgent) {
					priority = PriorityUrgent
				} else if key == c.streamKey(PriorityLow) {
					priority = PriorityLow
				}
				c.moveToDeadLetter(ctx, priority, claimed[0])
			} else {
				c.handleMessage(ctx, key, claimed[0])
			}
		}
	}
}

func (c *Client) calculateBackoff(config BackoffConfig, attempts int) int64 {
	retries := attempts - 1
	if retries < 0 {
		retries = 0
	}

	if config.Type == BackoffFixed {
		return config.Delay
	}

	if config.Type == BackoffExponential {
		// delay * 2^(retries-1)
		power := retries - 1
		if power < 0 {
			power = 0
		}
		delay := config.Delay * int64(1<<power)
		if config.MaxDelay > 0 && delay > config.MaxDelay {
			return config.MaxDelay
		}
		return delay
	}

	return 0
}

func (c *Client) moveToDeadLetter(ctx context.Context, priority Priority, msg redis.XMessage) {
	dlKey := c.deadLetterKey(priority)
	sKey := c.streamKey(priority)

	c.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: dlKey,
		Values: map[string]interface{}{
			"taskName":       msg.Values["taskName"],
			"payload":        msg.Values["payload"],
			"enqueuedAt":     msg.Values["enqueuedAt"],
			"originalId":     msg.ID,
			"deadLetteredAt": time.Now().UnixMilli(),
		},
	})

	c.ack(ctx, sKey, msg.ID)
}

// Lua script for atomic scheduled task processing
const processScheduledLuaConsumer = `
local zsetKey = KEYS[1]
local cutoff = tonumber(ARGV[1])
local prefix = ARGV[2]
local defaultPriority = ARGV[3]

local tasks = redis.call('ZRANGEBYSCORE', zsetKey, '-inf', cutoff)
local processed = 0

for _, taskData in ipairs(tasks) do
    local ok, task = pcall(cjson.decode, taskData)
    if ok and task then
        local priority = task.priority or defaultPriority
        local streamKey = prefix .. ':' .. priority
        
        redis.call('XADD', streamKey, '*',
            'taskName', task.taskName or '',
            'payload', task.payload or '{}',
            'enqueuedAt', tostring(task.enqueuedAt or 0)
        )
        
        redis.call('ZREM', zsetKey, taskData)
        processed = processed + 1
    end
end

return processed
`

func (c *Client) processScheduled(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for c.running {
		select {
		case <-ticker.C:
			now := time.Now().UnixMilli()

			// Use atomic Lua script to prevent race conditions
			c.redis.Eval(ctx, processScheduledLuaConsumer, []string{c.scheduledKey()},
				now,
				c.config.Prefix,
				string(PriorityDefault),
			)

		case <-ctx.Done():
			return
		}
	}
}

