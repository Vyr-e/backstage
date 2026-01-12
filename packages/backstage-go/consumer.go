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

	// Start reclaimer
	go c.runReclaimer(ctx, cfg)

	// Start scheduled task processor
	go c.processScheduled(ctx)

	// Main loop
	return c.processLoop(ctx, cfg)
}

// Stop stops the worker.
func (c *Client) Stop() {
	c.running = false
}

func (c *Client) initConsumerGroups(ctx context.Context) error {
	priorities := []Priority{PriorityUrgent, PriorityDefault, PriorityLow}

	for _, p := range priorities {
		key := streamKey(p)
		err := c.redis.XGroupCreateMkStream(ctx, key, c.config.ConsumerGroup, "0").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return err
		}
	}

	return nil
}

func (c *Client) processLoop(ctx context.Context, cfg ConsumerConfig) error {
	streams := []string{
		streamKey(PriorityUrgent),
		streamKey(PriorityDefault),
		streamKey(PriorityLow),
		">", ">", ">",
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

	handler, ok := c.handlers[taskName]
	if !ok {
		log.Printf("[Backstage] Unknown task: %s", taskName)
		c.ack(ctx, streamKey, msg.ID)
		return
	}

	result, err := handler(ctx, json.RawMessage(payloadStr))
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

	c.ack(ctx, streamKey, msg.ID)
}

func (c *Client) ack(ctx context.Context, stream, id string) {
	c.redis.XAck(ctx, stream, c.config.ConsumerGroup, id)
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
	priorities := []Priority{PriorityUrgent, PriorityDefault, PriorityLow}

	for _, p := range priorities {
		key := streamKey(p)

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
				c.moveToDeadLetter(ctx, p, claimed[0])
			} else {
				c.handleMessage(ctx, key, claimed[0])
			}
		}
	}
}

func (c *Client) moveToDeadLetter(ctx context.Context, priority Priority, msg redis.XMessage) {
	dlKey := deadLetterKey(priority)
	sKey := streamKey(priority)

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
			c.redis.Eval(ctx, processScheduledLuaConsumer, []string{scheduledKey()},
				now,
				StreamPrefix,
				string(PriorityDefault),
			)

		case <-ctx.Done():
			return
		}
	}
}

