// Package backstage broadcast implementation.
// Implements a fan-out messaging pattern where tasks are delivered to all active workers.
package backstage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// BroadcastStream is the Redis stream key used for broadcast messages.
const BroadcastStream = "backstage:broadcast"

// BroadcastConfig for the broadcast listener.
type BroadcastConfig struct {
	ConsumerIdleThreshold time.Duration // Threshold for ghost consumer cleanup
	BlockTimeout          time.Duration
}

// DefaultBroadcastConfig returns sensible defaults.
func DefaultBroadcastConfig() BroadcastConfig {
	return BroadcastConfig{
		ConsumerIdleThreshold: time.Hour,
		BlockTimeout:          5 * time.Second,
	}
}

// BroadcastMessage represents a message from the broadcast stream.
type BroadcastMessage struct {
	ID         string
	TaskName   string
	Payload    json.RawMessage
	EnqueuedAt int64
}

// BroadcastHandler is called for each broadcast message.
type BroadcastHandler func(ctx context.Context, msg BroadcastMessage) error

// BroadcastListener listens for broadcast messages on all workers.
// Unlike standard queues where one worker processes a message, broadcast messages
// are delivered to ALL active workers (fan-out). This is achieved by creating
// a unique consumer group for each worker instance.
type BroadcastListener struct {
	redis         *redis.Client
	consumerGroup string
	consumerID    string
	handler       BroadcastHandler
	config        BroadcastConfig
	running       bool
	logger        *Logger
}

// NewBroadcastListener creates a broadcast listener.
// Each worker gets its own consumer group to receive all messages.
func NewBroadcastListener(rdb *redis.Client, workerID string, handler BroadcastHandler, config BroadcastConfig) *BroadcastListener {
	return &BroadcastListener{
		redis:         rdb,
		consumerGroup: "broadcast-" + workerID,
		consumerID:    workerID,
		handler:       handler,
		config:        config,
		logger:        NewLogger("Broadcast"),
	}
}

// Start begins listening for broadcast messages.
// It runs a blocking loop that consumes messages from the worker's unique group.
// The method returns when the context is canceled or Stop() is called.
func (b *BroadcastListener) Start(ctx context.Context) error {
	// Create unique consumer group for this worker
	err := b.redis.XGroupCreateMkStream(ctx, BroadcastStream, b.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}

	b.running = true

	for b.running {
		result, err := b.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    b.consumerGroup,
			Consumer: b.consumerID,
			Streams:  []string{BroadcastStream, ">"},
			Count:    10,
			Block:    b.config.BlockTimeout,
		}).Result()

		if err == redis.Nil {
			continue
		}
		if err != nil {
			if b.running {
				time.Sleep(time.Second)
			}
			continue
		}

		for _, stream := range result {
			for _, msg := range stream.Messages {
				b.handleMessage(ctx, msg)
			}
		}
	}

	return nil
}

// Stop stops the broadcast listener.
func (b *BroadcastListener) Stop() {
	b.running = false
}

func (b *BroadcastListener) handleMessage(ctx context.Context, msg redis.XMessage) {
	taskName, _ := msg.Values["taskName"].(string)
	payloadStr, _ := msg.Values["payload"].(string)
	enqueuedAtStr, _ := msg.Values["enqueuedAt"].(string)

	var enqueuedAt int64
	if enqueuedAtStr != "" {
		enqueuedAt = parseInt64(enqueuedAtStr)
	}

	bm := BroadcastMessage{
		ID:         msg.ID,
		TaskName:   taskName,
		Payload:    json.RawMessage(payloadStr),
		EnqueuedAt: enqueuedAt,
	}

	if b.handler != nil {
		if err := b.handler(ctx, bm); err != nil {
			b.logger.Error("broadcast handler error", "error", err)
			return
		}
	}

	// Acknowledge
	b.redis.XAck(ctx, BroadcastStream, b.consumerGroup, msg.ID)
}

// Cleanup removes ghost consumer groups (consumers that have been idle beyond the threshold).
// This prevents the accumulation of stale consumer groups from workers that have terminated.
// It is typically called periodically by a maintenance task.
func (b *BroadcastListener) Cleanup(ctx context.Context) (int, error) {
	deleted := 0

	groups, err := b.redis.XInfoGroups(ctx, BroadcastStream).Result()
	if err != nil {
		return 0, err
	}

	for _, group := range groups {
		// Never delete our own group
		if group.Name == b.consumerGroup {
			continue
		}

		// Check if all consumers are ghosts
		if b.isGroupIdle(ctx, group.Name) {
			err := b.redis.XGroupDestroy(ctx, BroadcastStream, group.Name).Err()
			if err == nil {
				b.logger.Info("Deleted stale consumer group", "group", group.Name)
				deleted++
			}
		}
	}

	return deleted, nil
}

// isGroupIdle checks if all consumers in a group are idle (ghosts).
func (b *BroadcastListener) isGroupIdle(ctx context.Context, groupName string) bool {
	consumers, err := b.redis.XInfoConsumers(ctx, BroadcastStream, groupName).Result()
	if err != nil {
		return false
	}

	if len(consumers) == 0 {
		// No consumers = definitely stale
		return true
	}

	for _, consumer := range consumers {
		// If any consumer is active (not idle beyond threshold), don't delete
		if consumer.Idle < b.config.ConsumerIdleThreshold {
			return false
		}
	}

	// All consumers are ghosts
	return true
}

func parseInt64(s string) int64 {
	var v int64
	json.Unmarshal([]byte(s), &v)
	return v
}
