package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// EnqueueOptions for task enqueueing.
type EnqueueOptions struct {
	Priority Priority
	Delay    time.Duration
}

// Enqueue adds a task to the queue.
func (c *Client) Enqueue(ctx context.Context, taskName string, payload interface{}, opts ...EnqueueOptions) (string, error) {
	priority := PriorityDefault
	var delay time.Duration

	if len(opts) > 0 {
		if opts[0].Priority != "" {
			priority = opts[0].Priority
		}
		delay = opts[0].Delay
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	enqueuedAt := time.Now().UnixMilli()

	if delay > 0 {
		// Scheduled task
		executeAt := float64(time.Now().Add(delay).UnixMilli())
		data, _ := json.Marshal(map[string]interface{}{
			"taskName":   taskName,
			"payload":    string(payloadBytes),
			"enqueuedAt": enqueuedAt,
			"priority":   priority,
		})

		err := c.redis.ZAdd(ctx, scheduledKey(), redis.Z{
			Score:  executeAt,
			Member: string(data),
		}).Err()
		if err != nil {
			return "", fmt.Errorf("zadd scheduled: %w", err)
		}

		return fmt.Sprintf("scheduled:%d", int64(executeAt)), nil
	}

	// Immediate task
	result, err := c.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey(priority),
		Values: map[string]interface{}{
			"taskName":   taskName,
			"payload":    string(payloadBytes),
			"enqueuedAt": enqueuedAt,
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd: %w", err)
	}

	return result, nil
}

// Schedule adds a task to run after a delay.
func (c *Client) Schedule(ctx context.Context, taskName string, payload interface{}, delay time.Duration) (string, error) {
	return c.Enqueue(ctx, taskName, payload, EnqueueOptions{Delay: delay})
}

// Broadcast sends a task to all workers.
func (c *Client) Broadcast(ctx context.Context, taskName string, payload interface{}) (string, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	result, err := c.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: fmt.Sprintf("%s:broadcast", StreamPrefix),
		Values: map[string]interface{}{
			"taskName":   taskName,
			"payload":    string(payloadBytes),
			"enqueuedAt": time.Now().UnixMilli(),
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd broadcast: %w", err)
	}

	return result, nil
}
