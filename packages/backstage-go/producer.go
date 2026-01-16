package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// BackoffType defines the retry backoff strategy.
type BackoffType string

const (
	BackoffFixed       BackoffType = "fixed"
	BackoffExponential BackoffType = "exponential"
)

// BackoffConfig defines retry backoff behavior.
type BackoffConfig struct {
	Type     BackoffType `json:"type"`
	Delay    int64       `json:"delay"`    // Base delay in milliseconds
	MaxDelay int64       `json:"maxDelay"` // Max delay cap for exponential backoff
}

// DedupeConfig defines deduplication settings.
type DedupeConfig struct {
	Key string        // Unique key for this job instance
	TTL time.Duration // Deduplication window (default: 1 hour)
}

// EnqueueOptions for task enqueueing.
type EnqueueOptions struct {
	Priority Priority
	Queue    string        // Custom queue name (overrides Priority)
	Delay    time.Duration
	Dedupe   *DedupeConfig // Deduplication settings
	Attempts int           // Max retry attempts for this job
	Backoff  *BackoffConfig
	Timeout  time.Duration // Job processing timeout
}

// Enqueue adds a task to the queue.
// Returns empty string if deduplicated.
func (c *Client) Enqueue(ctx context.Context, taskName string, payload interface{}, opts ...EnqueueOptions) (string, error) {
	var opt EnqueueOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	// Handle deduplication
	if opt.Dedupe != nil {
		dedupeKey := fmt.Sprintf("%s:dedupe:%s", StreamPrefix, opt.Dedupe.Key)
		ttl := opt.Dedupe.TTL
		if ttl == 0 {
			ttl = time.Hour // Default 1 hour
		}
		set, err := c.redis.SetNX(ctx, dedupeKey, "1", ttl).Result()
		if err != nil {
			return "", fmt.Errorf("dedupe setnx: %w", err)
		}
		if !set {
			return "", nil // Duplicate, skip
		}
	}

	// Determine stream key
	var streamKey string
	if opt.Queue != "" {
		streamKey = fmt.Sprintf("%s:%s", StreamPrefix, opt.Queue)
	} else {
		priority := opt.Priority
		if priority == "" {
			priority = PriorityDefault
		}
		streamKey = fmt.Sprintf("%s:%s", StreamPrefix, priority)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	enqueuedAt := time.Now().UnixMilli()

	// Build values map with optional job metadata
	values := map[string]interface{}{
		"taskName":   taskName,
		"payload":    string(payloadBytes),
		"enqueuedAt": enqueuedAt,
	}
	if opt.Attempts > 0 {
		values["attempts"] = opt.Attempts
	}
	if opt.Backoff != nil {
		backoffJSON, _ := json.Marshal(opt.Backoff)
		values["backoff"] = string(backoffJSON)
	}
	if opt.Timeout > 0 {
		values["timeout"] = opt.Timeout.Milliseconds()
	}

	if opt.Delay > 0 {
		// Scheduled task
		executeAt := float64(time.Now().Add(opt.Delay).UnixMilli())
		scheduledData := map[string]interface{}{
			"taskName":   taskName,
			"payload":    string(payloadBytes),
			"enqueuedAt": enqueuedAt,
			"streamKey":  streamKey,
		}
		if opt.Priority != "" {
			scheduledData["priority"] = opt.Priority
		}
		if opt.Attempts > 0 {
			scheduledData["attempts"] = opt.Attempts
		}
		if opt.Backoff != nil {
			backoffJSON, _ := json.Marshal(opt.Backoff)
			scheduledData["backoff"] = string(backoffJSON)
		}
		if opt.Timeout > 0 {
			scheduledData["timeout"] = opt.Timeout.Milliseconds()
		}

		data, _ := json.Marshal(scheduledData)
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
		Stream: streamKey,
		Values: values,
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd: %w", err)
	}

	return result, nil
}

// Schedule adds a task to run after a delay.
func (c *Client) Schedule(ctx context.Context, taskName string, payload interface{}, delay time.Duration, opts ...EnqueueOptions) (string, error) {
	var opt EnqueueOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	opt.Delay = delay
	return c.Enqueue(ctx, taskName, payload, opt)
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

