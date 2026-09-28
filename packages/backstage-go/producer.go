package backstage

import (
	"context"
	"time"
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
	Key string
	TTL time.Duration // Deduplication window (default: 1 hour)
}

// EnqueueOptions configuration for task enqueueing.
type EnqueueOptions struct {
	Priority Priority
	Queue    string
	Delay    time.Duration
	Dedupe   *DedupeConfig
	Attempts int
	Backoff  *BackoffConfig
	Timeout  time.Duration
}

// Enqueue adds a task to the queue via the Provider.
// Returns the message ID, or empty string if deduplicated.
func (c *Client) Enqueue(ctx context.Context, taskName string, payload interface{}, opts ...EnqueueOptions) (string, error) {
	var opt EnqueueOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	return c.provider.Publish(ctx, taskName, payload, opt)
}

// Schedule adds a task to run after a specified delay.
func (c *Client) Schedule(ctx context.Context, taskName string, payload interface{}, delay time.Duration, opts ...EnqueueOptions) (string, error) {
	var opt EnqueueOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	opt.Delay = delay
	return c.Enqueue(ctx, taskName, payload, opt)
}

// Broadcast sends a task to all workers via the Provider.
func (c *Client) Broadcast(ctx context.Context, taskName string, payload interface{}) (string, error) {
	return c.provider.Broadcast(ctx, taskName, payload)
}
