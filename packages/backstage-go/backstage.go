package backstage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Priority levels for task queues.
type Priority string

const (
	PriorityUrgent  Priority = "urgent"
	PriorityDefault Priority = "default"
	PriorityLow     Priority = "low"
)

// StreamPrefix is the key prefix for all backstage streams.
const StreamPrefix = "backstage"

// Message represents a task message.
type Message struct {
	ID           string          `json:"id,omitempty"`
	TaskName     string          `json:"taskName"`
	Payload      json.RawMessage `json:"payload"`
	EnqueuedAt   int64           `json:"enqueuedAt"`
	DeliveryCount int            `json:"deliveryCount,omitempty"`
}

// WorkflowInstruction for chaining tasks.
type WorkflowInstruction struct {
	Next    string      `json:"next"`
	Delay   int64       `json:"delay,omitempty"` // milliseconds
	Payload interface{} `json:"payload,omitempty"`
}

// Config for the Backstage client.
type Config struct {
	Host          string
	Port          int
	Password      string
	DB            int
	ConsumerGroup string
	WorkerID      string
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Host:          "localhost",
		Port:          6379,
		DB:            0,
		ConsumerGroup: "backstage-workers",
	}
}

// Client provides both producer and consumer functionality.
type Client struct {
	redis         *redis.Client
	config        Config
	handlers      map[string]Handler
	running       bool
}

// Handler is a task handler function.
type Handler func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error)

// New creates a new Backstage client.
func New(cfg Config) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	return &Client{
		redis:    rdb,
		config:   cfg,
		handlers: make(map[string]Handler),
	}
}

// Close closes the Redis connection.
func (c *Client) Close() error {
	return c.redis.Close()
}

// streamKey returns the stream key for a priority.
func streamKey(priority Priority) string {
	return fmt.Sprintf("%s:%s", StreamPrefix, priority)
}

// scheduledKey returns the scheduled tasks sorted set key.
func scheduledKey() string {
	return fmt.Sprintf("%s:scheduled", StreamPrefix)
}

// deadLetterKey returns the dead-letter stream key.
func deadLetterKey(priority Priority) string {
	return fmt.Sprintf("%s:%s:dead-letter", StreamPrefix, priority)
}
