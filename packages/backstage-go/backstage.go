// Package backstage provides a robust, Redis-Streams-based background job processing system.
// It supports priority queues, delayed scheduling, deduplication, and broadcast messaging.
package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

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
	// Prefix for Redis keys (default: "backstage")
	Prefix        string
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Host:          "localhost",
		Port:          6379,
		DB:            0,
		ConsumerGroup: "backstage-workers",
		Prefix:        StreamPrefix,
	}
}

// Client provides both producer and consumer functionality.
type Client struct {
	redis         *redis.Client
	config        Config
	handlers      map[string]Handler
	logger        *Logger
	running       bool
	
	// Batched ACK support
	pendingAcks   map[string][]string // streamKey -> messageIDs
	ackChan       chan ackRequest
	ackWg         sync.WaitGroup
	ackMu         sync.Mutex

	// Custom queues
	customQueues  []string
	queuesMu      sync.RWMutex
}

type ackRequest struct {
	stream string
	id     string
}

// Handler is a task handler function.
type Handler func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error)

// New creates a new Backstage client.
func New(cfg Config) *Client {
	if cfg.Prefix == "" {
		cfg.Prefix = StreamPrefix
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	return &Client{
		redis:       rdb,
		config:      cfg,
		handlers:    make(map[string]Handler),
		logger:      NewLogger("Backstage"),
		pendingAcks: make(map[string][]string),
		ackChan:     make(chan ackRequest, 1000), // Buffer for high throughput
	}
}

// RegisterQueue adds a custom queue for the consumer to monitor.
func (c *Client) RegisterQueue(name string) {
	c.queuesMu.Lock()
	defer c.queuesMu.Unlock()
	
	for _, q := range c.customQueues {
		if q == name {
			return
		}
	}
	c.customQueues = append(c.customQueues, name)
}

// LogQueues periodically logs statistics for all registered queues.
func (c *Client) LogQueues(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var queues []*Queue
			
			// Default queues
			queues = append(queues, NewQueue(string(PriorityUrgent), WithPrefix(c.config.Prefix)))
			queues = append(queues, NewQueue(string(PriorityDefault), WithPrefix(c.config.Prefix)))
			queues = append(queues, NewQueue(string(PriorityLow), WithPrefix(c.config.Prefix)))

			// Custom queues
			c.queuesMu.RLock()
			for _, name := range c.customQueues {
				queues = append(queues, NewQueue(name, WithPrefix(c.config.Prefix)))
			}
			c.queuesMu.RUnlock()

			info, err := Inspect(ctx, c.redis, queues)
			if err != nil {
				c.logger.Error("Failed to inspect queues", "error", err)
				continue
			}

			for _, q := range info.Queues {
				c.logger.Info("Queue status", 
					"queue", q.Name, 
					"pending", q.Pending, 
					"scheduled", q.Scheduled, 
					"dead_letter", q.DeadLetter)
			}
			c.logger.Info("Total status", 
				"pending", info.TotalPending, 
				"scheduled", info.TotalScheduled, 
				"dead_letter", info.TotalDL)

		case <-ctx.Done():
			return
		}
	}
}

// Close closes the Redis connection.
func (c *Client) Close() error {
	return c.redis.Close()
}

// streamKey returns the stream key for a priority.
func (c *Client) streamKey(priority Priority) string {
	return fmt.Sprintf("%s:%s", c.config.Prefix, priority)
}

// scheduledKey returns the scheduled tasks sorted set key.
func (c *Client) scheduledKey() string {
	return fmt.Sprintf("%s:scheduled", c.config.Prefix)
}

// deadLetterKey returns the dead-letter stream key.
func (c *Client) deadLetterKey(priority Priority) string {
	return fmt.Sprintf("%s:%s:dead-letter", c.config.Prefix, priority)
}
