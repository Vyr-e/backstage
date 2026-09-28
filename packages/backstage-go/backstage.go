// Package backstage provides a robust background job processing system.
// Transport is pluggable via Provider (Redis Streams by default; RabbitMQ and Kafka available).
package backstage

import (
	"context"
	"encoding/json"
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

// StreamPrefix is the default key prefix for all backstage streams.
const StreamPrefix = WirePrefix

// Message represents a task message (legacy shape retained for callers).
type Message struct {
	ID            string          `json:"id,omitempty"`
	TaskName      string          `json:"taskName"`
	Payload       json.RawMessage `json:"payload"`
	EnqueuedAt    int64           `json:"enqueuedAt"`
	DeliveryCount int             `json:"deliveryCount,omitempty"`
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
	Prefix string
	// Queues specifies the exact queues to subscribe to.
	// If set, these replace the default priority queues (urgent, default, low).
	Queues []string
	// DeleteOnAck removes a message from its stream after successful ACK
	// (provider AckAndForget). Safe with a single consumer group per queue.
	DeleteOnAck bool
	// Provider injects a transport. When nil, New creates a RedisStreamsProvider.
	Provider Provider
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Host:          "localhost",
		Port:          6379,
		DB:            0,
		ConsumerGroup: WireDefaultConsumerGroup,
		Prefix:        StreamPrefix,
	}
}

// Client provides both producer and consumer functionality via a Provider.
type Client struct {
	provider Provider
	redis    *redis.Client // only set when provider is RedisStreamsProvider
	config   Config
	handlers map[string]Handler
	logger   *Logger
	running  bool

	pendingAcks []MessageRef
	ackChan     chan MessageRef
	ackMu       sync.Mutex

	customQueues []string
	queuesMu     sync.RWMutex

	lastErrors sync.Map // messageID -> error string
}

// Handler is a task handler function.
type Handler func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error)

// New creates a new Backstage client. Uses Redis Streams by default.
func New(cfg Config) *Client {
	if cfg.Prefix == "" {
		cfg.Prefix = StreamPrefix
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = WireDefaultConsumerGroup
	}

	var provider Provider
	var rdb *redis.Client

	if cfg.Provider != nil {
		provider = cfg.Provider
		if rp, ok := provider.(*RedisStreamsProvider); ok {
			rp.SetConsumerGroup(cfg.ConsumerGroup)
			rdb = rp.Client()
			if cfg.Prefix == "" || cfg.Prefix == StreamPrefix {
				cfg.Prefix = rp.Prefix()
			}
		}
	} else {
		rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{
			Host:          cfg.Host,
			Port:          cfg.Port,
			Password:      cfg.Password,
			DB:            cfg.DB,
			ConsumerGroup: cfg.ConsumerGroup,
			Prefix:        cfg.Prefix,
		})
		provider = rp
		rdb = rp.Client()
	}

	return &Client{
		provider: provider,
		redis:    rdb,
		config:   cfg,
		handlers: make(map[string]Handler),
		logger:   NewLogger("Backstage"),
		ackChan:  make(chan MessageRef, 1000),
	}
}

// NewWithProvider creates a Client bound to an explicit Provider.
func NewWithProvider(provider Provider, cfg Config) *Client {
	cfg.Provider = provider
	return New(cfg)
}

// Provider returns the underlying transport provider.
func (c *Client) Provider() Provider { return c.provider }

// Redis returns the underlying Redis client when using RedisStreamsProvider.
// Returns nil for non-Redis providers.
func (c *Client) Redis() *redis.Client { return c.redis }

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
// Only works with RedisStreamsProvider (uses Inspect).
func (c *Client) LogQueues(ctx context.Context, interval time.Duration) {
	if c.redis == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var queues []*Queue
			for _, name := range c.getQueueNames() {
				queues = append(queues, NewQueue(name, WithPrefix(c.config.Prefix)))
			}
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

// Close closes the provider (and Redis connection when owned).
func (c *Client) Close() error {
	return c.provider.Close()
}

// getQueueNames returns logical queue names the worker subscribes to.
func (c *Client) getQueueNames() []string {
	var names []string
	if len(c.config.Queues) > 0 {
		names = append(names, c.config.Queues...)
	} else {
		names = []string{string(PriorityUrgent), string(PriorityDefault), string(PriorityLow)}
	}
	c.queuesMu.RLock()
	names = append(names, c.customQueues...)
	c.queuesMu.RUnlock()
	return names
}

// getQueues returns stream keys (prefix:queue) for backwards-compatible tests/helpers.
func (c *Client) getQueues() []string {
	names := c.getQueueNames()
	keys := make([]string, len(names))
	for i, n := range names {
		keys[i] = wireStreamKey(c.config.Prefix, n)
	}
	return keys
}

func (c *Client) streamKey(priority Priority) string {
	return wireStreamKey(c.config.Prefix, string(priority))
}

func (c *Client) scheduledKey() string {
	return wireScheduledKey(c.config.Prefix)
}

func (c *Client) deadLetterKey(priority Priority) string {
	return wireDeadLetterKey(c.config.Prefix, string(priority))
}

func (c *Client) errorKey(id string) string {
	return wireErrorKey(c.config.Prefix, id)
}
