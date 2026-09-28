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
	// Queues specifies the exact queues to subscribe to.
	// If set, these replace the default priority queues (urgent, default, low).
	Queues        []string
	// DeleteOnAck removes a message from its stream after it is successfully
	// processed and acknowledged (XDEL follows XACK), keeping stream length
	// bounded instead of growing forever. Safe in the default pattern where a
	// single consumer group drains each work queue; leave false if another
	// consumer group replays the same streams. Does not affect broadcast.
	DeleteOnAck   bool

	// Provider swaps the transport (nil = Redis Streams from Host/Port/...).
	Provider Provider
	// Capabilities plugs in or overrides optional capabilities.
	Capabilities *Capabilities
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
	provider      Provider
	resolved      ResolvedCapabilities
	overrides     *Capabilities
	redisProvider *RedisStreamsProvider

	jobSub           Subscription
	topicSubs        []Subscription
	pendingTopicSubs []pendingTopicSub
	subMu            sync.Mutex
	consumerCfg      ConsumerConfig
	promoteStop      chan struct{}

	// Batched ACK support (legacy helpers for tests)
	pendingAcks map[string][]string
	ackChan     chan ackRequest
	ackWg       sync.WaitGroup
	ackMu       sync.Mutex

	customQueues []string
	queuesMu     sync.RWMutex
}

type pendingTopicSub struct {
	topic   string
	handler TopicHandler
	group   string
	from    TopicStart
}

// TopicHandler handles a topic message.
type TopicHandler func(ctx context.Context, payload json.RawMessage, msg TopicMessage) error

// TopicMessage is metadata delivered with a topic subscription.
type TopicMessage struct {
	ID          string
	Topic       string
	PublishedAt int64
}

// SubscribeOption configures Subscribe.
type SubscribeOption func(*pendingTopicSub)

// WithGroup delivers to exactly one instance per named group.
func WithGroup(group string) SubscribeOption {
	return func(s *pendingTopicSub) { s.group = group }
}

// FromEarliest starts a topic subscription from the beginning of the stream.
func FromEarliest() SubscribeOption {
	return func(s *pendingTopicSub) { s.from = TopicFromEarliest }
}

// FromLatest starts a topic subscription from new messages only (default).
func FromLatest() SubscribeOption {
	return func(s *pendingTopicSub) { s.from = TopicFromLatest }
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
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "backstage-workers"
	}
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	if cfg.Port == 0 {
		cfg.Port = 6379
	}

	c := &Client{
		config:      cfg,
		handlers:    make(map[string]Handler),
		logger:      NewLogger("Backstage"),
		pendingAcks: make(map[string][]string),
		ackChan:     make(chan ackRequest, 1000),
		overrides:   cfg.Capabilities,
	}

	if cfg.Provider != nil {
		c.provider = cfg.Provider
		if rp, ok := cfg.Provider.(*RedisStreamsProvider); ok {
			c.redisProvider = rp
			c.redis = rp.Redis()
		}
	} else {
		rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{
			Host: cfg.Host, Port: cfg.Port, Password: cfg.Password, DB: cfg.DB,
			Prefix: cfg.Prefix, DeleteOnAck: cfg.DeleteOnAck,
		})
		c.provider = rp
		c.redisProvider = rp
		c.redis = rp.Redis()
	}

	resolved, err := ResolveCapabilities(c.provider, c.overrides)
	if err != nil {
		c.resolved = ResolvedCapabilities{}
	} else {
		c.resolved = resolved
	}
	_ = c.provider.Init(context.Background(), ProviderContext{
		Capabilities: c.resolved,
		Logger:       c.logger,
	})
	return c
}

// Redis returns the underlying Redis client when using RedisStreamsProvider.
func (c *Client) Redis() *redis.Client {
	if c.redis == nil {
		panic("client.Redis() is only available when using RedisStreamsProvider")
	}
	return c.redis
}

// Capabilities returns the resolved capability report.
func (c *Client) Capabilities() CapabilityReport {
	return BuildCapabilityReport(c.provider, c.resolved, c.overrides)
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
			
			// Get active queues (respects Config.Queues override)
			for _, streamKey := range c.getQueues() {
				// Strip prefix to get queue name
				name := streamKey[len(c.config.Prefix)+1:]
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
	if c.provider != nil {
		return c.provider.Close()
	}
	if c.redis != nil {
		return c.redis.Close()
	}
	return nil
}

// getQueues returns the list of queue stream keys to subscribe to.
// If custom queues are configured via Config.Queues, those replace the defaults.
// Otherwise, the three default priority queues are used.
// Queues registered at runtime via RegisterQueue are always appended.
func (c *Client) getQueues() []string {
	var queues []string

	if len(c.config.Queues) > 0 {
		for _, q := range c.config.Queues {
			queues = append(queues, fmt.Sprintf("%s:%s", c.config.Prefix, q))
		}
	} else {
		priorities := []Priority{PriorityUrgent, PriorityDefault, PriorityLow}
		for _, p := range priorities {
			queues = append(queues, c.streamKey(p))
		}
	}

	c.queuesMu.RLock()
	for _, q := range c.customQueues {
		queues = append(queues, fmt.Sprintf("%s:%s", c.config.Prefix, q))
	}
	c.queuesMu.RUnlock()

	return queues
}

// streamKey returns the stream key for a priority.
func (c *Client) streamKey(priority Priority) string {
	return StreamKey(c.config.Prefix, string(priority))
}

func (c *Client) scheduledKey() string {
	return ScheduledKey(c.config.Prefix)
}

func (c *Client) deadLetterKey(priority Priority) string {
	return DeadLetterKey(c.config.Prefix, string(priority))
}

func (c *Client) errorKey(id string) string {
	return ErrorKey(c.config.Prefix, id)
}

func (c *Client) queueNames() []string {
	keys := c.getQueues()
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = QueueFromStreamKey(c.config.Prefix, k)
	}
	return names
}
