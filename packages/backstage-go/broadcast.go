package backstage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// BroadcastStream is the default Redis stream key used for broadcast messages.
const BroadcastStream = WirePrefix + ":broadcast"

// BroadcastConfig for the broadcast listener.
type BroadcastConfig struct {
	ConsumerIdleThreshold time.Duration
	BlockTimeout          time.Duration
	StartPosition         BroadcastStartPosition
}

// BroadcastStartPosition controls where a newly created broadcast consumer
// group begins reading. Alias kept for back-compat with existing callers.
type BroadcastStartPosition = BroadcastStart

// DefaultBroadcastConfig returns sensible defaults.
func DefaultBroadcastConfig() BroadcastConfig {
	return BroadcastConfig{
		ConsumerIdleThreshold: time.Hour,
		BlockTimeout:          5 * time.Second,
		StartPosition:         BroadcastStartLatest,
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

// BroadcastListener listens for broadcast messages on all workers via Provider.
type BroadcastListener struct {
	provider      Provider
	consumerGroup string
	consumerID    string
	handler       BroadcastHandler
	config        BroadcastConfig
	running       bool
	logger        *Logger
}

// NewBroadcastListener creates a broadcast listener backed by a Redis client.
// Prefer NewBroadcastListenerFromProvider when you already have a Provider.
func NewBroadcastListener(rdb *redis.Client, workerID string, handler BroadcastHandler, config BroadcastConfig) *BroadcastListener {
	rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{Redis: rdb})
	return NewBroadcastListenerFromProvider(rp, workerID, handler, config)
}

// NewBroadcastListenerFromProvider creates a broadcast listener over any Provider.
func NewBroadcastListenerFromProvider(provider Provider, workerID string, handler BroadcastHandler, config BroadcastConfig) *BroadcastListener {
	return &BroadcastListener{
		provider:      provider,
		consumerGroup: wireBroadcastGroup(workerID),
		consumerID:    workerID,
		handler:       handler,
		config:        config,
		logger:        NewLogger("Broadcast"),
	}
}

// Start begins listening for broadcast messages through the Provider.
func (b *BroadcastListener) Start(ctx context.Context) error {
	start := BroadcastStart(b.config.StartPosition)
	if start == "" {
		start = BroadcastStartLatest
	}
	if err := b.provider.EnsureBroadcast(ctx, b.consumerID, start); err != nil {
		return err
	}

	b.running = true
	blockMs := b.config.BlockTimeout.Milliseconds()

	for b.running {
		msgs, err := b.provider.ConsumeBroadcast(ctx, b.consumerID, 10, blockMs)
		if err != nil {
			if b.running {
				time.Sleep(time.Second)
			}
			continue
		}
		if len(msgs) == 0 {
			continue
		}
		var acked []string
		for _, msg := range msgs {
			if b.handleMessage(ctx, msg) {
				acked = append(acked, msg.ID)
			}
		}
		if len(acked) > 0 {
			_ = b.provider.AckBroadcast(ctx, b.consumerID, acked)
		}
	}
	return nil
}

// Stop stops the broadcast listener.
func (b *BroadcastListener) Stop() {
	b.running = false
}

func (b *BroadcastListener) handleMessage(ctx context.Context, msg MessageRef) bool {
	bm := BroadcastMessage{
		ID:         msg.ID,
		TaskName:   msg.TaskName,
		Payload:    msg.Payload,
		EnqueuedAt: msg.EnqueuedAt,
	}
	if b.handler != nil {
		if err := b.handler(ctx, bm); err != nil {
			b.logger.Error("broadcast handler error", "error", err)
			return false
		}
	}
	return true
}

// Cleanup removes ghost broadcast consumer groups via the Provider.
func (b *BroadcastListener) Cleanup(ctx context.Context) (int, error) {
	n, err := b.provider.CleanupBroadcastGhosts(ctx, b.config.ConsumerIdleThreshold.Milliseconds())
	return int(n), err
}
