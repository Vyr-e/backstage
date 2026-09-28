package backstage

import (
	"context"
	"encoding/json"
)

// ProviderCapabilities describes optional features a transport supports.
type ProviderCapabilities struct {
	Durable        bool
	Broadcast      bool
	Scheduling     bool
	Retries        bool
	Deduplication  bool
}

// PublishOptions configures a single publish. Alias of EnqueueOptions so the
// public enqueue API and the provider contract share one shape.
type PublishOptions = EnqueueOptions

// MessageRef is a transport-agnostic reference to an in-flight message.
type MessageRef struct {
	ID            string
	Queue         string
	TaskName      string
	Payload       json.RawMessage
	EnqueuedAt    int64
	DeliveryCount int
	Attempts      int
	Backoff       *BackoffConfig
	Timeout       int64 // milliseconds; 0 means unset
}

// ConsumeArgs selects messages from one or more logical queues.
type ConsumeArgs struct {
	Queues        []string
	ConsumerGroup string
	ConsumerID    string
	MaxMessages   int64
	BlockMs       int64
}

// ReclaimIdleArgs selects idle pending messages for reclaim.
type ReclaimIdleArgs struct {
	Queues        []string
	ConsumerGroup string
	ConsumerID    string
	IdleMs        int64
	MaxCount      int64
}

// DeadLetterMeta accompanies a message moved to the dead-letter queue.
type DeadLetterMeta struct {
	OriginalID    string
	DeliveryCount int
	Error         string
}

// BroadcastStart controls where a new broadcast consumer begins reading.
type BroadcastStart string

const (
	BroadcastStartLatest    BroadcastStart = "latest"
	BroadcastStartBeginning BroadcastStart = "beginning"
)

// Provider is the transport-agnostic contract. Core never reaches around it
// for enqueue/consume/ack/reclaim/DLQ/schedule/broadcast.
type Provider interface {
	Name() string
	Capabilities() ProviderCapabilities

	EnsureQueues(ctx context.Context, queues []string) error
	Close() error

	Publish(ctx context.Context, taskName string, payload interface{}, opts PublishOptions) (string, error)

	Consume(ctx context.Context, args ConsumeArgs) ([]MessageRef, error)
	Ack(ctx context.Context, messages []MessageRef) error
	// AckAndForget ACKs and removes from the store when supported (e.g. Redis XDEL).
	// Providers that do not support it should treat it as Ack.
	AckAndForget(ctx context.Context, messages []MessageRef) error

	ReclaimIdle(ctx context.Context, args ReclaimIdleArgs) ([]MessageRef, error)
	DeadLetter(ctx context.Context, message MessageRef, meta DeadLetterMeta) error

	PromoteDueScheduled(ctx context.Context, nowMs int64) (int64, error)

	EnsureBroadcast(ctx context.Context, consumerIdentity string, start BroadcastStart) error
	Broadcast(ctx context.Context, taskName string, payload interface{}) (string, error)
	ConsumeBroadcast(ctx context.Context, consumerIdentity string, maxMessages int64, blockMs int64) ([]MessageRef, error)
	AckBroadcast(ctx context.Context, consumerIdentity string, ids []string) error
	CleanupBroadcastGhosts(ctx context.Context, idleMs int64) (int64, error)
}
