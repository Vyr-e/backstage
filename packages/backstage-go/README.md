# Backstage Go SDK

Background worker system with pluggable transports (Redis Streams by default; RabbitMQ and Kafka available). At-least-once delivery via a shared `Provider` contract that mirrors the TypeScript SDK.

## Installation

```bash
go get github.com/vyr-e/backstage/packages/backstage-go@v1.0.4
```

## Quick Start (Redis — default)

```go
package main

import (
    "context"
    "encoding/json"

    backstage "github.com/vyr-e/backstage/packages/backstage-go"
)

func main() {
    client := backstage.New(backstage.Config{
        Host:          "localhost",
        Port:          6379,
        ConsumerGroup: "my-app",
    })
    defer client.Close()

    client.On("order.process", func(ctx context.Context, payload json.RawMessage) (*backstage.WorkflowInstruction, error) {
        var order Order
        json.Unmarshal(payload, &order)
        return &backstage.WorkflowInstruction{
            Next: "email.receipt", Delay: 5000, Payload: order,
        }, nil
    })

    client.Start(context.Background(), backstage.DefaultConsumerConfig())
}
```

## Provider architecture

All transport goes through `backstage.Provider`. Core never talks to Redis/Rabbit/Kafka directly for enqueue/consume/ack/reclaim/DLQ/schedule/broadcast.

```go
// Explicit Redis provider
rp := backstage.NewRedisStreamsProvider(backstage.RedisStreamsProviderConfig{
    Host: "localhost", Port: 6379, Prefix: "backstage",
})
client := backstage.NewWithProvider(rp, backstage.Config{
    ConsumerGroup: "my-app", WorkerID: "worker-1",
})

// RabbitMQ
rmq := backstage.NewRabbitMQProvider(backstage.RabbitMQProviderConfig{
    URL: "amqp://guest:guest@localhost:5672/",
})
client = backstage.NewWithProvider(rmq, backstage.Config{Queues: []string{"default"}})

// Kafka
kp := backstage.NewKafkaProvider(backstage.KafkaProviderConfig{
    Brokers: []string{"localhost:9092"},
})
client = backstage.NewWithProvider(kp, backstage.Config{Queues: []string{"default"}})
```

`backstage.New(cfg)` still creates a Redis Streams provider for backwards compatibility.

### Migration from pre-provider Client

| Before | After |
|--------|--------|
| `New(Config{Host, Port, ...})` | Unchanged — still Redis |
| Direct Redis usage via `client` internals | Prefer `client.Provider()` / `client.Redis()` |
| Wire keys (`backstage:{queue}`, DLQ, scheduled ZSET) | Unchanged — TS↔Go interop preserved |

Custom-queue dead-letter keys are `{prefix}:{queue}:dead-letter` (actual queue name, not priority-inferred).

## Monitoring

```go
go client.LogQueues(ctx, 30*time.Second) // Redis provider only
```

## Custom Queues

```go
client := backstage.New(backstage.Config{
    Queues: []string{"matching"}, // replaces default priorities
})
client.RegisterQueue("notifications") // runtime append
```

## Enqueueing Tasks

```go
client.Enqueue(ctx, "order.process", order)
client.Enqueue(ctx, "task", data, backstage.EnqueueOptions{Priority: backstage.PriorityUrgent})
client.Schedule(ctx, "reminder", data, 5*time.Minute)
client.Enqueue(ctx, "task", data, backstage.EnqueueOptions{Queue: "notifications"})
```

## Job Deduplication

```go
id, _ := client.Enqueue(ctx, "order.create", order, backstage.EnqueueOptions{
    Dedupe: &backstage.DedupeConfig{Key: "order-" + order.ID, TTL: time.Minute},
})
```

## Enhanced Job Options

```go
client.Enqueue(ctx, "payment.process", order, backstage.EnqueueOptions{
    Attempts: 3,
    Backoff: &backstage.BackoffConfig{
        Type: backstage.BackoffExponential, Delay: 1000, MaxDelay: 30000,
    },
    Timeout: 10 * time.Second,
})
```

## Features

- Pluggable providers: Redis Streams, RabbitMQ, Kafka
- Multi-priority queues + custom queues
- Job deduplication with TTL
- Attempts / backoff / timeout
- Batched ACKs + optional DeleteOnAck (Redis)
- Workflow chaining, cron scheduling, PEL reclaim
- Broadcast fan-out
- Graceful shutdown, slog logging

## Testing providers

```bash
go test ./...                                    # Redis path (needs Redis on :6379)
RABBITMQ_URL=amqp://guest:guest@localhost:5672/ go test -run Rabbit
KAFKA_BROKERS=localhost:9092 go test -run Kafka
```

## Documentation

- [Producer](docs/producer.md) - Enqueueing tasks
- [Consumer](docs/consumer.md) - Processing tasks
- [Scheduler](docs/scheduler.md) - Cron jobs
- [Logger](docs/logger.md) - slog integration
- [Broadcast](docs/broadcast.md) - Pub/sub messaging
