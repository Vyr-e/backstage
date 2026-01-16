# Backstage Go SDK

Background worker system using Redis Streams with at-least-once delivery.

## Installation

```bash
go get github.com/backstage/go
```

## Quick Start

```go
package main

import (
    "context"
    "encoding/json"

    backstage "github.com/backstage/go"
)

func main() {
    client := backstage.New(backstage.Config{
        Host:          "localhost",
        Port:          6379,
        ConsumerGroup: "my-app",
    })
    defer client.Close()

    // Register handler
    client.On("order.process", func(ctx context.Context, payload json.RawMessage) (*backstage.WorkflowInstruction, error) {
        var order Order
        json.Unmarshal(payload, &order)

        return &backstage.WorkflowInstruction{
            Next:    "email.receipt",
            Delay:   5000,
            Payload: order,
        }, nil
    })

    client.Start(context.Background())
}
```

## Enqueueing Tasks

```go
// Immediate
client.Enqueue(ctx, "order.process", order)

// With priority
client.Enqueue(ctx, "task", data, backstage.EnqueueOptions{
    Priority: backstage.PriorityUrgent,
})

// Delayed
client.Schedule(ctx, "reminder", data, 5*time.Minute)

// Custom queue
client.Enqueue(ctx, "task", data, backstage.EnqueueOptions{
    Queue: "notifications",
})
```

## Job Deduplication

Prevent duplicate submissions:

```go
// First call succeeds
id1, _ := client.Enqueue(ctx, "order.create", order, backstage.EnqueueOptions{
    Dedupe: &backstage.DedupeConfig{
        Key: fmt.Sprintf("order-%s", order.ID),
        TTL: time.Minute,
    },
})

// Second call within TTL returns empty string (skipped)
id2, _ := client.Enqueue(ctx, "order.create", order, backstage.EnqueueOptions{
    Dedupe: &backstage.DedupeConfig{
        Key: fmt.Sprintf("order-%s", order.ID),
    },
})
// id2 == ""
```

## Enhanced Job Options

```go
client.Enqueue(ctx, "payment.process", order, backstage.EnqueueOptions{
    Attempts: 3,
    Backoff: &backstage.BackoffConfig{
        Type:     backstage.BackoffExponential,
        Delay:    1000,    // Base delay in ms
        MaxDelay: 30000,   // Cap for exponential
    },
    Timeout: 10 * time.Second,
})
```

## Features

- Multi-priority queues (urgent, default, low) + custom queues
- Job deduplication with TTL
- Enhanced job options (attempts, backoff, timeout)
- Workflow chaining
- Cron scheduling
- PEL reclaimer
- Broadcast messaging
- Graceful shutdown
- slog-based logging

## Documentation

- [Producer](docs/producer.md) - Enqueueing tasks
- [Consumer](docs/consumer.md) - Processing tasks
- [Scheduler](docs/scheduler.md) - Cron jobs
- [Logger](docs/logger.md) - slog integration
- [Broadcast](docs/broadcast.md) - Pub/sub messaging
