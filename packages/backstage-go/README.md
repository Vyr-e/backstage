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

        // Process...

        return &backstage.WorkflowInstruction{
            Next:    "email.receipt",
            Delay:   5000,
            Payload: order,
        }, nil
    })

    // Start consuming
    client.Start(context.Background())
}
```

## Features

- Multi-priority queues (urgent, default, low)
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
