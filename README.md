# Backstage

A production-ready background worker system using Redis Streams with at-least-once delivery guarantees.

## Overview

Backstage provides robust SDKs for building distributed background job processing systems. It leverages Redis Streams for reliable message delivery and supports advanced features like priority queues, workflow chaining, and cron scheduling.

## Packages

| Package                                 | Description        |
| --------------------------------------- | ------------------ |
| [backstage-ts](./packages/backstage-ts) | TypeScript/Bun SDK |
| [backstage-go](./packages/backstage-go) | Go SDK             |

## Key Features

- **Multi-Priority Queues** - Route tasks to urgent, default, or low priority streams
- **Workflow Chaining** - Chain tasks together with delays and payload transformations
- **Cron Scheduling** - Schedule recurring tasks with cron expressions
- **PEL Reclaimer** - Automatic recovery of stuck/abandoned messages
- **Broadcast Messaging** - Send messages to all active workers
- **Graceful Shutdown** - Complete in-flight tasks before shutting down
- **Dead-Letter Queue** - Handle failed tasks after max retries
- **Backpressure Support** - Prevent worker overload with configurable limits

## Getting Started

### TypeScript

```bash
bun add @backstage/core
```

```typescript
import { Worker } from '@backstage/core';

const worker = new Worker({ host: 'localhost', port: 6379 });

worker.on('order.process', async (data) => {
  // Process the order
  return { next: 'email.receipt', payload: data };
});

await worker.start();
```

### Go

```bash
go get github.com/backstage/go
```

```go
client := backstage.New(backstage.Config{
    Host: "localhost",
    Port: 6379,
})

client.On("order.process", func(ctx context.Context, payload json.RawMessage) (*backstage.WorkflowInstruction, error) {
    // Process the order
    return &backstage.WorkflowInstruction{Next: "email.receipt"}, nil
})

client.Start(context.Background())
```

## Development

### Prerequisites

- Redis 7.0+ (AOF persistence enabled)
- Bun 1.3+ (for TypeScript SDK)
- Go 1.21+ (for Go SDK)

### Running Redis

```bash
docker-compose up -d
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute to this project.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
