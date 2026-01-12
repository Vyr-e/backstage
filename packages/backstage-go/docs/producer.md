# Producer

Enqueue tasks to Redis Streams.

## Basic Usage

```go
import (
    "context"
    backstage "github.com/backstage/go"
)

client := backstage.New(backstage.DefaultConfig())
defer client.Close()

ctx := context.Background()

// Enqueue immediate task
id, err := client.Enqueue(ctx, "email.send", map[string]string{
    "to": "user@example.com",
    "subject": "Welcome",
})

// With priority
id, err := client.Enqueue(ctx, "urgent.alert", data, backstage.EnqueueOptions{
    Priority: backstage.PriorityUrgent,
})

// Schedule for later (60 seconds)
id, err := client.Schedule(ctx, "reminder", data, 60*time.Second)

// Broadcast to all workers
id, err := client.Broadcast(ctx, "cache.invalidate", map[string]string{
    "key": "users",
})
```

## Priority Levels

```go
backstage.PriorityUrgent  // backstage:urgent stream
backstage.PriorityDefault // backstage:default stream
backstage.PriorityLow     // backstage:low stream
```

## Scheduled Tasks

Delayed tasks go to Redis Sorted Set:

```go
// Execute in 5 minutes
client.Schedule(ctx, "cleanup", nil, 5*time.Minute)
```

The consumer polls `backstage:scheduled` and moves due tasks to streams.

## Payload Types

Any JSON-serializable value:

```go
// Map
client.Enqueue(ctx, "task", map[string]interface{}{
    "orderId": "123",
    "amount": 99.99,
})

// Struct
type Order struct {
    ID     string  `json:"id"`
    Amount float64 `json:"amount"`
}
client.Enqueue(ctx, "process.order", Order{ID: "123", Amount: 99.99})

// Nil
client.Enqueue(ctx, "health.check", nil)
```
