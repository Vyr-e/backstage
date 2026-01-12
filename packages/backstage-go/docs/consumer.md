# Consumer

Process tasks from Redis Streams using XREADGROUP.

## Basic Usage

```go
import (
    "context"
    "encoding/json"

    backstage "github.com/backstage/go"
)

client := backstage.New(backstage.Config{
    Host:          "localhost",
    Port:          6379,
    ConsumerGroup: "my-workers",
    WorkerID:      "worker-1",
})
defer client.Close()

// Register handlers
client.On("order.process", processOrder)
client.On("email.send", sendEmail)

// Start consuming
ctx := context.Background()
client.Start(ctx)
```

## Configuration

```go
backstage.Config{
    Host:          "localhost",
    Port:          6379,
    Password:      "",
    DB:            0,
    ConsumerGroup: "backstage-workers",
    WorkerID:      "",  // Auto-generated
}

backstage.ConsumerConfig{
    BlockTimeout:  5 * time.Second,
    IdleTimeout:   30 * time.Second,
    MaxDeliveries: 5,
    GracePeriod:   30 * time.Second,
}
```

## Handler Signature

```go
type Handler func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error)
```

## Workflow Chaining

Return `WorkflowInstruction` to chain tasks:

```go
client.On("step1", func(ctx context.Context, payload json.RawMessage) (*backstage.WorkflowInstruction, error) {
    result := doStep1()

    return &backstage.WorkflowInstruction{
        Next:    "step2",
        Delay:   5000,  // Optional delay (ms)
        Payload: map[string]interface{}{
            "step1Result": result,
        },
    }, nil
})

client.On("step2", func(ctx context.Context, payload json.RawMessage) (*backstage.WorkflowInstruction, error) {
    doStep2()
    return nil, nil  // Terminal step
})
```

## Error Handling

```go
client.On("risky.task", func(ctx context.Context, payload json.RawMessage) (*backstage.WorkflowInstruction, error) {
    err := doRiskyThing()
    if err != nil {
        // Message stays in PEL, reclaimer will retry
        return nil, err
    }
    return nil, nil
})
```

## Graceful Shutdown

```go
ctx, cancel := context.WithCancel(context.Background())

go func() {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
    <-sigChan
    cancel()
}()

client.Start(ctx)  // Blocks until ctx cancelled
```
