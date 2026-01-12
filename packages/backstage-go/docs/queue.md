# Queue

Queue abstraction for managing stream keys and priorities.

## Basic Usage

```go
import backstage "github.com/backstage/go"

// Create queue
queue := backstage.NewQueue("notifications")

// With options
queue := backstage.NewQueue("payments", backstage.QueueOptions{
    Priority:    1,
    SoftTimeout: 5000,
    HardTimeout: 30000,
    MaxRetries:  3,
})
```

## Default Queues

```go
backstage.QueueUrgent   // Priority 1
backstage.QueueDefault  // Priority 2
backstage.QueueLow      // Priority 3
```

## Queue Keys

```go
queue := backstage.NewQueue("notifications")

queue.StreamKey()      // "backstage:notifications"
queue.ScheduledKey()   // "backstage:scheduled:notifications"
queue.DeadLetterKey()  // "backstage:notifications:dead-letter"
```

## With CronTask

```go
urgentQueue := backstage.NewQueue("urgent", backstage.QueueOptions{Priority: 1})

task, _ := backstage.NewCronTask("*/5 * * * *", "alert.check", urgentQueue)
```

## Options Reference

| Option        | Default | Description             |
| ------------- | ------- | ----------------------- |
| `Priority`    | 2       | Lower = higher priority |
| `SoftTimeout` | 30000   | Soft timeout (ms)       |
| `HardTimeout` | 120000  | Hard timeout (ms)       |
| `MaxRetries`  | 3       | Before dead-letter      |
