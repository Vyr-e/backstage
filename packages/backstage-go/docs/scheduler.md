# Scheduler

Run tasks on cron schedules.

## Basic Usage

```go
import (
    "context"
    backstage "github.com/backstage/go"
)

// Create cron tasks
cleanup, _ := backstage.NewCronTask("0 0 * * *", "cleanup.daily", nil)
health, _ := backstage.NewCronTask("*/5 * * * *", "health.check", nil)

scheduler := backstage.NewScheduler(backstage.SchedulerConfig{
    Host:      "localhost",
    Port:      6379,
    Schedules: []*backstage.CronTask{cleanup, health},
})

ctx := context.Background()
scheduler.Start(ctx)
```

## Cron Syntax

```
┌───────────── minute (0-59)
│ ┌─────────── hour (0-23)
│ │ ┌───────── day of month (1-31)
│ │ │ ┌─────── month (1-12)
│ │ │ │ ┌───── day of week (0-6, 0=Sunday)
│ │ │ │ │
* * * * *
```

## Examples

| Schedule         | Meaning          |
| ---------------- | ---------------- |
| `* * * * *`      | Every minute     |
| `*/5 * * * *`    | Every 5 minutes  |
| `0 * * * *`      | Every hour       |
| `0 0 * * *`      | Midnight daily   |
| `0 9-17 * * 1-5` | 9am-5pm weekdays |

## CronTask with Queue

```go
urgentQueue := backstage.NewQueue("urgent", backstage.QueueOptions{Priority: 1})

task, _ := backstage.NewCronTask(
    "*/1 * * * *",    // Every minute
    "urgent.check",   // Task name
    urgentQueue,      // Target queue
    "arg1", "arg2",   // Args passed to handler
)
```

## Get Next Run

```go
task, _ := backstage.NewCronTask("0 9 * * *", "daily.report", nil)

nextRun := task.NextRun(time.Now())
fmt.Printf("Next run: %v\n", nextRun)
```

## With Consumer

Scheduler and consumer are separate:

```go
// scheduler/main.go
scheduler := backstage.NewScheduler(config)
scheduler.Start(ctx)

// worker/main.go
client := backstage.New(config)
client.On("cleanup.daily", cleanupHandler)
client.On("health.check", healthHandler)
client.Start(ctx)
```
