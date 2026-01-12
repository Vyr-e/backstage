# Broadcast

Send messages to **all** workers (not just one). Useful for cache invalidation, config reload, etc.

## Send Broadcast

```go
import backstage "github.com/backstage/go"

client := backstage.New(backstage.DefaultConfig())

ctx := context.Background()
id, err := client.Broadcast(ctx, "cache.invalidate", map[string]string{
    "key": "users",
})
```

## Listen for Broadcasts

Each worker receives all broadcast messages (via unique consumer group):

```go
import backstage "github.com/backstage/go"

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

listener := backstage.NewBroadcastListener(rdb, "worker-1",
    func(ctx context.Context, msg backstage.BroadcastMessage) error {
        switch msg.TaskName {
        case "cache.invalidate":
            clearLocalCache()
        case "config.reload":
            reloadConfig()
        }
        return nil
    },
    backstage.DefaultBroadcastConfig(),
)

// Start listening (blocks)
go listener.Start(ctx)
```

## Configuration

```go
config := backstage.BroadcastConfig{
    ConsumerIdleThreshold: time.Hour,    // For ghost cleanup
    BlockTimeout:          5 * time.Second,
}
```

## Ghost Consumer Cleanup

Redis keeps disconnected consumers in memory. Periodically clean them:

```go
// Run occasionally (e.g., every 10 minutes)
deleted, err := listener.Cleanup(ctx)
fmt.Printf("Cleaned up %d stale consumer groups\n", deleted)
```

Only deletes groups where **all** consumers have been idle beyond threshold.

## Stream Key

All broadcasts use: `backstage:broadcast`

Each worker creates unique group: `broadcast-{workerID}`
