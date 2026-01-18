// Package backstage utilities.
// Helper functions for inspecting queue state, purging data, and other maintenance tasks.
package backstage

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// QueueInfo contains statistics about a specific queue.
type QueueInfo struct {
	Name       string
	Pending    int64 // Number of messages waiting in the stream
	Scheduled  int64 // Number of tasks scheduled for future execution (in ZSET)
	DeadLetter int64 // Number of messages in the dead letter queue
}

// QueuesInfo aggregates statistics for multiple queues.
type QueuesInfo struct {
	Queues         []QueueInfo
	TotalPending   int64
	TotalScheduled int64
	TotalDL        int64
}

// Inspect retrieves current statistics for the provided list of queues.
func Inspect(ctx context.Context, rdb *redis.Client, queues []*Queue) (*QueuesInfo, error) {
	info := &QueuesInfo{}

	for _, q := range queues {
		pending, _ := rdb.XLen(ctx, q.StreamKey()).Result()
		scheduled, _ := rdb.ZCard(ctx, q.ScheduledKey()).Result()
		dl, _ := rdb.XLen(ctx, q.DeadLetterKey()).Result()

		info.Queues = append(info.Queues, QueueInfo{
			Name:       q.Name,
			Pending:    pending,
			Scheduled:  scheduled,
			DeadLetter: dl,
		})

		info.TotalPending += pending
		info.TotalScheduled += scheduled
		info.TotalDL += dl
	}

	return info, nil
}

func NumPendingTasks(ctx context.Context, rdb *redis.Client, q *Queue) (int64, error) {
	return rdb.XLen(ctx, q.StreamKey()).Result()
}

func NumScheduledTasks(ctx context.Context, rdb *redis.Client, q *Queue) (int64, error) {
	return rdb.ZCard(ctx, q.ScheduledKey()).Result()
}

// PurgeQueue removes all messages from the main stream of a queue.
// WARNING: This deletes unprocessed data.
func PurgeQueue(ctx context.Context, rdb *redis.Client, q *Queue) (int64, error) {
	len, err := rdb.XLen(ctx, q.StreamKey()).Result()
	if err != nil {
		return 0, err
	}
	if len > 0 {
		rdb.Del(ctx, q.StreamKey())
	}
	return len, nil
}

func PurgeScheduled(ctx context.Context, rdb *redis.Client, q *Queue) (int64, error) {
	len, err := rdb.ZCard(ctx, q.ScheduledKey()).Result()
	if err != nil {
		return 0, err
	}
	if len > 0 {
		rdb.Del(ctx, q.ScheduledKey())
	}
	return len, nil
}

func PurgeDeadLetter(ctx context.Context, rdb *redis.Client, q *Queue) (int64, error) {
	len, err := rdb.XLen(ctx, q.DeadLetterKey()).Result()
	if err != nil {
		return 0, err
	}
	if len > 0 {
		rdb.Del(ctx, q.DeadLetterKey())
	}
	return len, nil
}
