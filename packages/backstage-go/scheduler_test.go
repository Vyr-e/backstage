package backstage

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestScheduler(t *testing.T) {
	ctx := context.Background()

	// Setup Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test, redis unavailble: %v", err)
	}

	// Cleanup
	defer func() {
		keys, _ := rdb.Keys(ctx, "backstage:*").Result()
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
	}()

	t.Run("EnqueueTasksExplicitly", func(t *testing.T) {
		task := &CronTask{
			Schedule: "* * * * *",
			TaskName: "scheduled.explicit",
			Queue:    NewQueue("custom-queue"),
		}
		
		cfg := SchedulerConfig{
			Host: "localhost",
			Port: 6379,
			Schedules: []*CronTask{task},
		}

		s := NewScheduler(cfg)
		s.enqueueTask(ctx, task)

		// Verify it's in the stream
		msgs, err := rdb.XRange(ctx, "backstage:custom-queue", "-", "+").Result()
		if err != nil {
			t.Fatalf("XRange failed: %v", err)
		}
		
		if len(msgs) != 1 {
			t.Fatalf("Expected 1 message, got %d", len(msgs))
		}
		
		if msgs[0].Values["taskName"] != "scheduled.explicit" {
			t.Errorf("Expected task name scheduled.explicit, got %v", msgs[0].Values["taskName"])
		}
	})

	t.Run("ProcessScheduledTasks", func(t *testing.T) {
		cfg := SchedulerConfig{
			Host: "localhost",
			Port: 6379,
		}
		s := NewScheduler(cfg)

		// Seed a scheduled task in ZSET manually (simulating Producer.Schedule)
		now := time.Now().UnixMilli()
		executeAt := float64(now - 1000) // 1 second ago (due now)
		
		taskData := `{"taskName":"scheduled.due","payload":"{\"data\":1}","enqueuedAt":123456789,"streamKey":"backstage:default"}`
		
		err := rdb.ZAdd(ctx, "backstage:scheduled", redis.Z{
			Score:  executeAt,
			Member: taskData,
		}).Err()
		if err != nil {
			t.Fatalf("ZAdd failed: %v", err)
		}

		// Process
		count, err := s.ProcessScheduledTasks(ctx, "default")
		if err != nil {
			t.Fatalf("ProcessScheduledTasks failed: %v", err)
		}

		if count != 1 {
			t.Errorf("Expected 1 processed task, got %d", count)
		}

		// Verify it moved to stream
		msgs, err := rdb.XRange(ctx, "backstage:default", "-", "+").Result()
		if err != nil {
			t.Fatalf("XRange failed: %v", err)
		}
		
		if len(msgs) != 1 {
			t.Errorf("Expected 1 message in stream, got %d", len(msgs))
		}
		if len(msgs) > 0 && msgs[0].Values["taskName"] != "scheduled.due" {
			t.Errorf("Expected taskName scheduled.due, got %v", msgs[0].Values["taskName"])
		}

		// Verify removed from ZSET
		zcount, _ := rdb.ZCard(ctx, "backstage:scheduled").Result()
		if zcount != 0 {
			t.Errorf("Expected ZSET to be empty, got %d", zcount)
		}
	})
}
