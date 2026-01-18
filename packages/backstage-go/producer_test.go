package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestProducer(t *testing.T) {
	ctx := context.Background()

	// Setup Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test, redis unavailble: %v", err)
	}

	client := New(DefaultConfig())
	defer client.Close()

	// Cleanup
	defer func() {
		keys, _ := rdb.Keys(ctx, "backstage:*").Result()
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
	}()

	t.Run("EnqueueBasic", func(t *testing.T) {
		id, err := client.Enqueue(ctx, "test.basic", map[string]string{"foo": "bar"})
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
		if id == "" {
			t.Error("Expected ID to be returned")
		}

		// Verify message in default queue
		msgs, err := rdb.XRange(ctx, "backstage:default", id, id).Result()
		if err != nil {
			t.Fatalf("XRange failed: %v", err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Expected 1 message, got %d", len(msgs))
		}
	})

	t.Run("Deduplication", func(t *testing.T) {
		key := "unique-key-1"
		
		// First enqueue
		id1, err := client.Enqueue(ctx, "test.dedupe", nil, EnqueueOptions{
			Dedupe: &DedupeConfig{
				Key: key,
				TTL: time.Minute,
			},
		})
		if err != nil {
			t.Fatalf("First enqueue failed: %v", err)
		}
		if id1 == "" {
			t.Error("Expected first enqueue to succeed")
		}

		// Second enqueue (duplicate)
		id2, err := client.Enqueue(ctx, "test.dedupe", nil, EnqueueOptions{
			Dedupe: &DedupeConfig{
				Key: key,
				TTL: time.Minute,
			},
		})
		if err != nil {
			t.Fatalf("Second enqueue failed: %v", err)
		}
		if id2 != "" {
			t.Errorf("Expected second enqueue to return empty string (deduplicated), got %s", id2)
		}
	})

	t.Run("CustomQueue", func(t *testing.T) {
		queueName := "analytics"
		id, err := client.Enqueue(ctx, "test.custom", nil, EnqueueOptions{
			Queue: queueName,
		})
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}

		// Verify message in custom queue stream
		msgs, err := rdb.XRange(ctx, "backstage:"+queueName, id, id).Result()
		if err != nil {
			t.Fatalf("XRange failed: %v", err)
		}
		if len(msgs) != 1 {
			t.Errorf("Expected message in %s queue", queueName)
		}
	})

	t.Run("JobMetadata", func(t *testing.T) {
		opts := EnqueueOptions{
			Attempts: 3,
			Timeout:  30 * time.Second,
			Backoff: &BackoffConfig{
				Type:     BackoffExponential,
				Delay:    1000,
				MaxDelay: 60000,
			},
		}

		id, err := client.Enqueue(ctx, "test.metadata", nil, opts)
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}

		msgs, err := rdb.XRange(ctx, "backstage:default", id, id).Result()
		if err != nil || len(msgs) == 0 {
			t.Fatalf("Failed to fetch message: %v", err)
		}

		values := msgs[0].Values
		
		if fmt.Sprintf("%v", values["attempts"]) != "3" {
			t.Errorf("Expected attempts=3, got %v", values["attempts"])
		}
		
		timeoutStr := fmt.Sprintf("%v", values["timeout"])
		if timeoutStr != "30000" {
			t.Errorf("Expected timeout=30000, got %v", timeoutStr)
		}

		backoffStr := fmt.Sprintf("%v", values["backoff"])
		var backoff BackoffConfig
		if err := json.Unmarshal([]byte(backoffStr), &backoff); err != nil {
			t.Fatalf("Failed to parse backoff JSON: %v", err)
		}
		if backoff.Type != BackoffExponential {
			t.Errorf("Expected backoff type exponential, got %v", backoff.Type)
		}
	})

	t.Run("ScheduledTask", func(t *testing.T) {
		rdb.Del(ctx, "backstage:scheduled")
		
		delay := time.Minute
		id, err := client.Schedule(ctx, "test.scheduled", nil, delay)
		if err != nil {
			t.Fatalf("Schedule failed: %v", err)
		}

		// Check ZSET
		count, err := rdb.ZCard(ctx, "backstage:scheduled").Result()
		if err != nil {
			t.Fatalf("ZCard failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 scheduled task, got %d", count)
		}
		_ = id
	})
}
