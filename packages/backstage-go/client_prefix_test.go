package backstage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestClientPrefix(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test, redis unavailble: %v", err)
	}

	prefix := "custom-ns"
	cfg := DefaultConfig()
	cfg.Prefix = prefix
	
	client := New(cfg)
	defer client.Close()

	// Cleanup
	defer func() {
		keys, _ := rdb.Keys(ctx, prefix+":*").Result()
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
	}()

	t.Run("EnqueueUsesPrefix", func(t *testing.T) {
		id, err := client.Enqueue(ctx, "test.prefix", nil)
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
		if id == "" {
			t.Fatal("Expected ID")
		}

		// Verify key exists with prefix
		expectedKey := fmt.Sprintf("%s:%s", prefix, PriorityDefault)
		exists, err := rdb.Exists(ctx, expectedKey).Result()
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists == 0 {
			t.Errorf("Expected stream key %s to exist", expectedKey)
		}
	})

	t.Run("ScheduledUsesPrefix", func(t *testing.T) {
		delay := time.Minute
		_, err := client.Schedule(ctx, "test.scheduled", nil, delay)
		if err != nil {
			t.Fatalf("Schedule failed: %v", err)
		}

		expectedKey := fmt.Sprintf("%s:scheduled", prefix)
		count, err := rdb.ZCard(ctx, expectedKey).Result()
		if err != nil {
			t.Fatalf("ZCard failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 scheduled task in %s, got %d", expectedKey, count)
		}
	})

	t.Run("ConsumerUsesPrefix", func(t *testing.T) {
		// Start consumer
		consumerCfg := DefaultConsumerConfig()
		consumerCfg.BlockTimeout = 100 * time.Millisecond
		
		go func() {
			client.Start(ctx, consumerCfg)
		}()
		time.Sleep(100 * time.Millisecond) // wait for group creation
		client.Stop()

		// Verify consumer group stream
		expectedKey := fmt.Sprintf("%s:%s", prefix, PriorityDefault)
		groups, err := rdb.XInfoGroups(ctx, expectedKey).Result()
		if err != nil {
			t.Fatalf("XInfoGroups failed: %v", err)
		}
		found := false
		for _, g := range groups {
			if g.Name == cfg.ConsumerGroup {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected consumer group %s to exist on stream %s", cfg.ConsumerGroup, expectedKey)
		}
	})
}
