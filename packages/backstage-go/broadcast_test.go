package backstage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestBroadcast(t *testing.T) {
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
	
	// Create client for sending broadcast
	client := New(DefaultConfig())
	defer client.Close()

	t.Run("ReceivesBroadcast", func(t *testing.T) {
		messageReceived := make(chan BroadcastMessage, 1)

		handler := func(ctx context.Context, msg BroadcastMessage) error {
			messageReceived <- msg
			return nil
		}

		listener := NewBroadcastListener(rdb, "worker-1", handler, DefaultBroadcastConfig())
		
		// Start listener in background
		go func() {
			if err := listener.Start(ctx); err != nil {
				// Ignore error on stop
			}
		}()
		defer listener.Stop()

		// Allow time for group creation
		time.Sleep(100 * time.Millisecond)

		// Send broadcast
		payload := map[string]string{"foo": "bar"}
		id, err := client.Broadcast(ctx, "test.broadcast", payload)
		if err != nil {
			t.Fatalf("Broadcast failed: %v", err)
		}
		if id == "" {
			t.Fatal("Expected ID returned from Broadcast")
		}

		// Wait for message
		select {
		case msg := <-messageReceived:
			if msg.TaskName != "test.broadcast" {
				t.Errorf("Expected task name 'test.broadcast', got %s", msg.TaskName)
			}
			var p map[string]string
			json.Unmarshal(msg.Payload, &p)
			if p["foo"] != "bar" {
				t.Errorf("Expected payload foo=bar, got %v", p)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for broadcast message")
		}
	})

	t.Run("CleanupsGhostConsumers", func(t *testing.T) {
		// create a ghost group by manually creating it and doing nothing
		ghostGroup := "broadcast-ghost"
		err := rdb.XGroupCreateMkStream(ctx, BroadcastStream, ghostGroup, "0").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			t.Fatalf("Failed to create ghost group: %v", err)
		}

		// Ensure it exists
		groups, err := rdb.XInfoGroups(ctx, BroadcastStream).Result()
		found := false
		for _, g := range groups {
			if g.Name == ghostGroup {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected ghost group to exist")
		}

		// Create listener with very short idle threshold
		config := DefaultBroadcastConfig()
		config.ConsumerIdleThreshold = 100 * time.Millisecond // fast cleanup
		
		listener := NewBroadcastListener(rdb, "worker-cleanup", nil, config)
		
		// Wait for ghost to be considered idle (no consumers implies idle)
		
		// Run cleanup
		deleted, err := listener.Cleanup(ctx)
		if err != nil {
			t.Fatalf("Cleanup failed: %v", err)
		}
		
		if deleted != 1 {
			t.Errorf("Expected 1 deleted group, got %d", deleted)
		}

		// Verify deletion
		groups, _ = rdb.XInfoGroups(ctx, BroadcastStream).Result()
		for _, g := range groups {
			if g.Name == ghostGroup {
				t.Error("Expected ghost group to be deleted")
			}
		}
	})
}
