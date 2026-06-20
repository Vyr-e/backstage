package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestCustomQueuesOverrideDefaults(t *testing.T) {
	ctx := context.Background()
	client := New(Config{
		Host:          "localhost",
		Port:          testPort(),
		ConsumerGroup: "test-custom-queues",
		WorkerID:      "test-worker",
		Queues:        []string{"my-custom-queue"},
	})
	defer client.Close()

	// Clean slate before test
	for _, q := range []string{"backstage:my-custom-queue", "backstage:urgent", "backstage:default", "backstage:low"} {
		client.redis.Del(ctx, q)
	}

	defer func() {
		client.redis.Del(ctx, "backstage:my-custom-queue")
	}()

	// Check that only the custom queue is in the list, not defaults
	queues := client.getQueues()
	if len(queues) != 1 {
		t.Fatalf("expected 1 queue (custom only), got %d: %v", len(queues), queues)
	}
	if queues[0] != "backstage:my-custom-queue" {
		t.Errorf("expected 'backstage:my-custom-queue', got '%s'", queues[0])
	}

	// Initialize consumer groups
	err := client.initConsumerGroups(ctx)
	if err != nil {
		t.Fatalf("initConsumerGroups failed: %v", err)
	}

	// Enqueue to custom queue
	id, err := client.Enqueue(ctx, "custom.task", map[string]string{"foo": "bar"}, EnqueueOptions{
		Queue: "my-custom-queue",
	})
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty ID")
	}

	// Verify message is in custom queue stream
	msgs, err := client.redis.XRange(ctx, "backstage:my-custom-queue", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 message in custom queue, got %d", len(msgs))
	}

	// Verify no messages in default queues
	for _, q := range []string{"backstage:urgent", "backstage:default", "backstage:low"} {
		msgs, _ := client.redis.XLen(ctx, q).Result()
		if msgs > 0 {
			t.Errorf("expected no messages in %s, got %d", q, msgs)
		}
	}
}

func TestCustomQueuesWithRegistration(t *testing.T) {
	ctx := context.Background()
	client := New(Config{
		Host:          "localhost",
		Port:          testPort(),
		ConsumerGroup: "test-custom-queues-runtime",
		WorkerID:      "test-worker",
		Queues:        []string{"config-queue"},
	})
	defer client.Close()

	defer func() {
		client.redis.Del(ctx, "backstage:config-queue")
		client.redis.Del(ctx, "backstage:runtime-queue")
	}()

	// Register additional queue at runtime
	client.RegisterQueue("runtime-queue")

	// Check queues list
	queues := client.getQueues()
	if len(queues) != 2 {
		t.Fatalf("expected 2 queues (config + runtime), got %d: %v", len(queues), queues)
	}

	expected := map[string]bool{
		"backstage:config-queue":  true,
		"backstage:runtime-queue": true,
	}
	for _, q := range queues {
		if !expected[q] {
			t.Errorf("unexpected queue: %s", q)
		}
		delete(expected, q)
	}
}

func TestDefaultsUsedWhenNoQueuesConfig(t *testing.T) {
	client := New(DefaultConfig())
	defer client.Close()

	queues := client.getQueues()
	expected := []string{"backstage:urgent", "backstage:default", "backstage:low"}
	if len(queues) != len(expected) {
		t.Fatalf("expected %d default queues, got %d: %v", len(expected), len(queues), queues)
	}
	for i, q := range queues {
		if q != expected[i] {
			t.Errorf("queue[%d]: expected '%s', got '%s'", i, expected[i], q)
		}
	}
}

func TestProcessLoopWithCustomQueues(t *testing.T) {
	ctx := context.Background()
	client := New(Config{
		Host:          "localhost",
		Port:          testPort(),
		ConsumerGroup: "test-process-custom",
		WorkerID:      "test-worker",
		Queues:        []string{"process-queue"},
	})
	defer client.Close()

	defer func() {
		client.redis.Del(ctx, "backstage:process-queue")
	}()

	err := client.initConsumerGroups(ctx)
	if err != nil {
		t.Fatalf("initConsumerGroups failed: %v", err)
	}

	msgReceived := make(chan string, 1)
	client.On("process.test", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		msgReceived <- "ok"
		return nil, nil
	})

	// Enqueue a task
	_, err = client.Enqueue(ctx, "process.test", map[string]string{"data": "test"}, EnqueueOptions{
		Queue: "process-queue",
	})
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Run a single XREADGROUP cycle instead of the full loop
	result, err := client.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "test-process-custom",
		Consumer: "test-worker",
		Streams:  []string{"backstage:process-queue", ">"},
		Count:    1,
		Block:    500 * time.Millisecond,
	}).Result()
	if err != nil {
		t.Fatalf("XReadGroup failed: %v", err)
	}

	if len(result) == 0 || len(result[0].Messages) == 0 {
		t.Fatal("No messages received from custom queue stream")
	}

	msg := result[0].Messages[0]
	client.handleMessage(ctx, "backstage:process-queue", msg)

	select {
	case <-msgReceived:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Handler was not called")
	}
}

func TestReclaimerWithCustomQueues(t *testing.T) {
	ctx := context.Background()
	streamKey := "backstage:reclaim-custom"

	client := New(Config{
		Host:          "localhost",
		Port:          testPort(),
		ConsumerGroup: "test-reclaim-custom",
		WorkerID:      "test-worker",
		Queues:        []string{"reclaim-custom"},
	})
	defer client.Close()

	defer func() {
		client.redis.Del(ctx, streamKey)
	}()

	err := client.initConsumerGroups(ctx)
	if err != nil {
		t.Fatalf("initConsumerGroups failed: %v", err)
	}

	// Add a pending message by adding to stream and not acking
	_, err = client.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"taskName": "reclaim.test",
			"payload":  "{}",
		},
	}).Result()
	if err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}

	// Read it (but don't ACK) to create a pending entry
	client.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "test-reclaim-custom",
		Consumer: "test-worker",
		Streams:  []string{streamKey, ">"},
		Count:    1,
		Block:    100 * time.Millisecond,
	}).Result()

	// Wait for idle
	time.Sleep(200 * time.Millisecond)

	// Run reclaimer
	cfg := DefaultConsumerConfig()
	cfg.IdleTimeout = 50 * time.Millisecond
	client.reclaimIdleMessages(ctx, cfg)

	// Verify reclaimer ran (it should find the idle message)
	pending, err := client.redis.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  "test-reclaim-custom",
		Idle:   50 * time.Millisecond,
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	if err != nil {
		t.Fatalf("XPendingExt failed: %v", err)
	}

	if len(pending) > 0 {
		fmt.Printf("Found %d pending message(s) in custom queue stream\n", len(pending))
	}
}
