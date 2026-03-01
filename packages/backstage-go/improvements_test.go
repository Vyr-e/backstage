package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestTaskTimeout(t *testing.T) {
	ctx := context.Background()
	client := New(DefaultConfig())
	defer client.Close()

	// Use a unique group for this test
	client.config.ConsumerGroup = "test-timeout-group"
	client.config.WorkerID = "test-worker-1"
	
	client.redis.Del(ctx, client.streamKey(PriorityDefault))
	client.redis.Del(ctx, "backstage:test-timeout-group:dead-letter")

	// Ensure group exists
	client.initConsumerGroups(ctx)

	timeoutReached := make(chan bool, 1)

	client.On("timeout.task", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		deadline, ok := ctx.Deadline()
		fmt.Printf("Handler started. Has deadline: %v, remaining: %v\n", ok, time.Until(deadline))
		select {
		case <-ctx.Done():
			fmt.Printf("Handler ctx done: %v\n", ctx.Err())
			timeoutReached <- true
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
			fmt.Printf("Handler finished without timeout\n")
			timeoutReached <- false
			return nil, nil
		}
	})

	// Start consumer
	go client.Start(ctx, DefaultConsumerConfig())
	defer client.Stop()

	// Enqueue with 100ms timeout
	client.Enqueue(ctx, "timeout.task", map[string]string{"foo": "bar"}, EnqueueOptions{
		Timeout: 100 * time.Millisecond,
	})

	select {
	case reached := <-timeoutReached:
		if !reached {
			t.Fatal("Task did not timeout as expected")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Test timed out waiting for task")
	}
}

func TestBackoffReclaimer(t *testing.T) {
	ctx := context.Background()
	client := New(DefaultConfig())
	defer client.Close()

	stream := "backstage:default"
	group := "test-backoff-group"
	client.config.ConsumerGroup = group
	client.config.WorkerID = "test-worker-backoff"
	
	client.redis.Del(ctx, stream)
	// Create group at "0" BEFORE enqueueing so it sees the message
	err := client.redis.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil {
		t.Logf("XGroupCreate error: %v", err)
	}

	handlerCalled := make(chan int, 5)

	client.On("backoff.task", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		handlerCalled <- 1
		return nil, fmt.Errorf("temporary failure")
	})

	// 1. Enqueue task with 1s fixed backoff
	client.Enqueue(ctx, "backoff.task", map[string]string{"id": "1"}, EnqueueOptions{
		Backoff: &BackoffConfig{
			Type:  BackoffFixed,
			Delay: 1000, // 1 second
		},
	})

	cfg := DefaultConsumerConfig()
	cfg.IdleTimeout = 100 * time.Millisecond // Reclaim after 100ms idle
	cfg.BlockTimeout = 100 * time.Millisecond
	cfg.ReclaimerInterval = 10 * time.Hour   // Don't run automatically

	// 2. Start worker loop temporarily
	stopLoop := make(chan bool)
	go func() {
		// Run a modified loop that stops after one message
		for {
			select {
			case <-stopLoop:
				return
			default:
				result, _ := client.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    group,
					Consumer: client.config.WorkerID,
					Streams:  []string{stream, ">"},
					Count:    1,
					Block:    100 * time.Millisecond,
				}).Result()

				for _, s := range result {
					for _, msg := range s.Messages {
						client.handleMessage(ctx, s.Stream, msg)
					}
				}
			}
		}
	}()

	// Wait for first attempt
	select {
	case <-handlerCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("First attempt never happened")
	}

	close(stopLoop) // Stop the loop
	time.Sleep(200 * time.Millisecond) // Let it become idle

	// 3. Run reclaimer after 200ms. Backoff is 1s, so it should NOT reclaim.
	client.reclaimIdleMessages(ctx, cfg)
	
	select {
	case <-handlerCalled:
		t.Fatal("Reclaimer reclaimed task too early (ignored backoff)")
	case <-time.After(500 * time.Millisecond):
		// Good, wasn't called
	}

	// 4. Wait for backoff to expire (total > 1s)
	time.Sleep(1 * time.Second)

	// 5. Run reclaimer again. Now it should reclaim.
	client.reclaimIdleMessages(ctx, cfg)

	select {
	case <-handlerCalled:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Reclaimer failed to reclaim task after backoff expired")
	}
}

func Pointer[T any](v T) T {
	return v
}
