package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestTaskTimeout(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("timeout-%d", time.Now().UnixNano())
	client := New(Config{
		Host: "localhost", Port: testPort(), Prefix: prefix,
		ConsumerGroup: "test-timeout-group", WorkerID: "test-worker-1",
	})
	defer client.Close()

	timeoutReached := make(chan bool, 1)

	client.On("timeout.task", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		select {
		case <-ctx.Done():
			timeoutReached <- true
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
			timeoutReached <- false
			return nil, nil
		}
	})

	go client.Start(ctx, ConsumerConfig{
		BlockTimeout: 100 * time.Millisecond, IdleTimeout: time.Minute,
		MaxDeliveries: 5, Prefetch: 2, Concurrency: 2,
		ReclaimerInterval: time.Hour, GracePeriod: time.Second,
	})
	defer client.Stop()

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
	prefix := fmt.Sprintf("backoff-%d", time.Now().UnixNano())
	rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{
		Host: "localhost", Port: testPort(), Prefix: prefix,
		ReclaimInterval: 200 * time.Millisecond, BlockTimeout: 100 * time.Millisecond,
	})
	client := New(Config{
		Provider: rp, Prefix: prefix,
		ConsumerGroup: "test-backoff-group", WorkerID: "test-worker-backoff",
	})
	defer client.Close()

	handlerCalled := make(chan int, 10)
	client.On("backoff.task", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		handlerCalled <- 1
		return nil, fmt.Errorf("temporary failure")
	})

	_, err := client.Enqueue(ctx, "backoff.task", map[string]string{"id": "1"}, EnqueueOptions{
		Backoff: &BackoffConfig{Type: BackoffFixed, Delay: 1000},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	cfg := ConsumerConfig{
		BlockTimeout:      100 * time.Millisecond,
		IdleTimeout:       100 * time.Millisecond,
		MaxDeliveries:     5,
		Prefetch:          1,
		Concurrency:       1,
		ReclaimerInterval: 200 * time.Millisecond,
		GracePeriod:       time.Second,
	}
	go client.Start(ctx, cfg)
	defer client.Stop()

	select {
	case <-handlerCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("First attempt never happened")
	}

	select {
	case <-handlerCalled:
		t.Fatal("Reclaimer reclaimed task too early (ignored backoff)")
	case <-time.After(500 * time.Millisecond):
	}

	select {
	case <-handlerCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("Reclaimer failed to reclaim task after backoff expired")
	}
}

func Pointer[T any](v T) T {
	return v
}
