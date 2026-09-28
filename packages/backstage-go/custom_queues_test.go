package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestCustomQueuesOverrideDefaults(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("cq-%d", time.Now().UnixNano())
	client := New(Config{
		Host: "localhost", Port: testPort(), Prefix: prefix,
		ConsumerGroup: "test-custom-queues", WorkerID: "test-worker",
		Queues: []string{"my-custom-queue"},
	})
	defer client.Close()

	queues := client.getQueues()
	if len(queues) != 1 {
		t.Fatalf("expected 1 queue (custom only), got %d: %v", len(queues), queues)
	}
	want := prefix + ":my-custom-queue"
	if queues[0] != want {
		t.Errorf("expected %q, got %q", want, queues[0])
	}

	if err := client.resolved.Jobs.EnsureQueues(ctx, client.queueNames()); err != nil {
		t.Fatalf("EnsureQueues failed: %v", err)
	}

	id, err := client.Enqueue(ctx, "custom.task", map[string]string{"foo": "bar"}, EnqueueOptions{
		Queue: "my-custom-queue",
	})
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty ID")
	}

	msgs, err := client.redis.XRange(ctx, want, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 message in custom queue, got %d", len(msgs))
	}

	for _, q := range []string{prefix + ":urgent", prefix + ":default", prefix + ":low"} {
		n, _ := client.redis.XLen(ctx, q).Result()
		if n > 0 {
			t.Errorf("expected no messages in %s, got %d", q, n)
		}
	}
}

func TestCustomQueuesWithRegistration(t *testing.T) {
	prefix := fmt.Sprintf("cqreg-%d", time.Now().UnixNano())
	client := New(Config{
		Host: "localhost", Port: testPort(), Prefix: prefix,
		ConsumerGroup: "test-custom-queues-runtime", WorkerID: "test-worker",
		Queues: []string{"config-queue"},
	})
	defer client.Close()

	client.RegisterQueue("runtime-queue")

	queues := client.getQueues()
	if len(queues) != 2 {
		t.Fatalf("expected 2 queues (config + runtime), got %d: %v", len(queues), queues)
	}
	expected := map[string]bool{
		prefix + ":config-queue":  true,
		prefix + ":runtime-queue": true,
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
	prefix := fmt.Sprintf("cqproc-%d", time.Now().UnixNano())
	client := New(Config{
		Host: "localhost", Port: testPort(), Prefix: prefix,
		ConsumerGroup: "test-process-custom", WorkerID: "test-worker",
		Queues: []string{"process-queue"},
	})
	defer client.Close()

	msgReceived := make(chan string, 1)
	client.On("process.test", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		msgReceived <- "ok"
		return nil, nil
	})

	_, err := client.Enqueue(ctx, "process.test", map[string]string{"data": "test"}, EnqueueOptions{
		Queue: "process-queue",
	})
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	go client.Start(ctx, ConsumerConfig{
		BlockTimeout: 100 * time.Millisecond, IdleTimeout: time.Minute,
		MaxDeliveries: 5, Prefetch: 2, Concurrency: 2,
		ReclaimerInterval: time.Hour, GracePeriod: time.Second,
	})
	defer client.Stop()

	select {
	case <-msgReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("Handler was not called")
	}
}

func TestReclaimerWithCustomQueues(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("cqrecl-%d", time.Now().UnixNano())
	rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{
		Host: "localhost", Port: testPort(), Prefix: prefix,
		ReclaimInterval: 150 * time.Millisecond, BlockTimeout: 100 * time.Millisecond,
	})
	client := New(Config{
		Provider: rp, Prefix: prefix,
		ConsumerGroup: "test-reclaim-custom", WorkerID: "test-worker",
		Queues: []string{"reclaim-custom"},
	})
	defer client.Close()

	got := make(chan struct{}, 2)
	attempts := 0
	client.On("reclaim.test", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		attempts++
		got <- struct{}{}
		if attempts == 1 {
			return nil, fmt.Errorf("fail once")
		}
		return nil, nil
	})

	_, err := client.Enqueue(ctx, "reclaim.test", map[string]string{}, EnqueueOptions{Queue: "reclaim-custom"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	go client.Start(ctx, ConsumerConfig{
		BlockTimeout: 100 * time.Millisecond, IdleTimeout: 100 * time.Millisecond,
		MaxDeliveries: 5, Prefetch: 1, Concurrency: 1,
		ReclaimerInterval: 150 * time.Millisecond, GracePeriod: time.Second,
	})
	defer client.Stop()

	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("first delivery missing")
	}
	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("reclaim delivery missing")
	}
}
