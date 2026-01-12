package backstage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStreamSequentialEnqueue(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()
	start := time.Now()

	for i := 0; i < 10000; i++ {
		_, err := client.Enqueue(ctx, fmt.Sprintf("seq.%d", i), map[string]int{"i": i})
		if err != nil {
			t.Fatalf("enqueue failed at %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	throughput := float64(10000) / elapsed.Seconds()

	t.Logf("Sequential: 10,000 in %v (%.0f ops/sec)", elapsed, throughput)
	if throughput < 500 {
		t.Errorf("throughput too low: %.0f ops/sec", throughput)
	}
}

func TestStreamConcurrentEnqueue(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	start := time.Now()
	errors := make(chan error, 10000)

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := client.Enqueue(ctx, fmt.Sprintf("concurrent.%d", idx), map[string]int{"i": idx})
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	elapsed := time.Since(start)
	throughput := float64(10000) / elapsed.Seconds()

	var errCount int
	for range errors {
		errCount++
	}

	t.Logf("Concurrent: 10,000 in %v (%.0f ops/sec), %d errors", elapsed, throughput, errCount)
}

func TestStreamLargePayload(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()

	// 1MB payload
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = 'x'
	}

	_, err := client.Enqueue(ctx, "large.payload", map[string]string{"data": string(largeData)})
	if err != nil {
		t.Fatalf("large payload failed: %v", err)
	}
}

func TestStreamEmptyPayload(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()
	id, err := client.Enqueue(ctx, "empty.payload", nil)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Error("expected non-empty ID")
	}
}

func TestStreamPriorities(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()

	// Enqueue to each priority
	urgentID, _ := client.Enqueue(ctx, "urgent.task", nil, EnqueueOptions{Priority: PriorityUrgent})
	defaultID, _ := client.Enqueue(ctx, "default.task", nil)
	lowID, _ := client.Enqueue(ctx, "low.task", nil, EnqueueOptions{Priority: PriorityLow})

	if urgentID == "" || defaultID == "" || lowID == "" {
		t.Error("one or more IDs empty")
	}
}

func TestStreamScheduledTasks(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			client.Schedule(ctx, fmt.Sprintf("scheduled.%d", idx), nil, time.Minute)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("Scheduled: 1,000 in %v", elapsed)
}

func TestStreamBroadcast(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()
	id, err := client.Broadcast(ctx, "broadcast.test", map[string]bool{"invalidate": true})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Error("expected non-empty ID")
	}
}
