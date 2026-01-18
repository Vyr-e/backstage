package backstage

import (
	"context"
	"testing"
)

func TestInteropMessageFormat(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()

	// Enqueue a task with the expected format
	id, err := client.Enqueue(ctx, "interop.go-to-ts", map[string]interface{}{
		"driverId": "driver123",
		"location": map[string]float64{
			"lat": 6.5244,
			"lng": 3.3792,
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Error("expected non-empty ID")
	}

	// Verify the message exists in the stream (can be read by TS)
	result, err := testRedis.XRange(ctx, "backstage:default", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}

	if len(result) == 0 {
		t.Error("expected at least one message")
	}

	// Verify message has required fields
	msg := result[len(result)-1]
	if _, ok := msg.Values["taskName"]; !ok {
		t.Error("missing taskName field")
	}
	if _, ok := msg.Values["payload"]; !ok {
		t.Error("missing payload field")
	}
	if _, ok := msg.Values["enqueuedAt"]; !ok {
		t.Error("missing enqueuedAt field")
	}
}

func TestInteropScheduledFormat(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()

	// Schedule a task
	id, err := client.Schedule(ctx, "interop.scheduled", nil, 60000)
	if err != nil {
		t.Fatal(err)
	}

	if id == "" {
		t.Error("expected non-empty ID")
	}

	// Verify it's in the sorted set
	count, _ := testRedis.ZCard(ctx, "backstage:scheduled").Result()
	if count == 0 {
		t.Error("expected scheduled task in sorted set")
	}
}

func TestInteropBroadcastFormat(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	ctx := context.Background()

	id, err := client.Broadcast(ctx, "interop.broadcast", map[string]bool{"invalidate": true})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Error("expected non-empty ID")
	}

	// Verify it's in the broadcast stream
	len, _ := testRedis.XLen(ctx, "backstage:broadcast").Result()
	if len == 0 {
		t.Error("expected message in broadcast stream")
	}
}

func TestInteropPriorityStreams(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	// Verify stream keys match what TS expects
	expected := map[Priority]string{
		PriorityUrgent:  "backstage:urgent",
		PriorityDefault: "backstage:default",
		PriorityLow:     "backstage:low",
	}

	for priority, expectedKey := range expected {
		key := client.streamKey(priority)
		if key != expectedKey {
			t.Errorf("priority %s: expected '%s', got '%s'", priority, expectedKey, key)
		}
	}
}

func TestInteropDeadLetterKey(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	// Verify dead letter key format matches TS
	key := client.deadLetterKey(PriorityDefault)
	expected := "backstage:default:dead-letter"
	if key != expected {
		t.Errorf("expected '%s', got '%s'", expected, key)
	}
}
