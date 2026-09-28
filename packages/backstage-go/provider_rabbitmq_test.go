package backstage

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestRabbitMQProviderIntegration(t *testing.T) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		t.Skip("RABBITMQ_URL not set")
	}

	ctx := context.Background()
	p := NewRabbitMQProvider(RabbitMQProviderConfig{URL: url, Prefix: "backstage-test"})
	defer p.Close()

	queue := "rabbit-contract"
	if err := p.EnsureQueues(ctx, []string{queue}); err != nil {
		t.Fatal(err)
	}

	id, err := p.Publish(ctx, "rabbit.task", map[string]string{"ok": "1"}, PublishOptions{Queue: queue})
	if err != nil || id == "" {
		t.Fatalf("publish: %q %v", id, err)
	}

	msgs, err := p.Consume(ctx, ConsumeArgs{
		Queues: []string{queue}, ConsumerGroup: "g", ConsumerID: "c",
		MaxMessages: 5, BlockMs: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected message")
	}
	if err := p.Ack(ctx, msgs); err != nil {
		t.Fatal(err)
	}

	// Delay path
	_, err = p.Publish(ctx, "rabbit.delayed", map[string]int{"n": 1}, PublishOptions{
		Queue: queue, Delay: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	msgs, err = p.Consume(ctx, ConsumeArgs{
		Queues: []string{queue}, ConsumerGroup: "g", ConsumerID: "c",
		MaxMessages: 5, BlockMs: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range msgs {
		if m.TaskName == "rabbit.delayed" {
			found = true
		}
	}
	if !found {
		t.Fatal("delayed message not promoted")
	}
	_ = p.Ack(ctx, msgs)

	// Broadcast
	if err := p.EnsureBroadcast(ctx, "worker-a", BroadcastStartLatest); err != nil {
		t.Fatal(err)
	}
	bid, err := p.Broadcast(ctx, "rabbit.bcast", map[string]bool{"x": true})
	if err != nil || bid == "" {
		t.Fatalf("broadcast: %q %v", bid, err)
	}
	bmsgs, err := p.ConsumeBroadcast(ctx, "worker-a", 5, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(bmsgs) == 0 {
		t.Fatal("expected broadcast message")
	}
	ids := make([]string, len(bmsgs))
	for i, m := range bmsgs {
		ids[i] = m.ID
	}
	_ = p.AckBroadcast(ctx, "worker-a", ids)
}
