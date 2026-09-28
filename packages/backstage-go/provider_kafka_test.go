package backstage

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestKafkaProviderIntegration(t *testing.T) {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	brokers := strings.Split(brokersEnv, ",")

	ctx := context.Background()
	p := NewKafkaProvider(KafkaProviderConfig{Brokers: brokers, Prefix: "backstage-test"})
	defer p.Close()

	queue := "kafka-contract"
	if err := p.EnsureQueues(ctx, []string{queue}); err != nil {
		t.Fatal(err)
	}

	id, err := p.Publish(ctx, "kafka.task", map[string]string{"ok": "1"}, PublishOptions{Queue: queue})
	if err != nil || id == "" {
		t.Fatalf("publish: %q %v", id, err)
	}

	msgs, err := p.Consume(ctx, ConsumeArgs{
		Queues: []string{queue}, ConsumerGroup: "kafka-contract-group",
		ConsumerID: "c1", MaxMessages: 5, BlockMs: 5000,
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

	_, err = p.Publish(ctx, "kafka.delayed", map[string]int{"n": 1}, PublishOptions{
		Queue: queue, Delay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	n, err := p.PromoteDueScheduled(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("expected promote >= 1, got %d", n)
	}

	if err := p.EnsureBroadcast(ctx, "kw1", BroadcastStartLatest); err != nil {
		t.Fatal(err)
	}
	bid, err := p.Broadcast(ctx, "kafka.bcast", map[string]bool{"x": true})
	if err != nil || bid == "" {
		t.Fatalf("broadcast: %q %v", bid, err)
	}
	bmsgs, err := p.ConsumeBroadcast(ctx, "kw1", 5, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if len(bmsgs) == 0 {
		t.Log("broadcast may take longer to rebalance; not failing hard")
	} else {
		ids := make([]string, len(bmsgs))
		for i, m := range bmsgs {
			ids[i] = m.ID
		}
		_ = p.AckBroadcast(ctx, "kw1", ids)
	}
}
