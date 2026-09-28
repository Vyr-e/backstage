package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/vyr-e/backstage/packages/backstage-go"
	"github.com/vyr-e/backstage/packages/backstage-go/backstagetest"
	"github.com/vyr-e/backstage/packages/backstage-go/providers/kafka"
)

func TestKafkaContract(t *testing.T) {
	p := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Prefix: "ktest"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	})
	if err != nil {
		t.Skipf("Kafka unreachable: %v", err)
	}
	// Probe with EnsureQueues
	if err := p.Jobs().EnsureQueues(ctx, []string{"probe"}); err != nil {
		t.Skipf("Kafka unreachable: %v", err)
	}
	_ = p.Close()

	backstagetest.RunProviderContract(t, func() backstage.Provider {
		return kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Prefix: "k-" + time.Now().Format("150405")})
	}, backstagetest.Options{SkipDelays: true, Timeout: 45 * time.Second})
}
