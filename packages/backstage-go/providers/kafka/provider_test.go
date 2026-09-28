package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/vyr-e/backstage/packages/backstage-go"
	"github.com/vyr-e/backstage/packages/backstage-go/backstagetest"
	"github.com/vyr-e/backstage/packages/backstage-go/providers/kafka"
)

type kafkaWithDelays struct {
	*kafka.Provider
	delays backstage.Delays
}

func (k *kafkaWithDelays) Delays() backstage.Delays { return k.delays }
func (k *kafkaWithDelays) Init(ctx context.Context, pctx backstage.ProviderContext) error {
	pctx.Capabilities.Delays = k.delays
	pctx.Capabilities.Jobs = k.Jobs()
	pctx.Capabilities.Topics = k.Topics()
	return k.Provider.Init(ctx, pctx)
}

func TestKafkaContract(t *testing.T) {
	rp := backstage.NewRedisStreamsProvider(backstage.RedisStreamsProviderConfig{
		Host: "localhost", Port: 6379, Prefix: fmt.Sprintf("kdelay-%d", time.Now().UnixNano()),
	})
	if err := rp.Init(context.Background(), backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: rp.Jobs(), Delays: rp.Delays(), Dedupe: rp.Dedupe(), Topics: rp.Topics()},
		Logger:       backstage.NewLogger("rd"),
	}); err != nil {
		t.Fatalf("redis delays: %v", err)
	}
	defer rp.Close()

	probe := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Prefix: "ktest"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := probe.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: probe.Jobs(), Topics: probe.Topics(), Delays: rp.Delays()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("Kafka required for proof but unreachable: %v", err)
	}
	if err := probe.Jobs().EnsureQueues(ctx, []string{"probe"}); err != nil {
		t.Fatalf("Kafka required for proof but unreachable: %v", err)
	}
	_ = probe.Close()

	backstagetest.RunProviderContract(t, func() backstage.Provider {
		p := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Prefix: "k-" + time.Now().Format("150405.000")})
		return &kafkaWithDelays{Provider: p, delays: rp.Delays()}
	}, backstagetest.Options{Timeout: 60 * time.Second})
}

func TestKafkaReconnectAfterBrokerKill(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("krecon-%d", time.Now().UnixNano())
	rp := backstage.NewRedisStreamsProvider(backstage.RedisStreamsProviderConfig{
		Host: "localhost", Port: 6379, Prefix: prefix + "-d",
	})
	_ = rp.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: rp.Jobs(), Delays: rp.Delays()},
		Logger:       backstage.NewLogger("rd"),
	})
	defer rp.Close()

	p := &kafkaWithDelays{
		Provider: kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Prefix: prefix}),
		delays:   rp.Delays(),
	}
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics(), Delays: rp.Delays()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer p.Close()

	q := "work"
	_ = p.Jobs().EnsureQueues(ctx, []string{q})
	processed := make(chan string, 4)
	sub, err := p.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q}, Group: "g-" + prefix, ConsumerID: "c", Prefetch: 2, IdleTimeout: 1000,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		var payload map[string]string
		_ = json.Unmarshal(d.Payload(), &payload)
		processed <- payload["id"]
		return d.Ack(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop(ctx)

	_, _ = p.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q, TaskName: "t", Payload: map[string]string{"id": "before"}, EnqueuedAt: time.Now().UnixMilli(),
	})
	select {
	case id := <-processed:
		if id != "before" {
			t.Fatalf("got %s", id)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("first job missing")
	}

	_ = exec.Command("sudo", "docker", "stop", "bs-kafka").Run()
	time.Sleep(2 * time.Second)
	_ = exec.Command("sudo", "docker", "start", "bs-kafka").Run()
	time.Sleep(8 * time.Second)

	var pubErr error
	for i := 0; i < 5; i++ {
		_, pubErr = p.Jobs().Publish(ctx, backstage.OutgoingJob{
			Queue: q, TaskName: "t", Payload: map[string]string{"id": "after"}, EnqueuedAt: time.Now().UnixMilli(),
		})
		if pubErr == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if pubErr != nil {
		t.Fatalf("publish after reconnect: %v", pubErr)
	}
	select {
	case id := <-processed:
		if id != "after" {
			t.Fatalf("expected after, got %s", id)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("job after kafka restart not processed")
	}
}
