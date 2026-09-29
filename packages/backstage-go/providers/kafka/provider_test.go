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
	sharedPrefix := fmt.Sprintf("kct-%d", time.Now().UnixNano())
	rp := backstage.NewRedisStreamsProvider(backstage.RedisStreamsProviderConfig{
		Host: "localhost", Port: 6379, Prefix: sharedPrefix + "-delays",
	})

	anchor := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Prefix: sharedPrefix, Partitions: 1, ReplicationFactor: 1})
	ctx := context.Background()
	if err := anchor.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: anchor.Jobs(), Topics: anchor.Topics()},
		Logger:       backstage.NewLogger("anchor"),
	}); err != nil {
		t.Fatalf("Kafka required for proof but unreachable: %v", err)
	}
	defer anchor.Close()
	if err := anchor.Jobs().EnsureQueues(ctx, []string{"probe"}); err != nil {
		t.Fatalf("Kafka required for proof but unreachable: %v", err)
	}

	// Redis delays must promote through Kafka jobs (same prefix as contract providers).
	if err := rp.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{
			Jobs: anchor.Jobs(), Delays: rp.Delays(), Dedupe: rp.Dedupe(), Topics: rp.Topics(),
		},
		Logger: backstage.NewLogger("rd"),
	}); err != nil {
		t.Fatalf("redis delays: %v", err)
	}
	defer rp.Close()

	stopPromo := make(chan struct{})
	go func() {
		tck := time.NewTicker(50 * time.Millisecond)
		defer tck.Stop()
		for {
			select {
			case <-tck.C:
				_, _ = rp.PromoteCrossProvider(context.Background())
			case <-stopPromo:
				return
			}
		}
	}()
	defer close(stopPromo)

	backstagetest.RunProviderContract(t, func() backstage.Provider {
		p := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Prefix: sharedPrefix, Partitions: 1, ReplicationFactor: 1})
		return &kafkaWithDelays{Provider: p, delays: rp.Delays()}
	}, backstagetest.Options{Timeout: 60 * time.Second})
}

func TestKafkaReconnectAfterBrokerKill(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("krecon-%d", time.Now().UnixNano())
	rp := backstage.NewRedisStreamsProvider(backstage.RedisStreamsProviderConfig{
		Host: "localhost", Port: 6379, Prefix: prefix + "-d",
	})
	p := &kafkaWithDelays{
		Provider: kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Prefix: prefix, Partitions: 1, ReplicationFactor: 1}),
		delays:   rp.Delays(),
	}
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics(), Delays: rp.Delays()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer p.Close()
	_ = rp.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Delays: rp.Delays()},
		Logger:       backstage.NewLogger("rd"),
	})
	defer rp.Close()

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
	time.Sleep(3 * time.Second)

	_, err = p.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q, TaskName: "t", Payload: map[string]string{"id": "before"}, EnqueuedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("publish before: %v", err)
	}
	select {
	case id := <-processed:
		if id != "before" {
			t.Fatalf("got %s", id)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("first job missing")
	}

	_ = exec.Command("sudo", "docker", "stop", "bs-kafka").Run()
	time.Sleep(2 * time.Second)
	_ = exec.Command("sudo", "docker", "start", "bs-kafka").Run()
	time.Sleep(12 * time.Second)

	var pubErr error
	for i := 0; i < 8; i++ {
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
	case <-time.After(60 * time.Second):
		t.Fatal("job after kafka restart not processed")
	}
}

func TestKafkaEncodePayloadBytes(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("ep-kfk-%d", time.Now().UnixNano())
	p := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}, Prefix: prefix, Partitions: 1, ReplicationFactor: 1})
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("Kafka required: %v", err)
	}
	defer p.Close()

	q := "ep"
	if err := p.Jobs().EnsureQueues(ctx, []string{q}); err != nil {
		t.Fatal(err)
	}

	got := make(chan []byte, 2)
	sub, err := p.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q}, Group: "g-" + prefix, ConsumerID: "c", Prefetch: 2, IdleTimeout: 1000,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		got <- append([]byte(nil), d.Payload()...)
		return d.Ack(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop(ctx)
	time.Sleep(2 * time.Second)

	sonicBytes := []byte(`{"hello":"world","n":1}`)
	_, err = p.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q, TaskName: "t", Payload: sonicBytes, EnqueuedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-got:
		if string(payload) != string(sonicBytes) {
			t.Fatalf("valid JSON []byte: want object %s, got %q", sonicBytes, payload)
		}
		var obj map[string]interface{}
		if err := json.Unmarshal(payload, &obj); err != nil || obj["hello"] != "world" {
			t.Fatalf("expected JSON object, got %s err=%v", payload, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout valid")
	}

	invalid := []byte("not-json")
	wantInvalid, _ := json.Marshal(invalid)
	_, err = p.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q, TaskName: "t", Payload: invalid, EnqueuedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-got:
		if string(payload) != string(wantInvalid) {
			t.Fatalf("invalid []byte: want base64 %s, got %q", wantInvalid, payload)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout invalid")
	}
}
