package rabbitmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/vyr-e/backstage/packages/backstage-go"
	"github.com/vyr-e/backstage/packages/backstage-go/backstagetest"
	"github.com/vyr-e/backstage/packages/backstage-go/providers/rabbitmq"
)

func TestRabbitContract(t *testing.T) {
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: "rpc-probe"})
	err := p.Init(context.Background(), backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	})
	if err != nil {
		t.Fatalf("RabbitMQ required for proof but unreachable: %v", err)
	}
	if p.Delays() == nil {
		t.Fatal("expected builtin delays after delayed-plugin detection")
	}
	_ = p.Close()

	backstagetest.RunProviderContract(t, func() backstage.Provider {
		return rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: "rpc-" + time.Now().Format("150405.000")})
	}, backstagetest.Options{Timeout: 45 * time.Second})
}

func TestRabbitDelaysResolvedAfterInit(t *testing.T) {
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: "cap-" + fmt.Sprint(time.Now().UnixNano())})
	client := backstage.New(backstage.Config{
		Provider: p, WorkerID: "w", ConsumerGroup: "g",
	})
	defer client.Close()
	if err := client.InitError(); err != nil {
		t.Fatalf("init: %v", err)
	}
	report := client.Capabilities()
	if !report.Delays.Available {
		t.Fatalf("delays should be available after rabbit init, got %+v", report.Delays)
	}
	if report.Delays.Name == "" {
		t.Fatal("delays name empty")
	}
}

func TestRabbitDeadLetterPublishFailureDoesNotAck(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("dlfail-%d", time.Now().UnixNano())
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: prefix})
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer p.Close()

	q := "q"
	_ = p.Jobs().EnsureQueues(ctx, []string{q})
	_, err := p.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q, TaskName: "t", Payload: map[string]int{"n": 1}, EnqueuedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Stop rabbit so dead-letter publish cannot confirm
	_ = exec.Command("sudo", "docker", "stop", "bs-rabbit").Run()
	defer exec.Command("sudo", "docker", "start", "bs-rabbit").Run()
	time.Sleep(2 * time.Second)

	got := make(chan backstage.JobDelivery, 1)
	// Reconnect will fail while stopped — start rabbit again for consume, then stop during DL
	_ = exec.Command("sudo", "docker", "start", "bs-rabbit").Run()
	time.Sleep(4 * time.Second)

	sub, err := p.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q}, Group: "g", ConsumerID: "c", Prefetch: 1, IdleTimeout: 1000,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		select {
		case got <- d:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var d backstage.JobDelivery
	select {
	case d = <-got:
	case <-time.After(15 * time.Second):
		t.Fatal("no delivery")
	}

	_ = exec.Command("sudo", "docker", "stop", "bs-rabbit").Run()
	time.Sleep(1 * time.Second)
	err = d.DeadLetter(ctx, backstage.DeadLetterOpts{Error: "x"})
	if err == nil {
		t.Fatal("expected deadLetter to fail when broker down")
	}
	_ = sub.Stop(ctx)
	// Restart and ensure message is still available (not acked)
	_ = exec.Command("sudo", "docker", "start", "bs-rabbit").Run()
	time.Sleep(6 * time.Second)

	got2 := make(chan struct{}, 1)
	p2 := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: prefix})
	_ = p2.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p2.Jobs(), Topics: p2.Topics()},
		Logger:       backstage.NewLogger("t2"),
	})
	defer p2.Close()
	sub2, err := p2.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q}, Group: "g2", ConsumerID: "c2", Prefetch: 1, IdleTimeout: 1000,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		select {
		case got2 <- struct{}{}:
		default:
		}
		return d.Ack(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Stop(ctx)
	select {
	case <-got2:
	case <-time.After(20 * time.Second):
		t.Fatal("job was lost after failed deadLetter publish")
	}
}

func TestRabbitReconnectAfterBrokerKill(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("recon-%d", time.Now().UnixNano())
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: prefix})
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer p.Close()

	q := "work"
	_ = p.Jobs().EnsureQueues(ctx, []string{q})
	processed := make(chan string, 4)
	sub, err := p.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q}, Group: "g", ConsumerID: "c", Prefetch: 2, IdleTimeout: 1000,
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
	case <-time.After(10 * time.Second):
		t.Fatal("first job missing")
	}

	_ = exec.Command("sudo", "docker", "stop", "bs-rabbit").Run()
	time.Sleep(2 * time.Second)
	_ = exec.Command("sudo", "docker", "start", "bs-rabbit").Run()
	time.Sleep(6 * time.Second)

	_, err = p.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q, TaskName: "t", Payload: map[string]string{"id": "after"}, EnqueuedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		// publish may need reconnect
		time.Sleep(2 * time.Second)
		_, err = p.Jobs().Publish(ctx, backstage.OutgoingJob{
			Queue: q, TaskName: "t", Payload: map[string]string{"id": "after"}, EnqueuedAt: time.Now().UnixMilli(),
		})
	}
	if err != nil {
		t.Fatalf("publish after reconnect: %v", err)
	}
	select {
	case id := <-processed:
		if id != "after" {
			t.Fatalf("expected after, got %s", id)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("job after broker restart not processed")
	}
}
