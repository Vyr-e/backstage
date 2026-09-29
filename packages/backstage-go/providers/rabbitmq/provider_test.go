package rabbitmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
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
	_ = dockerCtl("stop", "bs-rabbit")
	defer dockerCtl("start", "bs-rabbit")
	time.Sleep(2 * time.Second)

	got := make(chan backstage.JobDelivery, 1)
	// Reconnect will fail while stopped — start rabbit again for consume, then stop during DL
	_ = dockerCtl("start", "bs-rabbit")
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

	_ = dockerCtl("stop", "bs-rabbit")
	time.Sleep(1 * time.Second)
	err = d.DeadLetter(ctx, backstage.DeadLetterOpts{Error: "x"})
	if err == nil {
		t.Fatal("expected deadLetter to fail when broker down")
	}
	_ = sub.Stop(ctx)
	// Restart and ensure message is still available (not acked)
	_ = dockerCtl("start", "bs-rabbit")
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

	_ = dockerCtl("stop", "bs-rabbit")
	time.Sleep(2 * time.Second)
	_ = dockerCtl("start", "bs-rabbit")
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

func TestRabbitTopicRetryFanoutIsolation(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("topic-retry-%d", time.Now().UnixNano())
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: prefix, MaxDeliveries: 5})
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer p.Close()

	topic := "orders.placed"
	stableCh := make(chan int, 8)
	flakyCh := make(chan int, 8)
	var flakyAttempts atomic.Int32

	stable, err := p.Topics().Subscribe(ctx, backstage.TopicSubscribeOptions{
		Topic: topic, ConsumerID: "stable-1", From: backstage.TopicFromEarliest,
	}, func(ctx context.Context, m backstage.TopicDelivery) error {
		stableCh <- m.DeliveryCount()
		return nil // handleTopicDelivery acks on success
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stable.Stop(ctx)

	flaky, err := p.Topics().Subscribe(ctx, backstage.TopicSubscribeOptions{
		Topic: topic, ConsumerID: "flaky-1", From: backstage.TopicFromEarliest,
	}, func(ctx context.Context, m backstage.TopicDelivery) error {
		flakyCh <- m.DeliveryCount()
		if flakyAttempts.Add(1) == 1 {
			return fmt.Errorf("flaky once")
		}
		return nil // handleTopicDelivery acks on success
	})
	if err != nil {
		t.Fatal(err)
	}
	defer flaky.Stop(ctx)

	time.Sleep(500 * time.Millisecond)
	if _, err := p.Topics().Publish(ctx, topic, map[string]string{"id": "m1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var stableCounts, flakyCounts []int
	deadline := time.After(20 * time.Second)
	for len(stableCounts) < 1 || len(flakyCounts) < 2 {
		select {
		case c := <-stableCh:
			stableCounts = append(stableCounts, c)
		case c := <-flakyCh:
			flakyCounts = append(flakyCounts, c)
		case <-deadline:
			t.Fatalf("timeout: stable=%v flaky=%v", stableCounts, flakyCounts)
		}
	}
	// Drain any unexpected extras briefly
	time.Sleep(500 * time.Millisecond)
	for {
		select {
		case c := <-stableCh:
			stableCounts = append(stableCounts, c)
		default:
			goto check
		}
	}
check:
	if len(stableCounts) != 1 || stableCounts[0] != 1 {
		t.Fatalf("stable subscriber should get exactly 1 copy (count=1), got %v", stableCounts)
	}
	if len(flakyCounts) != 2 || flakyCounts[0] != 1 || flakyCounts[1] != 2 {
		t.Fatalf("flaky subscriber should get [1,2], got %v", flakyCounts)
	}
}

// TestRabbitPublishThroughput publishes 5k messages concurrently.
// Bound: must finish within 15s. The old serialized confirm path held a mutex
// across each confirm wait and would typically exceed this under concurrent load.
func TestRabbitPublishThroughput(t *testing.T) {
	const n = 5000
	const maxDuration = 15 * time.Second
	const concurrency = 64

	ctx := context.Background()
	prefix := fmt.Sprintf("thrput-%d", time.Now().UnixNano())
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: prefix})
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer p.Close()

	q := "work"
	if err := p.Jobs().EnsureQueues(ctx, []string{q}); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	sem := make(chan struct{}, concurrency)
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			_, err := p.Jobs().Publish(ctx, backstage.OutgoingJob{
				Queue: q, TaskName: "t", Payload: map[string]int{"i": i},
				EnqueuedAt: time.Now().UnixMilli(),
			})
			if err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	elapsed := time.Since(start)
	for err := range errCh {
		t.Fatalf("publish error: %v", err)
	}
	if elapsed > maxDuration {
		t.Fatalf("%d concurrent publishes took %v, want <= %v (deferred confirms)", n, elapsed, maxDuration)
	}
	t.Logf("%d concurrent publishes in %v", n, elapsed)
}

func TestRabbitEncodePayloadBytes(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("ep-rmq-%d", time.Now().UnixNano())
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: prefix})
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("RabbitMQ required: %v", err)
	}
	defer p.Close()

	q := "ep"
	_ = p.Jobs().EnsureQueues(ctx, []string{q})

	got := make(chan []byte, 2)
	sub, err := p.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q}, Group: "g", ConsumerID: "c", Prefetch: 2, IdleTimeout: 1000,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		got <- append([]byte(nil), d.Payload()...)
		return d.Ack(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop(ctx)

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
	case <-time.After(15 * time.Second):
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
	case <-time.After(15 * time.Second):
		t.Fatal("timeout invalid")
	}
}

func TestRabbitJobMetaInteropRoundTrip(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("rmeta-%d", time.Now().UnixNano())
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: prefix})
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("RabbitMQ required: %v", err)
	}
	defer p.Close()

	q := "meta"
	_ = p.Jobs().EnsureQueues(ctx, []string{q})

	got := make(chan backstage.JobDelivery, 1)
	sub, err := p.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q}, Group: "g", ConsumerID: "c", Prefetch: 2, IdleTimeout: 1000,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		got <- d
		return d.Ack(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop(ctx)

	_, err = p.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q, TaskName: "order.process",
		Payload:    map[string]string{"orderId": "o1"},
		EnqueuedAt: time.Now().UnixMilli(),
		Meta: backstage.JobMeta{
			Attempts: 3,
			Backoff:  &backstage.BackoffConfig{Type: backstage.BackoffFixed, Delay: 500},
			Timeout:  2000,
		},
		DeliveryCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case d := <-got:
		var payload map[string]string
		if err := json.Unmarshal(d.Payload(), &payload); err != nil || payload["orderId"] != "o1" {
			t.Fatalf("payload=%s err=%v", d.Payload(), err)
		}
		if d.DeliveryCount() != 2 {
			t.Fatalf("deliveryCount=%d", d.DeliveryCount())
		}
		m := d.Meta()
		if m.Attempts != 3 || m.Timeout != 2000 {
			t.Fatalf("meta=%+v", m)
		}
		if m.Backoff == nil || m.Backoff.Type != backstage.BackoffFixed || m.Backoff.Delay != 500 {
			t.Fatalf("backoff=%+v", m.Backoff)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for meta job")
	}
}

func TestRabbitJobMetaConsumesTSShapedWire(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("rtswire-%d", time.Now().UnixNano())
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: prefix})
	if err := p.Init(ctx, backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	}); err != nil {
		t.Fatalf("RabbitMQ required: %v", err)
	}
	defer p.Close()

	q := "tswire"
	_ = p.Jobs().EnsureQueues(ctx, []string{q})

	got := make(chan backstage.JobDelivery, 1)
	sub, err := p.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q}, Group: "g", ConsumerID: "c", Prefetch: 2, IdleTimeout: 1000,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		got <- d
		return d.Ack(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop(ctx)

	_, err = p.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q, TaskName: "from.ts",
		Payload:       map[string]any{"n": 1},
		EnqueuedAt:    time.Now().UnixMilli(),
		Meta:          backstage.JobMeta{Attempts: 5, Backoff: &backstage.BackoffConfig{Type: backstage.BackoffExponential, Delay: 100, MaxDelay: 1000}, Timeout: 1500},
		DeliveryCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if d.Meta().Attempts != 5 || d.Meta().Timeout != 1500 || d.DeliveryCount() != 1 {
			t.Fatalf("meta=%+v count=%d", d.Meta(), d.DeliveryCount())
		}
		if d.Meta().Backoff == nil || d.Meta().Backoff.Type != backstage.BackoffExponential {
			t.Fatalf("backoff=%+v", d.Meta().Backoff)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout")
	}
}
