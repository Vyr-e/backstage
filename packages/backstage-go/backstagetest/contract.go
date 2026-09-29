// Package backstagetest provides the shared provider contract suite.
package backstagetest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/vyr-e/backstage/packages/backstage-go"
)

// CreateProvider builds a fresh provider instance for contract tests.
type CreateProvider func() backstage.Provider

// Options configures optional contract sections.
type Options struct {
	SkipTopics bool
	SkipDelays bool
	Timeout    time.Duration
}

// RunProviderContract runs the shared contract suite.
func RunProviderContract(t *testing.T, create CreateProvider, opts Options) {
	t.Helper()
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	ctx := context.Background()
	provider := create()
	caps := backstage.ResolvedCapabilities{
		Jobs: provider.Jobs(), Topics: provider.Topics(),
		Delays: provider.Delays(), Dedupe: provider.Dedupe(),
	}
	if err := provider.Init(ctx, backstage.ProviderContext{Capabilities: caps, Logger: backstage.NewLogger("contract")}); err != nil {
		t.Fatal(err)
	}
	defer provider.Close()

	if provider.Jobs() == nil {
		t.Fatal("jobs required")
	}

	prefix := fmt.Sprintf("ct-%d", time.Now().UnixNano())
	queue := "q-" + prefix
	_ = provider.Jobs().EnsureQueues(ctx, []string{queue})

	// publish + consume + ack
	id, err := provider.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: queue, TaskName: "contract.ping", Payload: map[string]bool{"ok": true}, EnqueuedAt: time.Now().UnixMilli(),
	})
	if err != nil || id == "" {
		t.Fatalf("publish: %v", err)
	}
	got := make(chan backstage.JobDelivery, 1)
	sub, err := provider.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{queue}, Group: "cg-" + prefix, ConsumerID: "c-" + prefix, Prefetch: 2, IdleTimeout: 500,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		if d.TaskName() == "contract.ping" {
			select {
			case got <- d:
			default:
			}
			return d.Ack(ctx)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	waitChan(t, got, timeout)
	_ = sub.Stop(ctx)

	// crash redelivery
	qCrash := queue + "-crash"
	_ = provider.Jobs().EnsureQueues(ctx, []string{qCrash})
	_, _ = provider.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: qCrash, TaskName: "contract.crash", Payload: map[string]bool{"once": true}, EnqueuedAt: time.Now().UnixMilli(),
	})
	crashSeen := make(chan struct{}, 1)
	sub1, err := provider.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{qCrash}, Group: "cg-crash-" + prefix, ConsumerID: "ca-" + prefix, Prefetch: 1, IdleTimeout: 150,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		select {
		case crashSeen <- struct{}{}:
		default:
		}
		return nil // no ack
	})
	if err != nil {
		t.Fatalf("crash sub1: %v", err)
	}
	waitChan(t, crashSeen, timeout)
	_ = sub1.Stop(ctx)

	redelivered := make(chan struct{}, 1)
	sub2, err := provider.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{qCrash}, Group: "cg-crash-" + prefix, ConsumerID: "cb-" + prefix, Prefetch: 1, IdleTimeout: 100,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		if d.TaskName() == "contract.crash" && d.DeliveryCount() >= 2 {
			select {
			case redelivered <- struct{}{}:
			default:
			}
			return d.Ack(ctx)
		}
		return d.Retry(ctx, backstage.RetryOpts{DelayMs: 50})
	})
	if err != nil {
		t.Fatalf("crash sub2: %v", err)
	}
	waitChan(t, redelivered, timeout)
	_ = sub2.Stop(ctx)

	// prefetch
	q2 := queue + "-pf"
	_ = provider.Jobs().EnsureQueues(ctx, []string{q2})
	for i := 0; i < 5; i++ {
		_, _ = provider.Jobs().Publish(ctx, backstage.OutgoingJob{
			Queue: q2, TaskName: "contract.prefetch", Payload: map[string]int{"i": i}, EnqueuedAt: time.Now().UnixMilli(),
		})
	}
	var peak, inFlight, acked int
	donePf := make(chan struct{})
	subPf, _ := provider.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q2}, Group: "cg-pf-" + prefix, ConsumerID: "cpf-" + prefix, Prefetch: 2, IdleTimeout: 2000,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		time.Sleep(80 * time.Millisecond)
		inFlight--
		acked++
		if acked >= 5 {
			select {
			case <-donePf:
			default:
				close(donePf)
			}
		}
		return d.Ack(ctx)
	})
	waitChan(t, donePf, timeout)
	_ = subPf.Stop(ctx)
	if peak > 2 {
		t.Fatalf("prefetch exceeded: peak=%d", peak)
	}

	// retry delay + deliveryCount
	q3 := queue + "-retry"
	_ = provider.Jobs().EnsureQueues(ctx, []string{q3})
	_, _ = provider.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q3, TaskName: "contract.retry", Payload: map[string]int{}, EnqueuedAt: time.Now().UnixMilli(),
		Meta: backstage.JobMeta{Backoff: &backstage.BackoffConfig{Type: backstage.BackoffFixed, Delay: 400}},
	})
	var counts []int
	var times []time.Time
	doneRetry := make(chan struct{})
	subRetry, _ := provider.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q3}, Group: "cg-retry-" + prefix, ConsumerID: "cr-" + prefix, Prefetch: 1, IdleTimeout: 100,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		counts = append(counts, d.DeliveryCount())
		times = append(times, time.Now())
		if len(counts) < 2 {
			return d.Retry(ctx, backstage.RetryOpts{DelayMs: 400, Error: "boom"})
		}
		select {
		case <-doneRetry:
		default:
			close(doneRetry)
		}
		return d.Ack(ctx)
	})
	waitChan(t, doneRetry, timeout)
	_ = subRetry.Stop(ctx)
	if len(counts) < 2 || counts[0] != 1 || counts[1] < 2 {
		t.Fatalf("counts %v", counts)
	}
	if times[1].Sub(times[0]) < 250*time.Millisecond {
		t.Fatalf("retry too early: %v", times[1].Sub(times[0]))
	}

	// dead-letter
	q4 := queue + "-dlq"
	_ = provider.Jobs().EnsureQueues(ctx, []string{q4})
	_, _ = provider.Jobs().Publish(ctx, backstage.OutgoingJob{
		Queue: q4, TaskName: "contract.dlq", Payload: map[string]int{"x": 1}, EnqueuedAt: time.Now().UnixMilli(),
		Meta: backstage.JobMeta{Attempts: 1},
	})
	dlqDone := make(chan struct{})
	subDlq, _ := provider.Jobs().Consume(ctx, backstage.ConsumeOptions{
		Queues: []string{q4}, Group: "cg-dlq-" + prefix, ConsumerID: "cd-" + prefix, Prefetch: 1, IdleTimeout: 100,
	}, func(ctx context.Context, d backstage.JobDelivery) error {
		if d.DeliveryCount() > 1 {
			_ = d.DeadLetter(ctx, backstage.DeadLetterOpts{Error: "final-fail"})
			close(dlqDone)
			return nil
		}
		return d.Retry(ctx, backstage.RetryOpts{DelayMs: 50, Error: "temp"})
	})
	waitChan(t, dlqDone, timeout)
	_ = subDlq.Stop(ctx)

	// dedupe across two instances
	if provider.Dedupe() != nil {
		other := create()
		_ = other.Init(ctx, backstage.ProviderContext{Capabilities: backstage.ResolvedCapabilities{
			Jobs: other.Jobs(), Topics: other.Topics(), Delays: other.Delays(), Dedupe: other.Dedupe(),
		}, Logger: backstage.NewLogger("contract2")})
		defer other.Close()
		key := "dedupe-" + prefix
		a, _ := provider.Dedupe().Claim(ctx, key, 5000)
		b, _ := other.Dedupe().Claim(ctx, key, 5000)
		if !a || b {
			t.Fatalf("dedupe a=%v b=%v", a, b)
		}
	}

	// delays
	if !opts.SkipDelays && provider.Delays() != nil {
		q5 := queue + "-delay"
		_ = provider.Jobs().EnsureQueues(ctx, []string{q5})
		runAt := time.Now().Add(300 * time.Millisecond).UnixMilli()
		_, _ = provider.Delays().Schedule(ctx, backstage.OutgoingJob{
			Queue: q5, TaskName: "contract.delayed", Payload: map[string]bool{"late": true}, EnqueuedAt: time.Now().UnixMilli(),
		}, runAt)
		// Kick promote if Redis
		if rp, ok := provider.(*backstage.RedisStreamsProvider); ok {
			stop := make(chan struct{})
			go func() {
				tck := time.NewTicker(50 * time.Millisecond)
				defer tck.Stop()
				for {
					select {
					case <-tck.C:
						_, _ = rp.PromoteCrossProvider(ctx)
					case <-stop:
						return
					}
				}
			}()
			defer close(stop)
		}
		delayed := make(chan struct{})
		var early bool
		subD, _ := provider.Jobs().Consume(ctx, backstage.ConsumeOptions{
			Queues: []string{q5}, Group: "cg-delay-" + prefix, ConsumerID: "cdelay-" + prefix, Prefetch: 1, IdleTimeout: 500,
		}, func(ctx context.Context, d backstage.JobDelivery) error {
			if d.TaskName() == "contract.delayed" {
				select {
				case <-delayed:
				default:
					close(delayed)
				}
				return d.Ack(ctx)
			}
			return nil
		})
		time.Sleep(80 * time.Millisecond)
		select {
		case <-delayed:
			early = true
		default:
		}
		if early {
			t.Fatal("delayed job arrived too early")
		}
		waitChan(t, delayed, timeout)
		_ = subD.Stop(ctx)
	}

	// topics
	if !opts.SkipTopics && provider.Topics() != nil {
		topic := "t." + prefix
		var a, b int
		doneFan := make(chan struct{})
		subA, _ := provider.Topics().Subscribe(ctx, backstage.TopicSubscribeOptions{
			Topic: topic, ConsumerID: "fan-a-" + prefix, From: backstage.TopicFromLatest,
		}, func(ctx context.Context, m backstage.TopicDelivery) error {
			a++
			if a >= 1 && b >= 1 {
				select {
				case <-doneFan:
				default:
					close(doneFan)
				}
			}
			return m.Ack(ctx)
		})
		subB, _ := provider.Topics().Subscribe(ctx, backstage.TopicSubscribeOptions{
			Topic: topic, ConsumerID: "fan-b-" + prefix, From: backstage.TopicFromLatest,
		}, func(ctx context.Context, m backstage.TopicDelivery) error {
			b++
			if a >= 1 && b >= 1 {
				select {
				case <-doneFan:
				default:
					close(doneFan)
				}
			}
			return m.Ack(ctx)
		})
		time.Sleep(2 * time.Second)
		_, _ = provider.Topics().Publish(ctx, topic, map[string]int{"n": 1})
		waitChan(t, doneFan, timeout)
		_ = subA.Stop(ctx)
		_ = subB.Stop(ctx)
		if a < 1 || b < 1 {
			t.Fatalf("fanout a=%d b=%d", a, b)
		}

		// group exactly one
		var g1, g2 int
		doneG := make(chan struct{})
		gSub1, _ := provider.Topics().Subscribe(ctx, backstage.TopicSubscribeOptions{
			Topic: topic + ".g", Group: "billing-" + prefix, ConsumerID: "g1-" + prefix, From: backstage.TopicFromLatest,
		}, func(ctx context.Context, m backstage.TopicDelivery) error {
			g1++
			select {
			case <-doneG:
			default:
				close(doneG)
			}
			return m.Ack(ctx)
		})
		gSub2, _ := provider.Topics().Subscribe(ctx, backstage.TopicSubscribeOptions{
			Topic: topic + ".g", Group: "billing-" + prefix, ConsumerID: "g2-" + prefix, From: backstage.TopicFromLatest,
		}, func(ctx context.Context, m backstage.TopicDelivery) error {
			g2++
			select {
			case <-doneG:
			default:
				close(doneG)
			}
			return m.Ack(ctx)
		})
		time.Sleep(2 * time.Second)
		_, _ = provider.Topics().Publish(ctx, topic+".g", map[string]int{"n": 2})
		waitChan(t, doneG, timeout)
		time.Sleep(200 * time.Millisecond)
		_ = gSub1.Stop(ctx)
		_ = gSub2.Stop(ctx)
		if g1+g2 != 1 {
			t.Fatalf("group expected 1 got %d+%d", g1, g2)
		}

		// durability while members down
		durableTopic := topic + ".durable"
		durableGroup := "dur-" + prefix
		warm, _ := provider.Topics().Subscribe(ctx, backstage.TopicSubscribeOptions{
			Topic: durableTopic, Group: durableGroup, ConsumerID: "warm-" + prefix, From: backstage.TopicFromEarliest,
		}, func(ctx context.Context, m backstage.TopicDelivery) error { return m.Ack(ctx) })
		time.Sleep(80 * time.Millisecond)
		_ = warm.Stop(ctx)
		_, _ = provider.Topics().Publish(ctx, durableTopic, map[string]bool{"surviving": true})
		gotDurable := make(chan struct{})
		resume, _ := provider.Topics().Subscribe(ctx, backstage.TopicSubscribeOptions{
			Topic: durableTopic, Group: durableGroup, ConsumerID: "resume-" + prefix, From: backstage.TopicFromEarliest,
		}, func(ctx context.Context, m backstage.TopicDelivery) error {
			var payload map[string]bool
			_ = jsonUnmarshal(m.Payload(), &payload)
			if payload["surviving"] {
				select {
				case <-gotDurable:
				default:
					close(gotDurable)
				}
			}
			return m.Ack(ctx)
		})
		waitChan(t, gotDurable, timeout)
		_ = resume.Stop(ctx)
	}
}

func waitChan[T any](t *testing.T, ch <-chan T, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal("timeout waiting for contract event")
	}
}

func jsonUnmarshal(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}
