package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestWireGoldenEnqueue(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("gw-%d", time.Now().UnixNano())
	client := New(Config{Host: "localhost", Port: testPort(), Prefix: prefix, WorkerID: "gw", ConsumerGroup: "gw-g"})
	defer client.Close()
	defer func() {
		keys, _ := client.redis.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			client.redis.Del(ctx, keys...)
		}
	}()

	id, err := client.Enqueue(ctx, "order.process", map[string]string{"orderId": "o1"})
	if err != nil || id == "" {
		t.Fatalf("enqueue: %v id=%s", err, id)
	}
	msgs, err := client.redis.XRange(ctx, StreamKey(prefix, "default"), id, id).Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("xrange: %v len=%d", err, len(msgs))
	}
	m := msgs[0].Values
	if m["taskName"] != "order.process" {
		t.Fatalf("taskName %v", m["taskName"])
	}
	if m["payload"] != `{"orderId":"o1"}` {
		t.Fatalf("payload %v", m["payload"])
	}
	if m["enqueuedAt"] == nil {
		t.Fatal("missing enqueuedAt")
	}
}

func TestWireGoldenDelayDedupe(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("gwd-%d", time.Now().UnixNano())
	client := New(Config{Host: "localhost", Port: testPort(), Prefix: prefix, WorkerID: "gw", ConsumerGroup: "gw-g"})
	defer client.Close()
	defer func() {
		keys, _ := client.redis.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			client.redis.Del(ctx, keys...)
		}
	}()

	before := time.Now().UnixMilli()
	id, err := client.Enqueue(ctx, "later", map[string]int{"x": 1}, EnqueueOptions{Delay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if id[:10] != "scheduled:" {
		t.Fatalf("id %s", id)
	}
	members, err := client.redis.ZRangeWithScores(ctx, ScheduledKey(prefix), 0, -1).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("zrange: %v", err)
	}
	var member map[string]interface{}
	json.Unmarshal([]byte(members[0].Member.(string)), &member)
	if member["taskName"] != "later" {
		t.Fatalf("%v", member)
	}
	if member["streamKey"] != StreamKey(prefix, "default") {
		t.Fatalf("streamKey %v", member["streamKey"])
	}
	if int64(members[0].Score) < before+59_000 {
		t.Fatalf("score too low")
	}

	id1, _ := client.Enqueue(ctx, "t", nil, EnqueueOptions{Dedupe: &DedupeConfig{Key: "k1", TTL: 5 * time.Second}})
	id2, _ := client.Enqueue(ctx, "t", nil, EnqueueOptions{Dedupe: &DedupeConfig{Key: "k1", TTL: 5 * time.Second}})
	if id1 == "" || id2 != "" {
		t.Fatalf("dedupe id1=%s id2=%s", id1, id2)
	}
	val, _ := client.redis.Get(ctx, DedupeKey(prefix, "k1")).Result()
	if val != "1" {
		t.Fatalf("dedupe val %s", val)
	}
}

func TestWireGoldenDeadLetterCustomQueue(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("gwdl-%d", time.Now().UnixNano())
	rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{
		Host: "localhost", Port: testPort(), Prefix: prefix, BlockTimeout: 100 * time.Millisecond, ReclaimInterval: time.Minute,
	})
	defer rp.Close()
	_ = rp.Init(ctx, ProviderContext{Capabilities: ResolvedCapabilities{Jobs: rp.Jobs(), Delays: rp.Delays(), Dedupe: rp.Dedupe(), Topics: rp.Topics()}, Logger: NewLogger("t")})

	queue := "notifications"
	id, err := rp.Jobs().Publish(ctx, OutgoingJob{Queue: queue, TaskName: "dlq.me", Payload: map[string]bool{"x": true}, EnqueuedAt: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	sub, err := rp.Jobs().Consume(ctx, ConsumeOptions{
		Queues: []string{queue}, Group: prefix + "-g", ConsumerID: prefix + "-c", Prefetch: 1, IdleTimeout: 60_000,
	}, func(ctx context.Context, d JobDelivery) error {
		if d.TaskName() == "dlq.me" {
			_ = d.DeadLetter(ctx, DeadLetterOpts{Error: "final"})
			close(done)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop(ctx)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}

	entries, err := rp.Redis().XRange(ctx, DeadLetterKey(prefix, queue), "-", "+").Result()
	if err != nil || len(entries) == 0 {
		t.Fatalf("dlq: %v", err)
	}
	if entries[0].Values["originalId"] != id {
		t.Fatalf("originalId %v", entries[0].Values["originalId"])
	}
	if entries[0].Values["error"] != "final" {
		t.Fatalf("error %v", entries[0].Values["error"])
	}
	// PEL should be empty for that id
	pending, _ := rp.Redis().XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: StreamKey(prefix, queue), Group: prefix + "-g", Start: "-", End: "+", Count: 10,
	}).Result()
	for _, p := range pending {
		if p.ID == id {
			t.Fatal("message still in PEL after dead-letter")
		}
	}
}
