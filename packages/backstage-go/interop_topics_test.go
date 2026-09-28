package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestInteropTopicsFormat(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("itopic-%d", time.Now().UnixNano())
	client := New(Config{Host: "localhost", Port: testPort(), Prefix: prefix, WorkerID: "tw", ConsumerGroup: "tg"})
	defer client.Close()
	defer func() {
		keys, _ := client.redis.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			client.redis.Del(ctx, keys...)
		}
	}()

	id, err := client.Publish(ctx, "ride.cancelled", map[string]string{"rideId": "r1"})
	if err != nil || id == "" {
		t.Fatalf("publish: %v", err)
	}
	key := TopicStreamKey(prefix, "ride.cancelled")
	msgs, err := client.redis.XRange(ctx, key, "-", "+").Result()
	if err != nil || len(msgs) == 0 {
		t.Fatalf("xrange: %v", err)
	}
	payload, _ := msgs[0].Values["payload"].(string)
	var m map[string]string
	if err := json.Unmarshal([]byte(payload), &m); err != nil || m["rideId"] != "r1" {
		t.Fatalf("payload %s err %v", payload, err)
	}
	if msgs[0].Values["publishedAt"] == nil {
		t.Fatal("missing publishedAt")
	}
}

func TestInteropTopicsFanout(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("ifan-%d", time.Now().UnixNano())
	client := New(Config{Host: "localhost", Port: testPort(), Prefix: prefix, WorkerID: "tw", ConsumerGroup: "tg"})
	defer client.Close()
	defer func() {
		keys, _ := client.redis.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			client.redis.Del(ctx, keys...)
		}
	}()

	gotA := make(chan struct{}, 1)
	gotB := make(chan struct{}, 1)
	client.Subscribe("evt", func(ctx context.Context, payload json.RawMessage, msg TopicMessage) error {
		gotA <- struct{}{}
		return nil
	})
	// second fan-out via direct topics API with different consumer id
	subB, err := client.resolved.Topics.Subscribe(ctx, TopicSubscribeOptions{
		Topic: "evt", ConsumerID: "fan-b", From: TopicFromLatest,
	}, func(ctx context.Context, m TopicDelivery) error {
		gotB <- struct{}{}
		return m.Ack(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subB.Stop(ctx)

	// start client topic sub
	go client.Start(ctx, ConsumerConfig{BlockTimeout: 100 * time.Millisecond, IdleTimeout: time.Minute, MaxDeliveries: 5, Prefetch: 2, Concurrency: 2, ReclaimerInterval: time.Hour, GracePeriod: time.Second})
	defer client.Stop()
	time.Sleep(150 * time.Millisecond)

	_, err = client.Publish(ctx, "evt", map[string]int{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-gotA:
	case <-time.After(3 * time.Second):
		t.Fatal("fan A timeout")
	}
	select {
	case <-gotB:
	case <-time.After(3 * time.Second):
		t.Fatal("fan B timeout")
	}
}
