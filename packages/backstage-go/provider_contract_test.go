package backstage

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// fakeProvider is an in-memory Provider for contract tests (no Redis/Rabbit/Kafka).
type fakeProvider struct {
	mu       sync.Mutex
	queues   map[string][]MessageRef
	pending  map[string]struct {
		msg       MessageRef
		claimedAt time.Time
	}
	delayed []struct {
		executeAt int64
		msg       MessageRef
	}
	dedupe   map[string]int64
	bcast    []MessageRef
	bcastPend map[string]map[string]struct{}
	dlq      []MessageRef
	seq      int
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{
		queues:    make(map[string][]MessageRef),
		pending:   make(map[string]struct{ msg MessageRef; claimedAt time.Time }),
		dedupe:    make(map[string]int64),
		bcastPend: make(map[string]map[string]struct{}),
	}
}

func (f *fakeProvider) Name() string { return "fake" }
func (f *fakeProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{Durable: true, Broadcast: true, Scheduling: true, Retries: true, Deduplication: true}
}

func (f *fakeProvider) EnsureQueues(ctx context.Context, queues []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, q := range queues {
		if _, ok := f.queues[q]; !ok {
			f.queues[q] = nil
		}
	}
	return nil
}

func (f *fakeProvider) Close() error { return nil }

func (f *fakeProvider) Publish(ctx context.Context, taskName string, payload interface{}, opts PublishOptions) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if opts.Dedupe != nil {
		now := time.Now().UnixMilli()
		if exp, ok := f.dedupe[opts.Dedupe.Key]; ok && exp > now {
			return "", nil
		}
		ttl := opts.Dedupe.TTL
		if ttl == 0 {
			ttl = time.Hour
		}
		f.dedupe[opts.Dedupe.Key] = now + ttl.Milliseconds()
	}
	queue := opts.Queue
	if queue == "" {
		if opts.Priority != "" {
			queue = string(opts.Priority)
		} else {
			queue = "default"
		}
	}
	if _, ok := f.queues[queue]; !ok {
		f.queues[queue] = nil
	}
	f.seq++
	id := "fake-" + string(rune('0'+f.seq%10))
	// simpler unique id
	id = time.Now().Format("150405.000") + "-" + string(rune('a'+f.seq%26))
	body, _ := json.Marshal(payload)
	enqueuedAt := time.Now().UnixMilli()
	ref := MessageRef{
		ID: id, Queue: queue, TaskName: taskName, Payload: body,
		EnqueuedAt: enqueuedAt, DeliveryCount: 1,
		Attempts: opts.Attempts, Backoff: opts.Backoff,
		Timeout: opts.Timeout.Milliseconds(),
	}
	if opts.Delay > 0 {
		f.delayed = append(f.delayed, struct {
			executeAt int64
			msg       MessageRef
		}{enqueuedAt + opts.Delay.Milliseconds(), ref})
		return "scheduled:" + string(rune('0')), nil
	}
	f.queues[queue] = append(f.queues[queue], ref)
	return id, nil
}

func (f *fakeProvider) Consume(ctx context.Context, args ConsumeArgs) ([]MessageRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []MessageRef
	for _, q := range args.Queues {
		list := f.queues[q]
		for len(out) < int(args.MaxMessages) && len(list) > 0 {
			msg := list[0]
			list = list[1:]
			f.pending[msg.ID] = struct {
				msg       MessageRef
				claimedAt time.Time
			}{msg, time.Now()}
			out = append(out, msg)
		}
		f.queues[q] = list
	}
	return out, nil
}

func (f *fakeProvider) Ack(ctx context.Context, messages []MessageRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range messages {
		delete(f.pending, m.ID)
	}
	return nil
}

func (f *fakeProvider) AckAndForget(ctx context.Context, messages []MessageRef) error {
	return f.Ack(ctx, messages)
}

func (f *fakeProvider) ReclaimIdle(ctx context.Context, args ReclaimIdleArgs) ([]MessageRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	var claimed []MessageRef
	for id, entry := range f.pending {
		if len(claimed) >= int(args.MaxCount) && args.MaxCount > 0 {
			break
		}
		found := false
		for _, q := range args.Queues {
			if entry.msg.Queue == q {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		if now.Sub(entry.claimedAt) < time.Duration(args.IdleMs)*time.Millisecond {
			continue
		}
		entry.msg.DeliveryCount++
		entry.claimedAt = now
		f.pending[id] = entry
		claimed = append(claimed, entry.msg)
	}
	return claimed, nil
}

func (f *fakeProvider) DeadLetter(ctx context.Context, message MessageRef, meta DeadLetterMeta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	message.ID = "dlq-" + meta.OriginalID
	f.dlq = append(f.dlq, message)
	delete(f.pending, meta.OriginalID)
	return nil
}

func (f *fakeProvider) PromoteDueScheduled(ctx context.Context, nowMs int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	var due int64
	var remaining []struct {
		executeAt int64
		msg       MessageRef
	}
	for _, d := range f.delayed {
		if d.executeAt <= nowMs {
			f.queues[d.msg.Queue] = append(f.queues[d.msg.Queue], d.msg)
			due++
		} else {
			remaining = append(remaining, d)
		}
	}
	f.delayed = remaining
	return due, nil
}

func (f *fakeProvider) EnsureBroadcast(ctx context.Context, consumerIdentity string, start BroadcastStart) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.bcastPend[consumerIdentity]; !ok {
		f.bcastPend[consumerIdentity] = make(map[string]struct{})
	}
	return nil
}

func (f *fakeProvider) Broadcast(ctx context.Context, taskName string, payload interface{}) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := "bcast-" + string(rune('a'+f.seq%26))
	body, _ := json.Marshal(payload)
	ref := MessageRef{
		ID: id, Queue: "broadcast", TaskName: taskName,
		Payload: body, EnqueuedAt: time.Now().UnixMilli(), DeliveryCount: 1,
	}
	f.bcast = append(f.bcast, ref)
	return id, nil
}

func (f *fakeProvider) ConsumeBroadcast(ctx context.Context, consumerIdentity string, maxMessages int64, blockMs int64) ([]MessageRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.bcastPend[consumerIdentity]; !ok {
		f.bcastPend[consumerIdentity] = make(map[string]struct{})
	}
	var out []MessageRef
	for _, m := range f.bcast {
		if _, seen := f.bcastPend[consumerIdentity][m.ID]; seen {
			continue
		}
		if len(out) >= int(maxMessages) {
			break
		}
		f.bcastPend[consumerIdentity][m.ID] = struct{}{}
		f.pending[m.ID] = struct {
			msg       MessageRef
			claimedAt time.Time
		}{m, time.Now()}
		out = append(out, m)
	}
	return out, nil
}

func (f *fakeProvider) AckBroadcast(ctx context.Context, consumerIdentity string, ids []string) error {
	msgs := make([]MessageRef, len(ids))
	for i, id := range ids {
		msgs[i] = MessageRef{ID: id}
	}
	return f.Ack(ctx, msgs)
}

func (f *fakeProvider) CleanupBroadcastGhosts(ctx context.Context, idleMs int64) (int64, error) {
	return 0, nil
}

func TestProviderContractFake(t *testing.T) {
	ctx := context.Background()
	p := newFakeProvider()
	client := NewWithProvider(p, Config{
		ConsumerGroup: "contract-group",
		WorkerID:      "contract-worker",
		Queues:        []string{"default"},
	})
	defer client.Close()

	id, err := client.Enqueue(ctx, "contract.task", map[string]string{"x": "1"})
	if err != nil || id == "" {
		t.Fatalf("enqueue: id=%q err=%v", id, err)
	}

	// Dedupe
	id2, err := client.Enqueue(ctx, "contract.task", map[string]string{"x": "2"}, EnqueueOptions{
		Dedupe: &DedupeConfig{Key: "same", TTL: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	id3, err := client.Enqueue(ctx, "contract.task", map[string]string{"x": "3"}, EnqueueOptions{
		Dedupe: &DedupeConfig{Key: "same", TTL: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	if id2 == "" || id3 != "" {
		t.Fatalf("dedupe: first=%q second=%q", id2, id3)
	}

	// Delayed + promote
	_, err = client.Schedule(ctx, "contract.delayed", map[string]int{"n": 1}, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := p.PromoteDueScheduled(ctx, time.Now().Add(time.Second).UnixMilli())
	if n < 1 {
		t.Fatalf("expected promoted >= 1, got %d", n)
	}

	msgs, err := p.Consume(ctx, ConsumeArgs{
		Queues: []string{"default"}, ConsumerGroup: "g", ConsumerID: "c", MaxMessages: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected messages")
	}
	if err := p.Ack(ctx, msgs); err != nil {
		t.Fatal(err)
	}
}

func TestRedisProviderPublishConsumeAck(t *testing.T) {
	ctx := context.Background()
	rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{
		Host: "localhost", Port: testPort(),
		ConsumerGroup: "provider-contract-redis",
		Prefix:        "backstage",
	})
	defer rp.Close()

	queue := "provider-contract"
	_ = rp.Client().Del(ctx, wireStreamKey("backstage", queue), wireDeadLetterKey("backstage", queue))

	if err := rp.EnsureQueues(ctx, []string{queue}); err != nil {
		t.Fatal(err)
	}

	id, err := rp.Publish(ctx, "p.task", map[string]string{"a": "b"}, PublishOptions{Queue: queue})
	if err != nil || id == "" {
		t.Fatalf("publish: %q %v", id, err)
	}

	msgs, err := rp.Consume(ctx, ConsumeArgs{
		Queues: []string{queue}, ConsumerGroup: "provider-contract-redis",
		ConsumerID: "w1", MaxMessages: 5, BlockMs: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected at least one message")
	}
	if msgs[0].TaskName != "p.task" {
		t.Errorf("taskName=%s", msgs[0].TaskName)
	}
	if err := rp.Ack(ctx, msgs); err != nil {
		t.Fatal(err)
	}
}

func TestRedisProviderCustomQueueDLQ(t *testing.T) {
	ctx := context.Background()
	rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{
		Host: "localhost", Port: testPort(),
		ConsumerGroup: "provider-dlq-group",
	})
	defer rp.Close()

	queue := "custom-dlq-q"
	dlKey := wireDeadLetterKey("backstage", queue)
	_ = rp.Client().Del(ctx, wireStreamKey("backstage", queue), dlKey)

	ref := MessageRef{
		ID: "1-0", Queue: queue, TaskName: "fail.task",
		Payload: json.RawMessage(`{}`), EnqueuedAt: time.Now().UnixMilli(), DeliveryCount: 6,
	}
	// Seed stream + group so Ack works
	_ = rp.EnsureQueues(ctx, []string{queue})
	id, _ := rp.Publish(ctx, "fail.task", map[string]string{}, PublishOptions{Queue: queue})
	ref.ID = id
	msgs, _ := rp.Consume(ctx, ConsumeArgs{
		Queues: []string{queue}, ConsumerGroup: "provider-dlq-group",
		ConsumerID: "w1", MaxMessages: 1, BlockMs: 200,
	})
	if len(msgs) > 0 {
		ref = msgs[0]
	}

	if err := rp.DeadLetter(ctx, ref, DeadLetterMeta{
		OriginalID: ref.ID, DeliveryCount: 6, Error: "boom",
	}); err != nil {
		t.Fatal(err)
	}

	entries, err := rp.Client().XRange(ctx, dlKey, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected DLQ entry at %s", dlKey)
	}
	if entries[0].Values["error"] != "boom" {
		t.Errorf("error field=%v", entries[0].Values["error"])
	}
}

func TestNewWithProviderUsesInjected(t *testing.T) {
	p := newFakeProvider()
	c := NewWithProvider(p, Config{Queues: []string{"default"}, WorkerID: "w"})
	defer c.Close()
	if c.Provider().Name() != "fake" {
		t.Fatalf("got %s", c.Provider().Name())
	}
	if c.Redis() != nil {
		t.Fatal("expected nil Redis for fake provider")
	}
}
