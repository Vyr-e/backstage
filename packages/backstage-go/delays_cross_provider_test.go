package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// recordingJobs is a non-Redis jobs transport that records what is published.
type recordingJobs struct{ published chan OutgoingJob }

func (j *recordingJobs) Name() string                                 { return "recording" }
func (j *recordingJobs) Requires() []CapabilityName                   { return []CapabilityName{CapabilityDelays} }
func (j *recordingJobs) EnsureQueues(context.Context, []string) error { return nil }
func (j *recordingJobs) Publish(_ context.Context, job OutgoingJob) (string, error) {
	j.published <- job
	return "rec-1", nil
}
func (j *recordingJobs) Consume(context.Context, ConsumeOptions, func(context.Context, JobDelivery) error) (Subscription, error) {
	return noopSub{}, nil
}

type noopSub struct{}

func (noopSub) Stop(context.Context) error { return nil }

type recordingProvider struct{ jobs *recordingJobs }

func (p *recordingProvider) Name() string                                { return "recording" }
func (p *recordingProvider) Jobs() Jobs                                  { return p.jobs }
func (p *recordingProvider) Topics() Topics                              { return nil }
func (p *recordingProvider) Delays() Delays                              { return nil }
func (p *recordingProvider) Dedupe() Dedupe                              { return nil }
func (p *recordingProvider) Init(context.Context, ProviderContext) error { return nil }
func (p *recordingProvider) Close() error                                { return nil }

// A non-Redis transport with Redis plugged in for delays (e.g. Kafka): a due
// delayed job must be promoted onto the active transport, not left in Redis.
func TestDelaysPromoteOntoNonRedisTransport(t *testing.T) {
	prefix := fmt.Sprintf("xdelay-%d", time.Now().UnixNano())
	rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{Host: "localhost", Port: 6379, Prefix: prefix})
	defer func() {
		keys, _ := rp.Redis().Keys(context.Background(), prefix+":*").Result()
		if len(keys) > 0 {
			rp.Redis().Del(context.Background(), keys...)
		}
		_ = rp.Close()
	}()

	jobs := &recordingJobs{published: make(chan OutgoingJob, 1)}
	c := New(Config{Provider: &recordingProvider{jobs: jobs}, Capabilities: &Capabilities{Delays: rp.Delays()}})
	c.On("delayed:task", func(context.Context, json.RawMessage) (*WorkflowInstruction, error) { return nil, nil })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := c.Enqueue(ctx, "delayed:task", map[string]int{"n": 1}, EnqueueOptions{Delay: 200 * time.Millisecond}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	go func() { _ = c.Start(ctx, DefaultConsumerConfig()) }()
	defer c.Stop()

	select {
	case job := <-jobs.published:
		if job.TaskName != "delayed:task" {
			t.Fatalf("promoted %q, want delayed:task", job.TaskName)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delayed job was never promoted onto the active transport")
	}
}
