package backstage

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

var testRedis *redis.Client
var testCtx = context.Background()

func TestMain(m *testing.M) {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}

	testRedis = redis.NewClient(&redis.Options{
		Addr: host + ":" + port,
	})

	code := m.Run()

	// Cleanup
	keys, _ := testRedis.Keys(testCtx, "backstage:*").Result()
	if len(keys) > 0 {
		testRedis.Del(testCtx, keys...)
	}

	testRedis.Close()
	os.Exit(code)
}

func newTestClient() *Client {
	return New(Config{
		Host:          "localhost",
		Port:          6379,
		ConsumerGroup: "test-group",
		WorkerID:      "test-worker",
	})
}

func TestClientCreation(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	if client == nil {
		t.Fatal("client should not be nil")
	}
}

func TestClientDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Host != "localhost" {
		t.Errorf("expected localhost, got %s", cfg.Host)
	}
	if cfg.Port != 6379 {
		t.Errorf("expected 6379, got %d", cfg.Port)
	}
	if cfg.ConsumerGroup != "backstage-workers" {
		t.Errorf("expected backstage-workers, got %s", cfg.ConsumerGroup)
	}
}

func TestHandlerRegistration(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	client.On("test.handler", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		return nil, nil
	})

	if _, ok := client.handlers["test.handler"]; !ok {
		t.Error("handler not registered")
	}
}

func TestHandlerMultiple(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	client.On("task.one", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		return nil, nil
	})
	client.On("task.two", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		return nil, nil
	})
	client.On("task.three", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		return nil, nil
	})

	if len(client.handlers) != 3 {
		t.Errorf("expected 3 handlers, got %d", len(client.handlers))
	}
}

func TestHandlerWithChaining(t *testing.T) {
	client := newTestClient()
	defer client.Close()

	client.On("step1", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		return &WorkflowInstruction{
			Next:    "step2",
			Delay:   1000,
			Payload: map[string]bool{"step1Done": true},
		}, nil
	})

	client.On("step2", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		return &WorkflowInstruction{
			Next: "step3",
		}, nil
	})

	client.On("step3", func(ctx context.Context, payload json.RawMessage) (*WorkflowInstruction, error) {
		return nil, nil // Terminal
	})

	if _, ok := client.handlers["step1"]; !ok {
		t.Error("step1 not registered")
	}
	if _, ok := client.handlers["step2"]; !ok {
		t.Error("step2 not registered")
	}
	if _, ok := client.handlers["step3"]; !ok {
		t.Error("step3 not registered")
	}
}

func TestConsumerConfig(t *testing.T) {
	cfg := DefaultConsumerConfig()

	if cfg.MaxDeliveries != 5 {
		t.Errorf("expected 5, got %d", cfg.MaxDeliveries)
	}
	if cfg.GracePeriod.Seconds() != 30 {
		t.Errorf("expected 30s, got %v", cfg.GracePeriod)
	}
}
