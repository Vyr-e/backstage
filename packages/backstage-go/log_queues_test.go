package backstage

import (
	"context"
	"testing"
	"time"
)

func TestLogQueuesSkipsWithoutRedis(t *testing.T) {
	c := &Client{logger: NewLogger("test", LoggerConfig{Silent: true})}
	done := make(chan struct{})
	go func() {
		c.LogQueues(context.Background(), time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("LogQueues should return immediately when there is no Redis client")
	}
}

func TestInspectWithoutRedis(t *testing.T) {
	if _, err := Inspect(context.Background(), nil, []*Queue{NewQueue("q")}); err == nil {
		t.Fatal("expected an error for a nil Redis client")
	}
}
