package backstage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestEncodePayload(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		b, err := EncodePayload(nil)
		if err != nil || string(b) != "null" {
			t.Fatalf("got %s err=%v", b, err)
		}
	})
	t.Run("rawMessage", func(t *testing.T) {
		raw := json.RawMessage(`{"a":1}`)
		b, err := EncodePayload(raw)
		if err != nil || !bytes.Equal(b, []byte(`{"a":1}`)) {
			t.Fatalf("got %s err=%v", b, err)
		}
	})
	t.Run("emptyRawMessage", func(t *testing.T) {
		b, err := EncodePayload(json.RawMessage{})
		if err != nil || string(b) != "null" {
			t.Fatalf("got %s err=%v", b, err)
		}
	})
	t.Run("validJSONBytes", func(t *testing.T) {
		sonicBytes := []byte(`{"hello":"world"}`)
		b, err := EncodePayload(sonicBytes)
		if err != nil || !bytes.Equal(b, sonicBytes) {
			t.Fatalf("got %s err=%v", b, err)
		}
	})
	t.Run("invalidJSONBytesBase64", func(t *testing.T) {
		raw := []byte("not-json")
		want, _ := json.Marshal(raw)
		b, err := EncodePayload(raw)
		if err != nil || !bytes.Equal(b, want) {
			t.Fatalf("got %s want %s err=%v", b, want, err)
		}
	})
	t.Run("struct", func(t *testing.T) {
		b, err := EncodePayload(map[string]int{"x": 1})
		if err != nil || string(b) != `{"x":1}` {
			t.Fatalf("got %s err=%v", b, err)
		}
	})
}

func TestEncodePayloadEnqueueRedis(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("ep-%d", time.Now().UnixNano())
	client := New(Config{Host: "localhost", Port: testPort(), Prefix: prefix, WorkerID: "ep", ConsumerGroup: "ep-g"})
	defer client.Close()
	defer func() {
		keys, _ := client.redis.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			client.redis.Del(ctx, keys...)
		}
	}()

	sonicBytes := []byte(`{"hello":"world","n":1}`)
	id, err := client.Enqueue(ctx, "t", sonicBytes)
	if err != nil || id == "" {
		t.Fatalf("enqueue valid: %v id=%s", err, id)
	}
	msgs, err := client.redis.XRange(ctx, StreamKey(prefix, "default"), id, id).Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("xrange: %v len=%d", err, len(msgs))
	}
	got, _ := msgs[0].Values["payload"].(string)
	if got != string(sonicBytes) {
		t.Fatalf("valid JSON []byte: want object %s, got %q (base64?)", sonicBytes, got)
	}
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(got), &obj); err != nil {
		t.Fatalf("payload should be JSON object: %v", err)
	}
	if obj["hello"] != "world" {
		t.Fatalf("obj %#v", obj)
	}

	invalid := []byte("not-json")
	wantInvalid, _ := json.Marshal(invalid)
	id2, err := client.Enqueue(ctx, "t", invalid)
	if err != nil || id2 == "" {
		t.Fatalf("enqueue invalid: %v id=%s", err, id2)
	}
	msgs2, err := client.redis.XRange(ctx, StreamKey(prefix, "default"), id2, id2).Result()
	if err != nil || len(msgs2) != 1 {
		t.Fatalf("xrange invalid: %v", err)
	}
	got2, _ := msgs2[0].Values["payload"].(string)
	if got2 != string(wantInvalid) {
		t.Fatalf("invalid []byte: want base64 JSON %s, got %q", wantInvalid, got2)
	}
}
