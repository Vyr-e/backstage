package backstage

import (
	"encoding/json"
	"testing"
)

func TestJobMetaJSONTagsRoundTrip(t *testing.T) {
	meta := JobMeta{
		Attempts: 3,
		Backoff:  &BackoffConfig{Type: BackoffFixed, Delay: 500, MaxDelay: 5000},
		Timeout:  2000,
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	var asMap map[string]interface{}
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"attempts", "backoff", "timeout"} {
		if _, ok := asMap[key]; !ok {
			t.Fatalf("missing camelCase key %q in %s", key, raw)
		}
	}
	if _, ok := asMap["Attempts"]; ok {
		t.Fatalf("should not emit PascalCase Attempts: %s", raw)
	}

	// TS-shaped wire body (kafka/rabbit shared JSON)
	tsWire := []byte(`{
		"taskName":"order.process",
		"payload":{"orderId":"o1"},
		"enqueuedAt":1710000000000,
		"meta":{"attempts":3,"backoff":{"type":"fixed","delay":500},"timeout":2000},
		"deliveryCount":2
	}`)
	var decoded struct {
		TaskName      string          `json:"taskName"`
		Payload       json.RawMessage `json:"payload"`
		EnqueuedAt    int64           `json:"enqueuedAt"`
		Meta          JobMeta         `json:"meta"`
		DeliveryCount int             `json:"deliveryCount"`
	}
	if err := json.Unmarshal(tsWire, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Meta.Attempts != 3 || decoded.Meta.Timeout != 2000 || decoded.DeliveryCount != 2 {
		t.Fatalf("decoded meta=%+v deliveryCount=%d", decoded.Meta, decoded.DeliveryCount)
	}
	if decoded.Meta.Backoff == nil || decoded.Meta.Backoff.Type != BackoffFixed || decoded.Meta.Backoff.Delay != 500 {
		t.Fatalf("backoff: %+v", decoded.Meta.Backoff)
	}
	var payload map[string]string
	if err := json.Unmarshal(decoded.Payload, &payload); err != nil || payload["orderId"] != "o1" {
		t.Fatalf("payload %s err=%v", decoded.Payload, err)
	}
}
