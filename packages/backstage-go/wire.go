package backstage

import "fmt"

// Wire-format constants shared with the TypeScript SDK for Redis Streams interop.
const (
	WirePrefix               = "backstage"
	WireDefaultConsumerGroup = "backstage-workers"
)

// Wire field names written into Redis stream entries.
const (
	WireFieldTaskName       = "taskName"
	WireFieldPayload        = "payload"
	WireFieldEnqueuedAt     = "enqueuedAt"
	WireFieldAttempts       = "attempts"
	WireFieldBackoff        = "backoff"
	WireFieldTimeout        = "timeout"
	WireFieldOriginalID     = "originalId"
	WireFieldDeliveryCount  = "deliveryCount"
	WireFieldDeadLetteredAt = "deadLetteredAt"
	WireFieldError          = "error"
)

func wireStreamKey(prefix, queue string) string {
	if prefix == "" {
		prefix = WirePrefix
	}
	return fmt.Sprintf("%s:%s", prefix, queue)
}

func wireDeadLetterKey(prefix, queue string) string {
	if prefix == "" {
		prefix = WirePrefix
	}
	return fmt.Sprintf("%s:%s:dead-letter", prefix, queue)
}

func wireScheduledKey(prefix string) string {
	if prefix == "" {
		prefix = WirePrefix
	}
	return fmt.Sprintf("%s:scheduled", prefix)
}

func wireDedupeKey(prefix, key string) string {
	if prefix == "" {
		prefix = WirePrefix
	}
	return fmt.Sprintf("%s:dedupe:%s", prefix, key)
}

func wireBroadcastStream(prefix string) string {
	if prefix == "" {
		prefix = WirePrefix
	}
	return fmt.Sprintf("%s:broadcast", prefix)
}

func wireBroadcastGroup(workerID string) string {
	return "broadcast-" + workerID
}

func wireErrorKey(prefix, id string) string {
	if prefix == "" {
		prefix = WirePrefix
	}
	return fmt.Sprintf("%s:error:%s", prefix, id)
}

// queueFromStreamKey strips the prefix (and optional :dead-letter suffix) to
// recover the logical queue name. Used when mapping Redis stream keys back.
func queueFromStreamKey(prefix, streamKey string) string {
	if prefix == "" {
		prefix = WirePrefix
	}
	p := prefix + ":"
	if len(streamKey) <= len(p) || streamKey[:len(p)] != p {
		return streamKey
	}
	rest := streamKey[len(p):]
	const dlSuffix = ":dead-letter"
	if len(rest) > len(dlSuffix) && rest[len(rest)-len(dlSuffix):] == dlSuffix {
		return rest[:len(rest)-len(dlSuffix)]
	}
	return rest
}
