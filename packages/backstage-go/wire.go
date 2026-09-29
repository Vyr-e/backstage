package backstage

import "fmt"

func StreamKey(prefix, queue string) string {
	if prefix == "" {
		prefix = StreamPrefix
	}
	return fmt.Sprintf("%s:%s", prefix, queue)
}

func ScheduledKey(prefix string) string {
	if prefix == "" {
		prefix = StreamPrefix
	}
	return fmt.Sprintf("%s:scheduled", prefix)
}

func ScheduledClaimedKey(prefix string) string {
	if prefix == "" {
		prefix = StreamPrefix
	}
	return fmt.Sprintf("%s:scheduled:claimed", prefix)
}

func DedupeKey(prefix, key string) string {
	if prefix == "" {
		prefix = StreamPrefix
	}
	return fmt.Sprintf("%s:dedupe:%s", prefix, key)
}

func DeadLetterKey(prefix, queue string) string {
	if prefix == "" {
		prefix = StreamPrefix
	}
	return fmt.Sprintf("%s:%s:dead-letter", prefix, queue)
}

func ErrorKey(prefix, id string) string {
	if prefix == "" {
		prefix = StreamPrefix
	}
	return fmt.Sprintf("%s:error:%s", prefix, id)
}

func TopicStreamKey(prefix, topic string) string {
	if prefix == "" {
		prefix = StreamPrefix
	}
	return fmt.Sprintf("%s:topic:%s", prefix, topic)
}

func TopicFanoutGroup(consumerID string) string { return "sub:" + consumerID }
func TopicNamedGroup(group string) string       { return "grp:" + group }

func BroadcastStreamKey(prefix string) string {
	if prefix == "" {
		prefix = StreamPrefix
	}
	return fmt.Sprintf("%s:broadcast", prefix)
}

func BroadcastGroupName(workerID string) string { return "broadcast-" + workerID }

func QueueFromStreamKey(prefix, streamKey string) string {
	if prefix == "" {
		prefix = StreamPrefix
	}
	p := prefix + ":"
	if len(streamKey) <= len(p) || streamKey[:len(p)] != p {
		return streamKey
	}
	return streamKey[len(p):]
}
