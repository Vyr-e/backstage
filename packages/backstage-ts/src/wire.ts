export const STREAM_PREFIX = 'backstage';

export function streamKey(prefix: string, queue: string): string {
  return `${prefix}:${queue}`;
}

export function scheduledKey(prefix: string): string {
  return `${prefix}:scheduled`;
}

export function scheduledClaimedKey(prefix: string): string {
  return `${prefix}:scheduled:claimed`;
}

export function dedupeKey(prefix: string, key: string): string {
  return `${prefix}:dedupe:${key}`;
}

export function deadLetterKey(prefix: string, queue: string): string {
  return `${prefix}:${queue}:dead-letter`;
}

export function errorKey(prefix: string, id: string): string {
  return `${prefix}:error:${id}`;
}

export function topicStreamKey(prefix: string, topic: string): string {
  return `${prefix}:topic:${topic}`;
}

export function topicFanoutGroup(consumerId: string): string {
  return `sub:${consumerId}`;
}

export function topicNamedGroup(group: string): string {
  return `grp:${group}`;
}

export const BROADCAST_STREAM_SUFFIX = 'broadcast';

export function broadcastStreamKey(prefix: string): string {
  return `${prefix}:${BROADCAST_STREAM_SUFFIX}`;
}

export function broadcastGroup(workerId: string): string {
  return `broadcast-${workerId}`;
}
