/**
 * Shared wire-format constants for Go ↔ TS interop.
 * RedisStreamsProvider must preserve these exactly.
 */

export const WIRE_PREFIX = 'backstage';

export const WIRE_DEFAULT_CONSUMER_GROUP = 'backstage-workers';

export const WIRE_SCHEDULED_KEY = `${WIRE_PREFIX}:scheduled`;

export const WIRE_BROADCAST_STREAM = `${WIRE_PREFIX}:broadcast`;

export const WIRE_FIELD = {
  taskName: 'taskName',
  payload: 'payload',
  enqueuedAt: 'enqueuedAt',
  attempts: 'attempts',
  backoff: 'backoff',
  timeout: 'timeout',
  originalId: 'originalId',
  deliveryCount: 'deliveryCount',
  deadLetteredAt: 'deadLetteredAt',
} as const;

export function wireStreamKey(queue: string): string {
  return `${WIRE_PREFIX}:${queue}`;
}

export function wireDeadLetterKey(queue: string): string {
  return `${WIRE_PREFIX}:${queue}:dead-letter`;
}

export function wireDedupeKey(key: string): string {
  return `${WIRE_PREFIX}:dedupe:${key}`;
}

export function wireBroadcastGroup(workerId: string): string {
  return `broadcast-${workerId}`;
}

export function queueFromStreamKey(streamKey: string): string {
  const prefix = `${WIRE_PREFIX}:`;
  if (!streamKey.startsWith(prefix)) return streamKey;
  const rest = streamKey.slice(prefix.length);
  if (rest.endsWith(':dead-letter')) {
    return rest.slice(0, -':dead-letter'.length);
  }
  return rest;
}
