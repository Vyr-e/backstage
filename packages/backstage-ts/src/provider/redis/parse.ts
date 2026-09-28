import { parseFields, type BackoffConfig } from '../../types';
import { queueFromStreamKey } from '../../wire';
import type { MessageRef } from '../types';

export function parseStreamEntries(
  result: unknown,
): Array<{ streamKey: string; messages: Array<{ id: string; fields: unknown[] }> }> {
  if (!result || typeof result !== 'object') return [];

  const entries = Array.isArray(result)
    ? (result as [string, unknown[]][])
    : (Object.entries(result) as [string, unknown[]][]);

  const out: Array<{
    streamKey: string;
    messages: Array<{ id: string; fields: unknown[] }>;
  }> = [];

  for (const [streamKey, messages] of entries) {
    if (!Array.isArray(messages) || messages.length === 0) continue;
    const parsed: Array<{ id: string; fields: unknown[] }> = [];
    for (const msgEntry of messages) {
      if (!Array.isArray(msgEntry) || msgEntry.length < 2) continue;
      const [msgId, fields] = msgEntry as [string, unknown[]];
      parsed.push({ id: msgId, fields });
    }
    if (parsed.length > 0) {
      out.push({ streamKey, messages: parsed });
    }
  }
  return out;
}

export function fieldsToMessageRef(
  id: string,
  streamKey: string,
  fields: unknown[],
  deliveryCount: number = 1,
): MessageRef | null {
  try {
    const data = parseFields(fields);
    let backoff: BackoffConfig | undefined;
    if (data.backoff) {
      try {
        backoff = JSON.parse(data.backoff);
      } catch {
        /* ignore */
      }
    }
    return {
      id,
      queue: queueFromStreamKey(streamKey),
      taskName: data.taskName || '',
      payload: JSON.parse(data.payload || 'null'),
      deliveryCount,
      enqueuedAt: parseInt(data.enqueuedAt || '0', 10) || Date.now(),
      attempts: data.attempts ? parseInt(data.attempts, 10) : undefined,
      backoff,
      timeout: data.timeout ? parseInt(data.timeout, 10) : undefined,
    };
  } catch {
    return null;
  }
}

export function parseRedisInfo(data: unknown): Record<string, unknown> {
  const info: Record<string, unknown> = {};
  if (Array.isArray(data)) {
    for (let i = 0; i < data.length; i += 2) {
      const key = data[i];
      const value = data[i + 1];
      if (typeof key === 'string') {
        info[key] = value;
      }
    }
  }
  return info;
}

export function calculateBackoff(
  config: BackoffConfig,
  deliveryCount: number,
): number {
  const retries = Math.max(0, deliveryCount - 1);
  if (config.type === 'fixed') return config.delay;
  if (config.type === 'exponential') {
    const delay = config.delay * Math.pow(2, retries - 1);
    const max = config.maxDelay ?? 3600000;
    return Math.min(delay, max);
  }
  return 0;
}
