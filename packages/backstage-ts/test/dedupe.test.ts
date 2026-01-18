/**
 * Deduplication and custom queue tests
 */
import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Stream } from '../src/stream';
import { Queue } from '../src/queue';
import { Priority, type RedisClient } from '../src/types';

let redis: RedisClient;
let stream: Stream;

// Custom queues for testing
const customQueues = [
  new Queue('notifications', { priority: 1 }),
  new Queue('analytics', { priority: 10 }),
];

beforeAll(async () => {
  redis = new Bun.RedisClient('redis://localhost:6379');
  stream = new Stream(redis, 'dedupe-test-group', {
    queues: customQueues,
  });
  await stream.initialize();
});

afterAll(async () => {
  const keys = await redis.send('KEYS', ['backstage:*']);
  if (keys && Array.isArray(keys) && keys.length > 0) {
    await redis.send('DEL', keys as string[]);
  }
});

describe('Deduplication', () => {
  test('first enqueue succeeds', async () => {
    const id = await stream.enqueue(
      'dedupe.test',
      { foo: 'bar' },
      {
        dedupe: { key: 'unique-key-1' },
      },
    );
    expect(id).toBeTruthy();
    expect(id).not.toBeNull();
  });

  test('second enqueue with same key returns null', async () => {
    // First enqueue
    const id1 = await stream.enqueue(
      'dedupe.test',
      { foo: 'bar' },
      {
        dedupe: { key: 'unique-key-2' },
      },
    );
    expect(id1).toBeTruthy();

    // Second enqueue with same key - should be deduplicated
    const id2 = await stream.enqueue(
      'dedupe.test',
      { foo: 'baz' },
      {
        dedupe: { key: 'unique-key-2' },
      },
    );
    expect(id2).toBeNull();
  });

  test('different keys are independent', async () => {
    const id1 = await stream.enqueue(
      'dedupe.test',
      {},
      {
        dedupe: { key: 'key-a' },
      },
    );
    const id2 = await stream.enqueue(
      'dedupe.test',
      {},
      {
        dedupe: { key: 'key-b' },
      },
    );
    expect(id1).toBeTruthy();
    expect(id2).toBeTruthy();
    expect(id1).not.toBe(id2);
  });

  test('short TTL allows re-enqueue', async () => {
    const id1 = await stream.enqueue(
      'dedupe.test',
      {},
      {
        dedupe: { key: 'ttl-test', ttl: 1000 }, // 1 second TTL
      },
    );
    expect(id1).toBeTruthy();

    // Immediate retry should fail
    const id2 = await stream.enqueue(
      'dedupe.test',
      {},
      {
        dedupe: { key: 'ttl-test', ttl: 1000 },
      },
    );
    expect(id2).toBeNull();

    // Wait for TTL to expire
    await Bun.sleep(1100);

    // Should succeed now
    const id3 = await stream.enqueue(
      'dedupe.test',
      {},
      {
        dedupe: { key: 'ttl-test', ttl: 1000 },
      },
    );
    expect(id3).toBeTruthy();
  });
});

describe('Custom Queues', () => {
  test('enqueue to custom queue by name', async () => {
    const id = await stream.enqueue(
      'notification.send',
      { userId: 123 },
      {
        queue: 'notifications',
      },
    );
    expect(id).toBeTruthy();

    // Verify message is in the correct stream
    const len = await redis.send('XLEN', ['backstage:notifications']);
    expect(Number(len)).toBeGreaterThan(0);
  });

  test('stream keys include custom queues', () => {
    const keys = stream.getStreamKeys();

    // Should include default priority queues
    expect(keys).toContain('backstage:urgent');
    expect(keys).toContain('backstage:default');
    expect(keys).toContain('backstage:low');

    // Should include custom queues
    expect(keys).toContain('backstage:notifications');
    expect(keys).toContain('backstage:analytics');
  });

  test('custom queues are sorted by priority', () => {
    const keys = stream.getStreamKeys();
    const notifIndex = keys.indexOf('backstage:notifications');
    const analyticsIndex = keys.indexOf('backstage:analytics');

    // notifications (priority 1) should come before analytics (priority 10)
    expect(notifIndex).toBeLessThan(analyticsIndex);
  });
});

describe('Enhanced Job Options', () => {
  test('stores attempts in message', async () => {
    const id = await stream.enqueue(
      'retry.task',
      { data: 1 },
      {
        attempts: 3,
      },
    );
    expect(id).toBeTruthy();

    // Read the message back to verify metadata
    if (!id) throw new Error('ID should not be null');
    const messages = await redis.send('XRANGE', ['backstage:default', id, id]);
    expect(messages).toBeTruthy();
    expect(Array.isArray(messages)).toBe(true);
    if (messages && Array.isArray(messages) && messages.length > 0) {
      const [, fields] = messages[0] as [string, string[]];
      const fieldMap = new Map<string, string>();
      for (let i = 0; i < fields.length; i += 2) {
        const key = fields[i];
        const value = fields[i + 1];
        if (key && value) {
          fieldMap.set(key, value);
        }
      }
      expect(fieldMap.get('attempts')).toBe('3');
    }
  });

  test('stores backoff strategy in message', async () => {
    const id = await stream.enqueue(
      'backoff.task',
      { data: 1 },
      {
        backoff: { type: 'exponential', delay: 1000, maxDelay: 60000 },
      },
    );
    expect(id).toBeTruthy();

    if (!id) throw new Error('ID should not be null');
    const messages = await redis.send('XRANGE', ['backstage:default', id, id]);
    if (messages && Array.isArray(messages) && messages.length > 0) {
      const [, fields] = messages[0] as [string, string[]];
      const fieldMap = new Map<string, string>();
      for (let i = 0; i < fields.length; i += 2) {
        const key = fields[i];
        const value = fields[i + 1];
        if (key && value) {
          fieldMap.set(key, value);
        }
      }
      const backoff = JSON.parse(fieldMap.get('backoff') || '{}');
      expect(backoff.type).toBe('exponential');
      expect(backoff.delay).toBe(1000);
      expect(backoff.maxDelay).toBe(60000);
    }
  });

  test('stores timeout in message', async () => {
    const id = await stream.enqueue(
      'timeout.task',
      { data: 1 },
      {
        timeout: 30000,
      },
    );
    expect(id).toBeTruthy();

    if (!id) throw new Error('ID should not be null');
    const messages = await redis.send('XRANGE', ['backstage:default', id, id]);
    if (messages && Array.isArray(messages) && messages.length > 0) {
      const [, fields] = messages[0] as [string, string[]];
      const fieldMap = new Map<string, string>();
      for (let i = 0; i < fields.length; i += 2) {
        const key = fields[i];
        const value = fields[i + 1];
        if (key && value) {
          fieldMap.set(key, value);
        }
      }
      expect(fieldMap.get('timeout')).toBe('30000');
    }
  });
});
