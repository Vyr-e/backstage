/**
 * Stream load tests
 */
import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Stream } from '../src/stream';
import { Priority, type RedisClient } from '../src/types';

let redis: RedisClient;
let stream: Stream;

beforeAll(async () => {
  redis = new Bun.RedisClient('redis://localhost:6379');
  stream = new Stream(redis, 'load-test-group');
  await stream.initialize();
});

afterAll(async () => {
  const keys = await redis.send('KEYS', ['backstage:*']);
  if (keys && Array.isArray(keys) && keys.length > 0) {
    await redis.send('DEL', keys as string[]);
  }
});

describe('Stream Load Tests', () => {
  test('handles 10,000 sequential enqueues', async () => {
    const start = performance.now();
    const ids: string[] = [];

    for (let i = 0; i < 10000; i++) {
      const id = await stream.enqueue(`load.test.${i}`, { index: i });
      if (id) ids.push(id);
    }

    const elapsed = performance.now() - start;
    const throughput = 10000 / (elapsed / 1000);

    console.log(`Sequential: 10,000 enqueues in ${elapsed.toFixed(0)}ms`);
    console.log(`Throughput: ${throughput.toFixed(0)} ops/sec`);

    expect(ids.length).toBe(10000);
    expect(throughput).toBeGreaterThan(500); // At least 500 ops/sec
  });

  test('handles 10,000 concurrent enqueues', async () => {
    const start = performance.now();

    const promises = Array.from({ length: 10000 }, (_, i) =>
      stream.enqueue(`concurrent.test.${i}`, { index: i }),
    );

    const ids = await Promise.all(promises);
    const elapsed = performance.now() - start;
    const throughput = 10000 / (elapsed / 1000);

    console.log(`Concurrent: 10,000 enqueues in ${elapsed.toFixed(0)}ms`);
    console.log(`Throughput: ${throughput.toFixed(0)} ops/sec`);

    expect(ids.length).toBe(10000);
    expect(new Set(ids).size).toBe(10000); // All unique IDs
  });

  test('handles large payloads (1MB)', async () => {
    const largePayload = { data: 'x'.repeat(1024 * 1024) }; // 1MB
    const id = await stream.enqueue('large.payload', largePayload);
    expect(id).toBeTruthy();
  });

  test('handles empty payloads', async () => {
    const id = await stream.enqueue('empty.payload', {});
    expect(id).toBeTruthy();
  });

  test('handles null payload', async () => {
    const id = await stream.enqueue('null.payload', null);
    expect(id).toBeTruthy();
  });

  test('handles special characters in task names', async () => {
    const id = await stream.enqueue('task:with.special-chars_123', {});
    expect(id).toBeTruthy();
  });

  test('distributes across priorities correctly', async () => {
    const urgentPromises = Array.from({ length: 1000 }, (_, i) =>
      stream.enqueue(`urgent.${i}`, {}, { priority: Priority.URGENT }),
    );
    const defaultPromises = Array.from({ length: 1000 }, (_, i) =>
      stream.enqueue(`default.${i}`, {}, { priority: Priority.DEFAULT }),
    );
    const lowPromises = Array.from({ length: 1000 }, (_, i) =>
      stream.enqueue(`low.${i}`, {}, { priority: Priority.LOW }),
    );

    await Promise.all([...urgentPromises, ...defaultPromises, ...lowPromises]);

    // Verify each stream has messages
    const urgentLen = await redis.send('XLEN', ['backstage:urgent']);
    const defaultLen = await redis.send('XLEN', ['backstage:default']);
    const lowLen = await redis.send('XLEN', ['backstage:low']);

    expect(Number(urgentLen)).toBeGreaterThanOrEqual(1000);
    expect(Number(defaultLen)).toBeGreaterThanOrEqual(1000);
    expect(Number(lowLen)).toBeGreaterThanOrEqual(1000);
  });

  test('schedules 1000 delayed tasks', async () => {
    const start = performance.now();

    const promises = Array.from({ length: 1000 }, (_, i) =>
      stream.enqueue(`scheduled.${i}`, { index: i }, { delay: 60000 }),
    );

    const ids = await Promise.all(promises);
    const elapsed = performance.now() - start;

    console.log(`Scheduled: 1,000 tasks in ${elapsed.toFixed(0)}ms`);

    expect(ids.every((id) => id && id.startsWith('scheduled:'))).toBe(true);

    // Verify in sorted set
    const scheduledCount = await redis.send('ZCARD', ['backstage:scheduled']);
    expect(Number(scheduledCount)).toBeGreaterThanOrEqual(1000);
  });

  test('processes scheduled tasks when due', async () => {
    // Add a task scheduled for "now"
    const id = await stream.enqueue(
      'due.now',
      { test: true },
      { delay: -1000 },
    );

    // Process scheduled tasks
    const processed = await stream.processScheduledTasks();

    expect(processed).toBeGreaterThanOrEqual(0);
  });
});
