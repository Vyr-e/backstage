import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Stream } from '../src/stream';
import { Queue } from '../src/queue';
import { Worker } from '../src/worker';
import { Priority, type RedisClient } from '../src/types';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

describe('Queue Override — Stream', () => {
  test('queues replace default priority queues when provided', () => {
    const custom = new Queue('my-queue');
    const stream = new Stream({} as any, 'test-group', {
      queues: [custom],
    });

    const keys = stream.getStreamKeys();
    expect(keys).toEqual(['backstage:my-queue']);
    expect(keys).not.toContain('backstage:urgent');
    expect(keys).not.toContain('backstage:default');
    expect(keys).not.toContain('backstage:low');
  });

  test('default queues used when no custom queues', () => {
    const stream = new Stream({} as any, 'test-group');
    const keys = stream.getStreamKeys();
    expect(keys).toEqual([
      'backstage:urgent',
      'backstage:default',
      'backstage:low',
    ]);
  });

  test('empty queues array overrides defaults', () => {
    const stream = new Stream({} as any, 'test-group', {
      queues: [],
    });

    const keys = stream.getStreamKeys();
    expect(keys).toEqual([]);
  });

  test('multiple custom queues sorted by priority', () => {
    const low = new Queue('low-priority', { priority: 10 });
    const high = new Queue('high-priority', { priority: 1 });
    const mid = new Queue('mid-priority', { priority: 5 });

    const stream = new Stream({} as any, 'test-group', {
      queues: [low, high, mid],
    });

    const keys = stream.getStreamKeys();
    expect(keys).toEqual([
      'backstage:high-priority',
      'backstage:mid-priority',
      'backstage:low-priority',
    ]);
  });
});

describe('Queue Override — Worker', () => {
  beforeAll(async () => {
    // Clean slate
    const redis = new Bun.RedisClient(REDIS_URL);
    const keys = await redis.send('KEYS', ['backstage:*']);
    if (keys && Array.isArray(keys) && keys.length > 0) {
      await redis.send('DEL', keys as string[]);
    }
  });

  test('stream only subscribes to custom queues', () => {
    const worker = new Worker({
      queues: [new Queue('custom-only')],
    });

    const stream = (worker as any).stream;
    const keys = stream.getStreamKeys() as string[];
    expect(keys).toEqual(['backstage:custom-only']);
    expect(keys).not.toContain('backstage:urgent');
    expect(keys).not.toContain('backstage:default');
    expect(keys).not.toContain('backstage:low');
  });

  test('worker with no config queues uses defaults', () => {
    const worker = new Worker();
    const stream = (worker as any).stream;
    const keys = stream.getStreamKeys() as string[];
    expect(keys).toContain('backstage:urgent');
    expect(keys).toContain('backstage:default');
    expect(keys).toContain('backstage:low');
  });
});

describe('Queue Override — Integration', () => {
  let redis: RedisClient;
  let stream: Stream;

  beforeAll(async () => {
    redis = new Bun.RedisClient(REDIS_URL);

    const keys = await redis.send('KEYS', ['backstage:*']);
    if (keys && Array.isArray(keys) && keys.length > 0) {
      await redis.send('DEL', keys as string[]);
    }
  });

  afterAll(async () => {
    const keys = await redis.send('KEYS', ['backstage:*']);
    if (keys && Array.isArray(keys) && keys.length > 0) {
      await redis.send('DEL', keys as string[]);
    }
  });

  test('initialize only creates custom queue consumer groups', async () => {
    const customQueue = new Queue('integration-queue');
    stream = new Stream(redis, 'integration-group', {
      queues: [customQueue],
    });

    await stream.initialize();

    // Default queues should NOT exist
    for (const q of ['backstage:urgent', 'backstage:default', 'backstage:low']) {
      const exists = await redis.send('EXISTS', [q]);
      expect(exists).toBe(0);
    }

    // Custom queue should exist
    const exists = await redis.send('EXISTS', ['backstage:integration-queue']);
    expect(exists).toBe(1);
  });

  test('enqueue to custom-only stream works', async () => {
    const id = await stream.enqueue(
      'custom.test',
      { msg: 'hello' },
      { queue: 'integration-queue' },
    );
    expect(id).toBeTruthy();

    const len = await redis.send('XLEN', ['backstage:integration-queue']);
    expect(Number(len)).toBeGreaterThan(0);
  });

  test('scheduled tasks get enqueued to custom queue', async () => {
    const id = await stream.enqueue(
      'scheduled.test',
      { scheduled: true },
      { queue: 'integration-queue', delay: -1000 },
    );
    expect(id).toBeTruthy();
    expect(String(id)).toStartWith('scheduled:');

    const processed = await stream.processScheduledTasks();
    expect(processed).toBeGreaterThan(0);
  });
});
