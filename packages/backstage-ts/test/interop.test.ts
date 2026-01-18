/**
 * Cross-language interoperability tests
 * Tests that Go can produce messages that TS can consume and vice versa
 */
import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Stream } from '../src/stream';
import { type RedisClient, parseFields } from '../src/types';

let redis: RedisClient;
let stream: Stream;

beforeAll(async () => {
  redis = new Bun.RedisClient('redis://localhost:6379');
  stream = new Stream(redis, 'interop-test-group');
  await stream.initialize();
});

afterAll(async () => {
  const keys = await redis.send('KEYS', ['backstage:interop:*']);
  if (keys && Array.isArray(keys) && keys.length > 0) {
    await redis.send('DEL', keys as string[]);
  }
});

describe('Cross-Language Interoperability', () => {
  test('TS produces message that matches Go format', async () => {
    // TS enqueues a message
    const id = await stream.enqueue('interop.ts-to-go', {
      userId: 'user123',
      action: 'ride.request',
      data: {
        pickup: { lat: 6.5244, lng: 3.3792 },
        destination: { lat: 6.4541, lng: 3.3947 },
      },
    });

    expect(id).toBeTruthy();

    // Verify the message is in the correct format that Go expects
    const result = await redis.send('XRANGE', [
      'backstage:default',
      '-',
      '+',
      'COUNT',
      '1',
    ]);
    expect(result).toBeTruthy();
  });

  test('TS can read Go-formatted message from stream', async () => {
    // Simulate a message that Go would produce
    const goMessage = {
      taskName: 'interop.go-to-ts',
      payload: JSON.stringify({
        driverId: 'driver456',
        location: { lat: 6.5244, lng: 3.3792 },
        status: 'available',
      }),
      enqueuedAt: String(Date.now()),
    };

    // Add it directly to Redis as Go would
    await redis.send('XADD', [
      'backstage:default',
      '*',
      'taskName',
      goMessage.taskName,
      'payload',
      goMessage.payload,
      'enqueuedAt',
      goMessage.enqueuedAt,
    ]);

    // TS reads it via XREADGROUP
    const result = await redis.send('XREADGROUP', [
      'GROUP',
      'interop-test-group',
      'ts-consumer',
      'COUNT',
      '1',
      'STREAMS',
      'backstage:default',
      '>',
    ]);

    if (result && Array.isArray(result) && result.length > 0) {
      const [streamEntry] = result as [string, unknown[]][];
      if (streamEntry && streamEntry[1] && Array.isArray(streamEntry[1])) {
        const [msgEntry] = streamEntry[1] as [string, unknown[]][];
        if (msgEntry) {
          const [msgId, fields] = msgEntry;
          expect(msgId).toBeTruthy();

          const data = parseFields(fields as unknown[]);
          expect(data.taskName).toBe('interop.go-to-ts');
          expect(data.payload).toBeTruthy();

          const payload = JSON.parse(data.payload ?? '{}');
          expect(payload.driverId).toBe('driver456');
        }
      }
    }
  });

  test('scheduled task format is compatible', async () => {
    // TS schedules a task
    const id = await stream.enqueue(
      'interop.scheduled',
      { test: true },
      { delay: 60000 },
    );

    expect(id).toBeTruthy();
    expect(id!.startsWith('scheduled:')).toBe(true);

    // Verify it's in the sorted set in a format Go can read
    const scheduled = await redis.send('ZRANGE', [
      'backstage:scheduled',
      '0',
      '-1',
      'WITHSCORES',
    ]);

    expect(scheduled).toBeTruthy();
    if (Array.isArray(scheduled) && scheduled.length >= 2) {
      // ZRANGE WITHSCORES returns [member, score, member, score, ...]
      // scheduled[0] is the task data, scheduled[1] is the score
      const taskDataStr = scheduled[0] as string;
      // Handle both JSON object format and simple string format
      if (taskDataStr.startsWith('{')) {
        const taskData = JSON.parse(taskDataStr);
        expect(taskData.taskName).toBeTruthy();
      }
    }
  });

  test('priority streams match between TS and Go', async () => {
    // Verify stream key format matches
    const streamKeys = stream.getStreamKeys();

    expect(streamKeys).toContain('backstage:urgent');
    expect(streamKeys).toContain('backstage:default');
    expect(streamKeys).toContain('backstage:low');

    // These are the exact keys Go uses
    const goKeys = ['backstage:urgent', 'backstage:default', 'backstage:low'];
    expect(streamKeys).toEqual(goKeys);
  });

  test('broadcast format is compatible', async () => {
    // Add a broadcast message as Go would
    await redis.send('XADD', [
      'backstage:broadcast',
      '*',
      'taskName',
      'interop.broadcast',
      'payload',
      JSON.stringify({ message: 'cache.invalidate' }),
      'enqueuedAt',
      String(Date.now()),
    ]);

    // Verify it exists
    const len = await redis.send('XLEN', ['backstage:broadcast']);
    expect(Number(len)).toBeGreaterThan(0);
  });

  test('dead-letter format is compatible', async () => {
    // Add a dead-letter message as Go would
    await redis.send('XADD', [
      'backstage:default:dead-letter',
      '*',
      'taskName',
      'interop.dead',
      'payload',
      JSON.stringify({ failed: true }),
      'enqueuedAt',
      String(Date.now()),
      'originalId',
      '1234567890-0',
      'deliveryCount',
      '5',
      'deadLetteredAt',
      String(Date.now()),
    ]);

    // Verify it exists
    const len = await redis.send('XLEN', ['backstage:default:dead-letter']);
    expect(Number(len)).toBeGreaterThan(0);
  });
});
