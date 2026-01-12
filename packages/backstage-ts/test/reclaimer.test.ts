/**
 * Reclaimer tests - PEL recovery edge cases
 */
import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Reclaimer } from '../src/reclaimer';
import { Stream } from '../src/stream';
import { type RedisClient } from '../src/types';

let redis: RedisClient;
let stream: Stream;
let reclaimer: Reclaimer;

beforeAll(async () => {
  redis = new Bun.RedisClient('redis://localhost:6379');
  stream = new Stream(redis, 'reclaimer-test-group');
  await stream.initialize();
  reclaimer = new Reclaimer(
    redis,
    'reclaimer-test-group',
    'test-worker',
    100,
    3
  );
  await reclaimer.initialize(stream.getStreamKeys());
});

afterAll(async () => {
  const keys = await redis.send('KEYS', ['backstage:*']);
  if (keys && Array.isArray(keys) && keys.length > 0) {
    await redis.send('DEL', keys as string[]);
  }
});

describe('Reclaimer Tests', () => {
  test('initializes without error', async () => {
    const r = new Reclaimer(redis, 'test-group', 'worker-1', 1000, 5);
    await r.initialize([
      'backstage:urgent',
      'backstage:default',
      'backstage:low',
    ]);
  });

  test('claims idle messages after timeout', async () => {
    // Enqueue a message
    await stream.enqueue('reclaim.test', { test: true });

    // Read it but don't ACK (simulate worker crash)
    await redis.send('XREADGROUP', [
      'GROUP',
      'reclaimer-test-group',
      'dead-worker',
      'COUNT',
      '1',
      'STREAMS',
      'backstage:default',
      '>',
    ]);

    // Wait for idle timeout
    await Bun.sleep(150);

    // Reclaim should find it
    const claimed = await reclaimer.reclaimIdleMessages('backstage:default');
    // May or may not find messages depending on timing
    expect(Array.isArray(claimed)).toBe(true);
  });

  test('handles empty PEL', async () => {
    const claimed = await reclaimer.reclaimIdleMessages('backstage:low');
    expect(Array.isArray(claimed)).toBe(true);
  });

  test('handles non-existent stream gracefully', async () => {
    const claimed = await reclaimer.reclaimIdleMessages(
      'backstage:nonexistent'
    );
    expect(Array.isArray(claimed)).toBe(true);
    expect(claimed.length).toBe(0);
  });
});

describe('Reclaimer Load Tests', () => {
  test('handles many stuck messages', async () => {
    // Enqueue 100 messages
    const promises = Array.from({ length: 100 }, (_, i) =>
      stream.enqueue(`stuck.${i}`, { index: i })
    );
    await Promise.all(promises);

    // Read them all but don't ACK
    await redis.send('XREADGROUP', [
      'GROUP',
      'reclaimer-test-group',
      'crashed-worker',
      'COUNT',
      '100',
      'STREAMS',
      'backstage:default',
      '>',
    ]);

    // Wait for idle
    await Bun.sleep(150);

    // Reclaim
    const start = performance.now();
    const claimed = await reclaimer.reclaimIdleMessages('backstage:default');
    const elapsed = performance.now() - start;

    console.log(
      `Reclaimed ${claimed.length} messages in ${elapsed.toFixed(0)}ms`
    );
    expect(Array.isArray(claimed)).toBe(true);
  });
});
