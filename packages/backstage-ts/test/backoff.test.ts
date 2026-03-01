import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Reclaimer } from '../src/reclaimer';
import { Stream } from '../src/stream';
import { type RedisClient, Priority } from '../src/types';

let redis: RedisClient;
let stream: Stream;
let reclaimer: Reclaimer;
const STREAM_KEY = 'backstage:default';
const GROUP = 'backoff-test-group';

beforeAll(async () => {
  redis = new Bun.RedisClient('redis://localhost:6379');
  stream = new Stream(redis, GROUP);
  await stream.initialize();

  // Reclaimer with short idle timeout
  reclaimer = new Reclaimer(
    redis,
    GROUP,
    'recovery-worker',
    50, // 50ms idle timeout
    3,
  );
  await reclaimer.initialize([STREAM_KEY]);
});

afterAll(async () => {
  // Clean up
  const keys = await redis.send('KEYS', ['backstage:*']);
  if (keys && Array.isArray(keys) && keys.length > 0) {
    await redis.send('DEL', keys as string[]);
  }
});

describe('Reclaimer Backoff', () => {
  test('respects fixed backoff', async () => {
    // 1. Enqueue with fixed backoff of 500ms
    const msgId = await stream.enqueue(
      'backoff.test',
      { foo: 'bar' },
      {
        backoff: {
          type: 'fixed',
          delay: 500,
        },
      },
    );

    expect(msgId).toBeTruthy();

    // 2. Consume it with a "crashed" worker to put it in PEL
    await redis.send('XREADGROUP', [
      'GROUP',
      GROUP,
      'crashed-worker',
      'COUNT',
      '1',
      'STREAMS',
      STREAM_KEY,
      '>',
    ]);

    // 3. Wait 100ms - less than the 500ms backoff delay, so reclaimer should NOT claim yet
    await Bun.sleep(100);

    // DEBUG: Check what XPENDING returns
    const pending = await redis.send('XPENDING', [
      STREAM_KEY,
      GROUP,
      'IDLE',
      '50', // Same as reclaimer's idleTimeout
      '-',
      '+',
      '10',
    ]);
    console.log('DEBUG XPENDING result:', JSON.stringify(pending, null, 2));
    if (Array.isArray(pending) && pending.length > 0) {
      const entry = pending[0];
      console.log('DEBUG entry:', entry);
      if (Array.isArray(entry)) {
        console.log('DEBUG entry[0] (msgId):', entry[0]);
        console.log('DEBUG entry[1] (consumer):', entry[1]);
        console.log('DEBUG entry[2] (idleTime):', entry[2], typeof entry[2]);
        console.log(
          'DEBUG entry[3] (deliveryCount):',
          entry[3],
          typeof entry[3],
        );
      }
    }

    // Also check the message fields to verify backoff is stored
    const msgDetails = await redis.send('XRANGE', [
      STREAM_KEY,
      msgId!,
      msgId!,
      'COUNT',
      '1',
    ]);
    console.log('DEBUG message fields:', JSON.stringify(msgDetails, null, 2));

    // Should NOT claim yet
    const claimedEarly = await reclaimer.reclaimIdleMessages(STREAM_KEY);
    expect(claimedEarly.length).toBe(0);

    // 4. Wait another 500ms. Total idle ~600ms > 500ms.
    await Bun.sleep(500);

    // Should claim now
    const claimedLate = await reclaimer.reclaimIdleMessages(STREAM_KEY);
    expect(claimedLate.length).toBeGreaterThan(0);
    expect(claimedLate[0]!.id).toBe(msgId!);
  });
});
