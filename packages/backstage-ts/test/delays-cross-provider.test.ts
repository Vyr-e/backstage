import { afterAll, describe, expect, test } from 'bun:test';
import { RedisStreamsProvider } from '../src/provider/redis';
import type { BackstageProvider, OutgoingJob } from '../src/provider/types';
import { Worker } from '../src/worker';

// A non-Redis transport (e.g. Kafka) with Redis plugged in for delays: a due
// delayed job must be promoted onto the active transport, not left in Redis.
describe('delays plugged into a non-Redis transport', () => {
  const prefix = `xdelay-${Date.now()}`;
  const redis = new RedisStreamsProvider({ host: 'localhost', port: 6379, prefix });
  const published: OutgoingJob[] = [];
  const provider: BackstageProvider = {
    name: 'recording',
    jobs: {
      name: 'recording',
      requires: ['delays'],
      async ensureQueues() {},
      async publish(job) {
        published.push(job);
        return 'rec-1';
      },
      async consume() {
        return { async stop() {} };
      },
    },
    async close() {},
  };
  const worker = new Worker({
    provider,
    capabilities: { delays: redis.delays },
    workerId: 'xdelay',
  });

  afterAll(async () => {
    await worker.stop();
    const keys = (await redis.redis.send('KEYS', [`${prefix}:*`])) as string[];
    if (keys.length > 0) await redis.redis.send('DEL', keys);
    await redis.close();
  });

  test('due job is published to the active transport', async () => {
    worker.on('delayed:task', async () => {});
    await worker.schedule('delayed:task', { n: 1 }, 200);
    void worker.start();

    const t0 = Date.now();
    while (published.length === 0 && Date.now() - t0 < 5000) {
      await Bun.sleep(50);
    }
    expect(published.map((j) => j.taskName)).toEqual(['delayed:task']);
  }, 15_000);
});
