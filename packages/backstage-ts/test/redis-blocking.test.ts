import { afterAll, describe, expect, test } from 'bun:test';
import { RedisStreamsProvider } from '../src/provider/redis';
import type { Subscription } from '../src/provider/types';

// Blocking reads (XREADGROUP ... BLOCK) must not share the provider's command
// connection: Redis serves one connection's commands in order, so every
// blocked read would hold up publishes, acks and every other subscription.
describe('RedisStreamsProvider blocking reads', () => {
  const prefix = `rbk-${Date.now()}`;
  const blockTimeout = 3000;
  const provider = new RedisStreamsProvider({
    host: 'localhost',
    port: 6379,
    prefix,
    blockTimeout,
  });
  const subs: Subscription[] = [];

  afterAll(async () => {
    await Promise.all(subs.map((s) => s.stop()));
    const keys = (await provider.redis.send('KEYS', [
      `${prefix}:*`,
    ])) as string[];
    if (keys.length > 0) await provider.redis.send('DEL', keys);
    await provider.close();
  });

  test(
    'subscriptions, consume and publish do not wait on each other',
    async () => {
      const topics = provider.topics!;
      const jobs = provider.jobs;

      await jobs.ensureQueues(['q']);
      subs.push(
        await jobs.consume(
          {
            queues: ['q'],
            group: 'g',
            consumerId: 'c',
            prefetch: 1,
            idleTimeout: 60_000,
          },
          async (d) => {
            await d.ack();
          },
        ),
      );

      const received: string[] = [];
      const names = ['a', 'b', 'c', 'd', 'e'].map((n) => `t.${n}`);
      const setupStart = performance.now();
      for (const topic of names) {
        subs.push(
          await topics.subscribe(
            { topic, group: 'grp', consumerId: 'c', from: 'latest' },
            async (m) => {
              received.push(m.topic);
              await m.ack();
            },
          ),
        );
      }
      const setupMs = performance.now() - setupStart;
      // On a shared connection each subscribe would wait behind the previous BLOCK.
      expect(setupMs).toBeLessThan(blockTimeout);

      await Bun.sleep(200); // let every loop enter its BLOCK

      const publishStart = performance.now();
      await topics.publish('t.e', { n: 1 });
      const publishMs = performance.now() - publishStart;
      expect(publishMs).toBeLessThan(blockTimeout / 2);

      const deliverStart = performance.now();
      while (
        !received.includes('t.e') &&
        performance.now() - deliverStart < 4 * blockTimeout
      ) {
        await Bun.sleep(20);
      }
      const deliverMs = performance.now() - deliverStart;
      expect(received).toContain('t.e');
      expect(deliverMs).toBeLessThan(blockTimeout / 2);

      // Stopping ends each pending BLOCK instead of waiting it out.
      const stopStart = performance.now();
      await Promise.all(subs.splice(0).map((s) => s.stop()));
      expect(performance.now() - stopStart).toBeLessThan(blockTimeout / 2);
    },
    { timeout: 60_000 },
  );
});
