import { describe, test, expect, afterEach } from 'bun:test';
import { Worker } from '../src/worker';
import { RedisStreamsProvider } from '../src/provider/redis';

describe('Worker Timeouts', () => {
  let worker: Worker | null = null;

  afterEach(async () => {
    if (worker) {
      await worker.stop().catch(() => {});
      worker = null;
    }
  });

  test('enforces hard timeout from task config', async () => {
    const prefix = `timeout-${Date.now()}`;
    const provider = new RedisStreamsProvider({
      host: 'localhost',
      port: 6379,
      prefix,
      reclaimIntervalMs: 60_000,
      blockTimeout: 200,
    });
    worker = new Worker({
      provider,
      consumerGroup: `cg-${prefix}`,
      workerId: `w-${prefix}`,
      idleTimeout: 60_000,
      maxDeliveries: 5,
      concurrency: 2,
      prefetch: 2,
    });

    let captured = '';
    (worker as any).logger.error = (_msg: string, meta: any) => {
      captured = meta?.error || '';
    };

    worker.on(
      'slow.task',
      async () => {
        await Bun.sleep(500);
      },
      { hardTimeout: 50 },
    );

    await worker.start();
    const start = performance.now();
    await worker.enqueue('slow.task', {});

    const deadline = Date.now() + 3000;
    while (!captured && Date.now() < deadline) {
      await Bun.sleep(20);
    }

    const elapsed = performance.now() - start;
    expect(elapsed).toBeLessThan(400);
    expect(captured).toContain('exceeded 50ms');
  });

  test('honors per-message timeout over task hardTimeout', async () => {
    const prefix = `timeout-msg-${Date.now()}`;
    const provider = new RedisStreamsProvider({
      host: 'localhost',
      port: 6379,
      prefix,
      reclaimIntervalMs: 60_000,
      blockTimeout: 200,
    });
    worker = new Worker({
      provider,
      consumerGroup: `cg-${prefix}`,
      workerId: `w-${prefix}`,
      idleTimeout: 60_000,
      maxDeliveries: 5,
    });

    let captured = '';
    (worker as any).logger.error = (_msg: string, meta: any) => {
      captured = meta?.error || '';
    };

    worker.on(
      'slow.task',
      async () => {
        await Bun.sleep(500);
      },
      { hardTimeout: 5000 },
    );

    await worker.start();
    await worker.enqueue('slow.task', {}, { timeout: 40 });

    const deadline = Date.now() + 3000;
    while (!captured && Date.now() < deadline) {
      await Bun.sleep(20);
    }
    expect(captured).toContain('exceeded 40ms');
  });
});
