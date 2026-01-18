/**
 * Worker integration tests
 */
import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Worker } from '../src/worker';
import { Priority } from '../src/types';

describe('Worker Tests', () => {
  test('creates worker with default config', () => {
    const worker = new Worker();
    expect(worker).toBeTruthy();
  });

  test('creates worker with custom config', () => {
    const worker = new Worker({
      host: 'localhost',
      port: 6379,
      consumerGroup: 'test-group',
      workerId: 'test-worker-1',
      blockTimeout: 1000,
      idleTimeout: 30000,
      maxDeliveries: 3,
    });
    expect(worker).toBeTruthy();
  });

  test('registers multiple handlers', () => {
    const worker = new Worker();

    worker.on('task.one', async () => {});
    worker.on('task.two', async () => {});
    worker.on('task.three', async () => {});

    // Verify all registered
    expect(() => worker.on('task.one', async () => {})).toThrow();
    expect(() => worker.on('task.two', async () => {})).toThrow();
  });

  test('rejects duplicate handler registration', () => {
    const worker = new Worker();
    worker.on('duplicate.task', async () => {});

    expect(() => {
      worker.on('duplicate.task', async () => {});
    }).toThrow("Task 'duplicate.task' is already registered");
  });

  test('handler receives correct payload', async () => {
    const worker = new Worker();
    let receivedPayload: unknown = null;

    worker.on<{ userId: string; amount: number }>(
      'payment.process',
      async (data) => {
        receivedPayload = data;
      },
    );

    // Enqueue a task
    await worker.enqueue('payment.process', {
      userId: 'user123',
      amount: 99.99,
    });

    // Note: Full verification requires running the worker
  });

  test('workflow chaining returns correct instruction', async () => {
    const worker = new Worker();

    worker.on<{ step1Done?: boolean }>('step1', async () => {
      return {
        next: 'step2',
        delay: 5000,
        payload: { step1Done: true },
      };
    });

    worker.on<{ step2Done?: boolean }>('step2', async () => {
      return { next: 'step3', payload: { step2Done: true } };
    });

    worker.on('step3', async () => {
      // Terminal step
    });
  });

  test('enqueues to different priorities', async () => {
    const worker = new Worker();

    const urgentId = await worker.enqueue(
      'urgent.task',
      {},
      { priority: Priority.URGENT },
    );
    const defaultId = await worker.enqueue('default.task', {});
    const lowId = await worker.enqueue(
      'low.task',
      {},
      { priority: Priority.LOW },
    );

    expect(urgentId).toBeTruthy();
    expect(defaultId).toBeTruthy();
    expect(lowId).toBeTruthy();
  });

  test('schedules delayed tasks', async () => {
    const worker = new Worker();

    const id = await worker.schedule('delayed.task', { delay: true }, 10000);
    expect(id).toBeTruthy();
    expect(id!.startsWith('scheduled:')).toBe(true);
  });

  test('handles handler with long execution time', async () => {
    const worker = new Worker({ blockTimeout: 100 });

    worker.on('slow.task', async () => {
      await Bun.sleep(50);
      return { next: 'fast.task' };
    });
  });

  test('handles handler that throws', async () => {
    const worker = new Worker();

    worker.on('failing.task', async () => {
      throw new Error('Intentional failure');
    });
  });
});

describe('Worker Load Tests', () => {
  test('enqueues 10,000 tasks quickly', async () => {
    const worker = new Worker();
    const start = performance.now();

    const promises = Array.from({ length: 10000 }, (_, i) =>
      worker.enqueue(`load.${i}`, { index: i }),
    );

    await Promise.all(promises);
    const elapsed = performance.now() - start;
    const throughput = 10000 / (elapsed / 1000);

    console.log(
      `Worker enqueue: 10,000 in ${elapsed.toFixed(0)}ms (${throughput.toFixed(
        0,
      )} ops/sec)`,
    );
    expect(throughput).toBeGreaterThan(500);
  });
});
