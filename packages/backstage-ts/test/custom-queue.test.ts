import { describe, test, expect } from 'bun:test';
import { Worker } from '../src/worker';
import { Queue } from '../src/queue';

describe('Worker Custom Queues', () => {
  test('registers custom queue via config', () => {
    const customQueue = new Queue('explicit-queue');
    const worker = new Worker({
      queues: [customQueue],
    });

    // Access private property for testing
    const stream = (worker as any).stream;
    const queues = stream.customQueues as Queue[];

    expect(queues.length).toBe(1);
    expect(queues[0]!.name).toBe('explicit-queue');
  });

  test('registers custom queue via task registration', async () => {
    const worker = new Worker();

    // Register task with custom queue
    worker.on('custom.task', async () => {}, {
      queue: 'dynamic-queue',
    });

    // Allow async addQueue to complete
    await Bun.sleep(50);

    // Access private property for testing
    const stream = (worker as any).stream;
    const queues = stream.customQueues as Queue[];

    // Should have added the queue
    expect(queues.length).toBe(1);
    expect(queues[0]!.name).toBe('dynamic-queue');
    expect(queues[0]!.streamKey).toContain('dynamic-queue');
  });

  test('prevents duplicate queue registration', async () => {
    const worker = new Worker();

    // Register two tasks on same queue
    worker.on('task1', async () => {}, { queue: 'shared-queue' });
    worker.on('task2', async () => {}, { queue: 'shared-queue' });

    await Bun.sleep(50);

    const stream = (worker as any).stream;
    const queues = stream.customQueues as Queue[];

    // Should still only have 1 queue
    expect(queues.length).toBe(1);
    expect(queues[0]!.name).toBe('shared-queue');
  });

  test('mixes config and dynamic queues', async () => {
    const worker = new Worker({
      queues: [new Queue('config-queue')],
    });

    worker.on('dynamic.task', async () => {}, { queue: 'dynamic-queue' });

    await Bun.sleep(50);

    const stream = (worker as any).stream;
    const queues = stream.customQueues as Queue[];

    expect(queues.length).toBe(2);
    expect(queues.map((q) => q.name).sort()).toEqual([
      'config-queue',
      'dynamic-queue',
    ]);
  });
});
