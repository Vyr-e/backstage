import { describe, test, expect } from 'bun:test';
import { Worker } from '../src/worker';
import { Queue } from '../src/queue';

describe('Worker Custom Queues', () => {
  test('registers custom queue via config', () => {
    const worker = new Worker({
      queues: [new Queue('explicit-queue')],
    });
    const names = (worker as any).getQueueNames() as string[];
    expect(names).toEqual(['explicit-queue']);
  });

  test('registers custom queue via task registration', () => {
    const worker = new Worker();
    worker.on('custom.task', async () => {}, { queue: 'dynamic-queue' });
    const names = (worker as any).getQueueNames() as string[];
    expect(names).toContain('dynamic-queue');
    expect(names).toContain('urgent');
    expect(names).toContain('default');
    expect(names).toContain('low');
  });

  test('prevents duplicate queue registration', () => {
    const worker = new Worker();
    worker.on('task1', async () => {}, { queue: 'shared-queue' });
    worker.on('task2', async () => {}, { queue: 'shared-queue' });
    const names = (worker as any).getQueueNames() as string[];
    expect(names.filter((n) => n === 'shared-queue').length).toBe(1);
  });

  test('mixes config and dynamic queues', () => {
    // Config queues replace defaults entirely
    const worker = new Worker({
      queues: [new Queue('config-queue')],
    });
    worker.on('dynamic.task', async () => {}, { queue: 'dynamic-queue' });
    const names = (worker as any).getQueueNames() as string[];
    // With config.queues set, only those are used (overrides defaults)
    expect(names).toEqual(['config-queue']);
  });
});
