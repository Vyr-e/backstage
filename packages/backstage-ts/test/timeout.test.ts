import { describe, test, expect } from 'bun:test';
import { Worker } from '../src/worker';

describe('Worker Timeouts', () => {
  test('enforces hard timeout', async () => {
    const worker = new Worker();
    const start = performance.now();

    worker.on(
      'slow.task',
      async () => {
        await Bun.sleep(500);
      },
      { hardTimeout: 50 },
    );

    const taskConfig = (worker as any).tasks.get('slow.task');
    const executeTask = (worker as any).executeTask.bind(worker);

    const message = {
      id: '1-0',
      queue: 'default',
      taskName: 'slow.task',
      payload: {},
      deliveryCount: 1,
      enqueuedAt: Date.now(),
    };

    let capturedErrorStr = '';
    (worker as any).logger.error = (_msg: string, meta: any) => {
      capturedErrorStr = meta?.error || '';
    };

    await executeTask(message, taskConfig);

    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(200);
    expect(capturedErrorStr).toContain('HardTimeout');
    expect(capturedErrorStr).toContain('exceeded 50ms');
  });
});
