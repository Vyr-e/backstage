import { describe, test, expect } from 'bun:test';
import { Worker } from '../src/worker';
import { HardTimeout } from '../src/exceptions';

describe('Worker Timeouts', () => {
  test('enforces hard timeout', async () => {
    const worker = new Worker();
    let error: Error | undefined;

    // Mock logger to capture error
    const originalError = worker['logger'].error;
    worker['logger'].error = (msg, meta) => {
      if (meta?.error) {
        // It comes as a string in the logger
      }
    };

    const start = performance.now();

    worker.on(
      'slow.task',
      async () => {
        await Bun.sleep(500);
      },
      { hardTimeout: 50 },
    ); // 50ms timeout

    // Manually trigger executeTask (private method)
    const taskConfig = (worker as any).tasks.get('slow.task');
    const executeTask = (worker as any).executeTask.bind(worker);

    // Mock message
    const message = {
      id: '1-0',
      taskName: 'slow.task',
      payload: {},
      deliveryCount: 1,
      enqueuedAt: Date.now(),
    };
    const streamKey = 'backstage:default';

    // We need to spy on the logger to verify the error type
    let capturedErrorStr = '';
    (worker as any).logger.error = (msg: string, meta: any) => {
      capturedErrorStr = meta?.error || '';
    };
    (worker as any).ack = async () => {}; // Mock ack

    await executeTask(streamKey, message, taskConfig);

    const elapsed = performance.now() - start;

    // It should have finished near 50ms, not 500ms
    expect(elapsed).toBeLessThan(200);
    expect(capturedErrorStr).toContain('HardTimeout');
    expect(capturedErrorStr).toContain('exceeded 50ms');
  });
});
