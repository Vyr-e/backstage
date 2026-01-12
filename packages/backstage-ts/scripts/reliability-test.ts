/**
 * Backstage Reliability Test
 *
 * This script tests the at-least-once delivery guarantee:
 * 1. Enqueue a task with a 10-second sleep
 * 2. Kill the worker 5 seconds in
 * 3. Restart the worker
 * 4. Verify the reclaimer recovers and completes the task
 *
 * Run: bun run scripts/reliability-test.ts
 */

import { Worker, Priority } from '../src/index';

const TASK_NAME = 'reliability.test';
const SLEEP_DURATION = 10000; // 10 seconds

async function main() {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║           Backstage Reliability Test                     ║');
  console.log('╠══════════════════════════════════════════════════════════╣');
  console.log('║ This test verifies at-least-once delivery.               ║');
  console.log('║                                                          ║');
  console.log('║ Steps:                                                   ║');
  console.log('║ 1. Start a worker with a 10-second sleep task            ║');
  console.log('║ 2. After 5 seconds, press Ctrl+C to kill the worker      ║');
  console.log('║ 3. Re-run this script                                    ║');
  console.log('║ 4. The reclaimer should recover and complete the task    ║');
  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log();

  const worker = new Worker({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    consumerGroup: 'reliability-test-group',
    reclaimerInterval: 10000, // Check every 10 seconds for faster demo
    idleTimeout: 15000, // Claim after 15 seconds idle
  });

  let taskStarted = false;
  let taskCompleted = false;

  worker.on(TASK_NAME, async (payload: { testId: string }) => {
    console.log(
      `\n[${new Date().toISOString()}] Task started: ${payload.testId}`
    );
    taskStarted = true;

    console.log(
      `[${new Date().toISOString()}] Sleeping for ${
        SLEEP_DURATION / 1000
      } seconds...`
    );
    console.log('[!] You can kill this process now to test recovery');
    console.log();

    // Progress indicator
    const startTime = Date.now();
    while (Date.now() - startTime < SLEEP_DURATION) {
      const elapsed = Math.floor((Date.now() - startTime) / 1000);
      const remaining = Math.ceil(
        (SLEEP_DURATION - (Date.now() - startTime)) / 1000
      );
      process.stdout.write(
        `\r[Progress] ${elapsed}s elapsed, ${remaining}s remaining...`
      );
      await Bun.sleep(1000);
    }

    console.log(
      `\n[${new Date().toISOString()}] Task completed: ${payload.testId}`
    );
    taskCompleted = true;

    // Exit successfully after task completes
    setTimeout(() => {
      console.log('\n✅ Reliability test PASSED!');
      process.exit(0);
    }, 1000);
  });

  // Enqueue a test task if running fresh
  const testId = `test-${Date.now()}`;
  console.log(
    `[${new Date().toISOString()}] Enqueueing task with ID: ${testId}`
  );

  // Start the worker (this will process any pending tasks too)
  console.log(`[${new Date().toISOString()}] Starting worker...`);
  console.log();

  // Enqueue after a short delay to let the worker start
  setTimeout(async () => {
    if (!taskStarted) {
      await worker.enqueue(TASK_NAME, { testId });
      console.log(`[${new Date().toISOString()}] Task enqueued`);
    }
  }, 1000);

  await worker.start();
}

main().catch((err) => {
  console.error('Test failed:', err);
  process.exit(1);
});
