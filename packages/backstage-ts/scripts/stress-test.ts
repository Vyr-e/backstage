/**
 * Backstage Heavy Stress Test
 *
 * Pushes the system to the limit to test throughput and memory stability.
 * Ideal for M1/M2/M3 Apple Silicon.
 *
 * Usage: bun run scripts/stress-test.ts
 */

import { Worker } from '../src/index';
import { LogLevel } from '../src/logger';

// Configuration
const CONFIG = {
  NUM_TASKS: 50_000, // Total tasks to process
  CONCURRENCY: 100, // Worker concurrency
  PAYLOAD_SIZE: 256, // Bytes per payload
  REPORT_INTERVAL: 1000, // ms
};

// Generate a payload string once
const PAYLOAD = {
  data: 'X'.repeat(CONFIG.PAYLOAD_SIZE),
  timestamp: Date.now(),
};

async function main() {
  console.clear();
  console.log('🔥 BACKSTAGE STRESS TEST 🔥');
  console.log('===========================');
  console.log(`tasks:       ${CONFIG.NUM_TASKS.toLocaleString()}`);
  console.log(`concurrency: ${CONFIG.CONCURRENCY}`);
  console.log(`payload:     ${CONFIG.PAYLOAD_SIZE} bytes`);
  console.log('===========================\n');

  const worker = new Worker(
    {
      consumerGroup: `stress-test-group-${Date.now()}`,
      concurrency: CONFIG.CONCURRENCY,
      workerId: `stress-worker-${process.pid}`,
    },
    { level: LogLevel.WARN },
  );

  // Clean up old test data from previous runs
  console.log('🧹 Cleaning up old test data...');
  const redis = worker.redis;
  await redis.send('DEL', ['backstage:urgent']);
  await redis.send('DEL', ['backstage:default']);
  await redis.send('DEL', ['backstage:low']);
  await redis.send('DEL', ['backstage:scheduled']);
  console.log('   Done!\n');

  let processed = 0;
  const startWait = new Promise<void>((resolve) => {
    // Register a fast handler
    worker.on('stress.test', async () => {
      processed++;
      if (processed === CONFIG.NUM_TASKS) {
        resolve();
      }
    });
  });

  // Initialize stream consumer groups BEFORE enqueueing
  // This ensures messages can be properly consumed
  await worker['stream'].initialize();

  // --- PHASE 1: ENQUEUE ---
  console.log('🚀 Phase 1: Enqueueing...');
  const enqueueStart = performance.now();

  // Use a separate connection logic for enqueuing if needed,
  // but worker.enqueue works fine.
  // We chunk promises to avoid blowing up the microtask queue too hard.
  const CHUNK_SIZE = 1000;
  for (let i = 0; i < CONFIG.NUM_TASKS; i += CHUNK_SIZE) {
    const chunkPromises = [];
    for (let j = 0; j < CHUNK_SIZE && i + j < CONFIG.NUM_TASKS; j++) {
      chunkPromises.push(worker.enqueue('stress.test', PAYLOAD));
    }
    await Promise.all(chunkPromises);

    // Tiny yield to allow GC/IO
    // if (i % 10000 === 0) process.stdout.write('.');
  }

  const enqueueEnd = performance.now();
  const enqueueTime = enqueueEnd - enqueueStart;
  const enqueueRate = CONFIG.NUM_TASKS / (enqueueTime / 1000);

  console.log(`\n✅ Enqueue Complete!`);
  console.log(`   Time: ${enqueueTime.toFixed(0)}ms`);
  console.log(
    `   Rate: ${enqueueRate.toLocaleString(undefined, { maximumFractionDigits: 0 })} ops/sec`,
  );
  console.log('-----------------------------------');

  // --- PHASE 2: PROCESSING ---
  console.log('\n⚙️  Phase 2: Processing...');

  // Debug: show which streams we're reading from
  const streamKeys = worker['stream'].getStreamKeys();
  console.log(`   Streams: ${streamKeys.join(', ')}`);
  console.log(`   Consumer group: ${worker['config'].consumerGroup}`);

  // Start the worker (don't await - it blocks until stop() is called)
  // The processLoop runs indefinitely, processing messages
  const processStart = performance.now();
  worker.start().catch((err) => console.error('Worker error:', err));

  // Monitoring Interval
  const statsInterval = setInterval(() => {
    const elapsed = (performance.now() - processStart) / 1000;
    const rate = processed / elapsed;
    const progress = (processed / CONFIG.NUM_TASKS) * 100;
    const mem = process.memoryUsage().rss / 1024 / 1024; // MB

    process.stdout.write(
      `\r[${progress.toFixed(1)}%] ` +
        `Processed: ${processed.toLocaleString()} | ` +
        `Rate: ${rate.toFixed(0)} ops/s | ` +
        `RAM: ${mem.toFixed(0)} MB`,
    );
  }, 100);

  // Wait for finish
  await startWait;
  clearInterval(statsInterval);

  const processEnd = performance.now();
  const processTime = processEnd - processStart;
  const processRate = CONFIG.NUM_TASKS / (processTime / 1000);

  console.log(`\n\n🏁 DONE!`);
  console.log(`   Time: ${processTime.toFixed(0)}ms`);
  console.log(
    `   Rate: ${processRate.toLocaleString(undefined, { maximumFractionDigits: 0 })} ops/sec`,
  );

  // Cleanup
  await worker.stop();
  process.exit(0);
}

main().catch(console.error);
