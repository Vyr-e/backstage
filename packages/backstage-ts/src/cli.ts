/**
 * Backstage SDK - CLI
 *
 * Command-line interface for running workers and schedulers.
 */

import { parseArgs } from 'util';
import { hostname } from 'os';
import { Worker } from './worker';
import type { WorkerConfig } from './types';

const COMMANDS = {
  'run-workers': 'Start the worker pool',
  help: 'Show this help message',
} as const;

/**
 * Parse command line arguments and run the appropriate command.
 */
async function main() {
  const { positionals } = parseArgs({
    args: Bun.argv.slice(2),
    strict: false,
    allowPositionals: true,
  });

  const command = positionals[0] || 'help';

  switch (command) {
    case 'run-workers':
      await runWorkers();
      break;
    case 'help':
    default:
      showHelp();
      break;
  }
}

/**
 * Start the worker pool.
 */
async function runWorkers() {
  const config: WorkerConfig = {
    host: Bun.env.REDIS_HOST || 'localhost',
    port: parseInt(Bun.env.REDIS_PORT || '6379', 10),
    password: Bun.env.REDIS_PASSWORD,
    db: parseInt(Bun.env.REDIS_DB || '0', 10),
    consumerGroup: Bun.env.BACKSTAGE_CONSUMER_GROUP || 'backstage-workers',
    workerId: Bun.env.BACKSTAGE_WORKER_ID || `${hostname()}-${process.pid}`,
  };

  console.log('[Backstage CLI] Starting worker pool...');
  console.log(`[Backstage CLI] Redis: ${config.host}:${config.port}`);

  const worker = new Worker(config);

  // Example: Register a test task
  worker.on('test.ping', async (payload) => {
    console.log('[Test] Received ping:', payload);
    return {
      next: 'test.pong',
      payload: { ...(payload as object), pong: true },
    };
  });

  worker.on('test.pong', async (payload) => {
    console.log('[Test] Received pong:', payload);
  });

  try {
    await worker.start();
  } catch (err) {
    console.error('[Backstage CLI] Worker error:', err);
    process.exit(1);
  }
}

/**
 * Show help message.
 */
function showHelp() {
  console.log(`
Backstage CLI - Background Worker System

Usage: bun run cli.ts <command>

Commands:
${Object.entries(COMMANDS)
  .map(([cmd, desc]) => `  ${cmd.padEnd(15)} ${desc}`)
  .join('\n')}

Environment Variables:
  REDIS_HOST                Redis server host (default: localhost)
  REDIS_PORT                Redis server port (default: 6379)
  REDIS_PASSWORD            Redis password (optional)
  REDIS_DB                  Redis database number (default: 0)
  BACKSTAGE_CONSUMER_GROUP  Consumer group name (default: backstage-workers)
  BACKSTAGE_WORKER_ID       Unique worker ID (default: hostname-pid)
`);
}

// Run main
main().catch((err) => {
  console.error('[Backstage CLI] Fatal error:', err);
  process.exit(1);
});
