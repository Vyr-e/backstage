/**
 * Backstage Scheduler - runs cron tasks on schedule.
 */

import type { CronTask } from './cron';
import type { Queue } from './queue';
import type { RedisClient } from './types';
import { Logger, LogLevel, createLogger } from './logger';
import { serialize } from './serializer';

export interface SchedulerConfig {
  host?: string;
  port?: number;
  password?: string;
  db?: number;
  schedules?: CronTask[];
  queues?: Queue[];
  logLevel?: LogLevel;
  logFile?: string;
}

/**
 * Scheduler for generating periodic tasks based on cron expressions.
 * Enqueues tasks to Redis Streams when their schedule is due.
 *
 * @example
 * ```typescript
 * const scheduler = new Scheduler({
 *   schedules: [
 *     CronTask.create('daily-report', '0 0 * * *', { emails: true })
 *   ]
 * });
 * await scheduler.start();
 * ```
 */
export class Scheduler {
  private config: SchedulerConfig;
  private redis: RedisClient;
  private schedules: CronTask[];
  private queues: Map<string, Queue>;
  private logger: Logger;
  private running = false;

  /**
   * Create a new Scheduler instance.
   *
   * @param config - Configuration options including schedules and connection details
   */
  constructor(config: SchedulerConfig = {}) {
    this.config = config;
    this.schedules = config.schedules ?? [];
    this.queues = new Map();

    for (const queue of config.queues ?? []) {
      this.queues.set(queue.name, queue);
    }

    this.logger = createLogger({
      level: config.logLevel ?? LogLevel.INFO,
      file: config.logFile,
      isScheduler: true,
    });

    const redisUrl = this.buildRedisUrl();
    this.redis = new Bun.RedisClient(redisUrl);
  }

  private buildRedisUrl(): string {
    const host = this.config.host ?? 'localhost';
    const port = this.config.port ?? 6379;
    const password = this.config.password;
    const db = this.config.db ?? 0;

    if (password) {
      return `redis://:${password}@${host}:${port}/${db}`;
    }
    return `redis://${host}:${port}/${db}`;
  }

  /**
   * Start the scheduler.
   * Begins checking for due tasks and enqueueing them.
   */
  async start(): Promise<void> {
    if (this.schedules.length === 0) {
      this.logger.error('No scheduled tasks configured');
      return;
    }

    this.logger.info(`Starting scheduler with ${this.schedules.length} tasks`);
    this.running = true;

    process.on('SIGTERM', () => this.stop());
    process.on('SIGINT', () => this.stop());

    let upcomingTasks: CronTask[] = [];

    while (this.running) {
      const now = new Date();

      // Enqueue tasks that are due
      for (const cronTask of upcomingTasks) {
        await this.enqueueTask(cronTask);
        cronTask.markRun(now);
      }

      // Calculate next run times
      const nextRuns = this.schedules.map((task) => ({
        task,
        next: task.getNextRun(now),
        delay: task.getNextRun(now).getTime() - now.getTime(),
      }));

      // Find the soonest task(s)
      const minDelay = Math.min(...nextRuns.map((r) => r.delay));
      upcomingTasks = nextRuns
        .filter((r) => r.delay <= minDelay + 1000)
        .map((r) => r.task);

      // Sleep until next task
      const sleepMs = Math.max(minDelay, 1000);
      this.logger.debug(
        `Sleeping ${Math.round(sleepMs / 1000)}s until next task`,
      );
      await Bun.sleep(sleepMs);
    }

    this.logger.info('Scheduler stopped');
  }

  /**
   * Stop the scheduler.
   * Gracefully shuts down the scheduling loop.
   */
  stop(): void {
    this.running = false;
  }

  private async enqueueTask(cronTask: CronTask): Promise<void> {
    const queue = cronTask.queue ?? this.getDefaultQueue();
    const streamKey = queue?.streamKey ?? 'backstage:default';

    const payload = serialize({
      taskName: cronTask.taskName,
      args: cronTask.args,
    });

    await this.redis.send('XADD', [
      streamKey,
      '*',
      'taskName',
      cronTask.taskName,
      'payload',
      payload,
      'enqueuedAt',
      String(Date.now()),
    ]);

    this.logger.info(`Enqueued scheduled task: ${cronTask.taskName}`);
  }

  private getDefaultQueue(): Queue | undefined {
    const queues = Array.from(this.queues.values());
    if (queues.length === 0) return undefined;
    return queues.sort((a, b) => b.priority - a.priority)[0];
  }
}
