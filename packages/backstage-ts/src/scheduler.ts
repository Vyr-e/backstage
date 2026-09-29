/**
 * Backstage Scheduler - runs cron tasks on schedule.
 */

import type { CronTask } from './cron';
import type { Queue } from './queue';
import { Logger, LogLevel, createLogger } from './logger';
import {
  resolveCapabilities,
  type BackstageProvider,
  type CapabilityOverrides,
  type ResolvedCapabilities,
} from './provider';
import { RedisStreamsProvider } from './provider/redis';

export interface SchedulerConfig {
  host?: string;
  port?: number;
  password?: string;
  db?: number;
  schedules?: CronTask[];
  queues?: Queue[];
  logLevel?: LogLevel;
  logFile?: string;
  /** Optional transport provider (default: Redis via host/port). */
  provider?: BackstageProvider;
  capabilities?: CapabilityOverrides;
}

/**
 * Scheduler for generating periodic tasks based on cron expressions.
 * Enqueues tasks through the jobs capability when their schedule is due.
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
  private schedules: CronTask[];
  private queues: Map<string, Queue>;
  private logger: Logger;
  private running = false;
  private provider: BackstageProvider;
  private resolved: ResolvedCapabilities;
  private providerReady: Promise<void>;
  private ownsProvider: boolean;

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

    if (config.provider) {
      this.provider = config.provider;
      this.ownsProvider = false;
    } else {
      this.provider = new RedisStreamsProvider({
        host: config.host,
        port: config.port,
        password: config.password,
        db: config.db,
      });
      this.ownsProvider = true;
    }

    this.resolved = resolveCapabilities(this.provider, config.capabilities);
    this.providerReady = (this.provider.init
      ? this.provider.init({
          capabilities: this.resolved,
          logger: this.logger,
        })
      : Promise.resolve()
    ).then(() => {
      this.resolved = resolveCapabilities(this.provider, config.capabilities);
    });
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

    await this.providerReady;

    this.logger.info(`Starting scheduler with ${this.schedules.length} tasks`);
    this.running = true;

    process.on('SIGTERM', () => this.stop());
    process.on('SIGINT', () => this.stop());

    let upcomingTasks: CronTask[] = [];

    while (this.running) {
      const now = new Date();

      for (const cronTask of upcomingTasks) {
        await this.enqueueTask(cronTask);
        cronTask.markRun(now);
      }

      const nextRuns = this.schedules.map((task) => ({
        task,
        next: task.getNextRun(now),
        delay: task.getNextRun(now).getTime() - now.getTime(),
      }));

      const minDelay = Math.min(...nextRuns.map((r) => r.delay));
      upcomingTasks = nextRuns
        .filter((r) => r.delay <= minDelay + 1000)
        .map((r) => r.task);

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
    if (this.ownsProvider) {
      void this.provider.close();
    }
  }

  private async enqueueTask(cronTask: CronTask): Promise<void> {
    const queue = cronTask.queue ?? this.getDefaultQueue();
    const queueName = queue?.name ?? 'default';

    // Preserve historical cron payload shape: { taskName, args }
    await this.resolved.jobs.publish({
      queue: queueName,
      taskName: cronTask.taskName,
      payload: {
        taskName: cronTask.taskName,
        args: cronTask.args,
      },
      enqueuedAt: Date.now(),
      meta: {},
    });

    this.logger.info(`Enqueued scheduled task: ${cronTask.taskName}`);
  }

  private getDefaultQueue(): Queue | undefined {
    const queues = Array.from(this.queues.values());
    if (queues.length === 0) return undefined;
    return queues.sort((a, b) => b.priority - a.priority)[0];
  }
}
