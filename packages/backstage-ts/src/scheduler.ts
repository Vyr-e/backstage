import type { CronTask } from './cron';
import type { Queue } from './queue';
import type { RedisClient } from './types';
import { Logger, LogLevel, createLogger } from './logger';
import type { BackstageProvider } from './provider/types';
import { RedisStreamsProvider } from './provider/redis';

export interface SchedulerConfig {
  host?: string;
  port?: number;
  password?: string;
  db?: number;
  /** Optional; omit to default to Redis Streams from host/port. */
  provider?: BackstageProvider;
  schedules?: CronTask[];
  queues?: Queue[];
  logLevel?: LogLevel;
  logFile?: string;
}

export class Scheduler {
  private config: SchedulerConfig;
  private provider: BackstageProvider;
  private schedules: CronTask[];
  private queues: Map<string, Queue>;
  private logger: Logger;
  private running = false;

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
    } else {
      this.provider = new RedisStreamsProvider({
        host: config.host,
        port: config.port,
        password: config.password,
        db: config.db,
      });
    }
  }

  get redis(): RedisClient {
    if (this.provider instanceof RedisStreamsProvider) {
      return this.provider.getClient();
    }
    throw new Error(
      'scheduler.redis is only available when using RedisStreamsProvider',
    );
  }

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

  stop(): void {
    this.running = false;
  }

  private async enqueueTask(cronTask: CronTask): Promise<void> {
    const queue = cronTask.queue ?? this.getDefaultQueue();
    const queueName = queue?.name ?? 'default';

    // Wire format: payload is plain JSON.stringify of args object (Go interop).
    // Cron historically used serialize(); keep taskName + args shape via JSON.
    await this.provider.publish(
      cronTask.taskName,
      { taskName: cronTask.taskName, args: cronTask.args },
      { queue: queueName },
    );

    this.logger.info(`Enqueued scheduled task: ${cronTask.taskName}`);
  }

  private getDefaultQueue(): Queue | undefined {
    const queues = Array.from(this.queues.values());
    if (queues.length === 0) return undefined;
    return queues.sort((a, b) => b.priority - a.priority)[0];
  }
}
