/**
 * Backstage SDK - Worker
 */

import { computeBackoff } from './compute-backoff';
import { HardTimeout } from './exceptions';
import { Logger, createLogger, LogLevel, type LoggerConfig } from './logger';
import {
  buildCapabilityReport,
  formatCapabilityReport,
  requireCapability,
  resolveCapabilities,
  type BackstageProvider,
  type CapabilityOverrides,
  type CapabilityReport,
  type JobDelivery,
  type ResolvedCapabilities,
  type Subscription,
} from './provider';
import { RedisStreamsProvider } from './provider/redis';
import { Queue } from './queue';
import { ScriptRegistry } from './script-registry';
import {
  Priority,
  getDefaultWorkerId,
  type RedisClient,
  type WorkerConfig,
  type TaskConfig,
  type TaskHandler,
  type WorkflowInstruction,
  type EnqueueOptions,
  DEFAULT_WORKER_CONFIG,
} from './types';

export interface WorkerOptions extends WorkerConfig {
  provider?: BackstageProvider;
  capabilities?: CapabilityOverrides;
}

type TopicHandler = (
  payload: unknown,
  msg: { id: string; topic: string; publishedAt: number },
) => Promise<void>;

interface PendingTopicSub {
  topic: string;
  handler: TopicHandler;
  group?: string;
  from: 'latest' | 'earliest';
}

/**
 * Main Worker — orchestration over a transport provider.
 * Default provider is Redis Streams (same wire format as before).
 */
export class Worker {
  private config: Required<WorkerConfig>;
  private logger: Logger;
  private provider: BackstageProvider;
  private resolved: ResolvedCapabilities;
  private overrides?: CapabilityOverrides;
  private redisProvider: RedisStreamsProvider | null;

  private tasks: Map<string, TaskConfig> = new Map();
  private running = false;
  private activeTasks: Set<Promise<void>> = new Set();
  private registeredQueues: Set<string> = new Set();
  private jobSubscription: Subscription | null = null;
  private topicSubscriptions: Subscription[] = [];
  private pendingTopicSubs: PendingTopicSub[] = [];
  private promoteTimer: Timer | null = null;
  private providerReady: Promise<void>;
  private stopResolve: (() => void) | null = null;
  private startDone: Promise<void> | null = null;

  get redis(): RedisClient {
    if (!this.redisProvider) {
      throw new Error(
        'worker.redis is only available when using RedisStreamsProvider',
      );
    }
    return this.redisProvider.redis;
  }

  get scripts(): ScriptRegistry {
    if (!this.redisProvider) {
      throw new Error(
        'worker.scripts is only available when using RedisStreamsProvider',
      );
    }
    return this.redisProvider.scripts;
  }

  get workerId(): string {
    return this.config.workerId;
  }

  constructor(config: WorkerOptions = {}, loggerConfig?: LoggerConfig) {
    const workerId = config.workerId || getDefaultWorkerId();
    const { provider: injected, capabilities: overrides, ...rest } = config;
    this.config = {
      ...DEFAULT_WORKER_CONFIG,
      ...rest,
      workerId,
    };
    this.overrides = overrides;
    this.logger = createLogger({
      level: LogLevel.INFO,
      ...loggerConfig,
    });

    if (injected) {
      this.provider = injected;
      this.redisProvider =
        injected instanceof RedisStreamsProvider ? injected : null;
    } else {
      const redisProvider = new RedisStreamsProvider({
        host: this.config.host,
        port: this.config.port,
        password: this.config.password,
        db: this.config.db,
        deleteOnAck: this.config.deleteOnAck,
        blockTimeout: this.config.blockTimeout,
        reclaimIntervalMs: this.config.reclaimerInterval,
        maxDeliveries: this.config.maxDeliveries,
      });
      this.provider = redisProvider;
      this.redisProvider = redisProvider;
    }

    this.resolved = resolveCapabilities(this.provider, this.overrides);

    if (this.config.queues) {
      for (const q of this.config.queues) {
        this.registeredQueues.add(q.name);
      }
    }

    this.providerReady = this.bootProvider();
  }

  private async bootProvider(): Promise<void> {
    if (this.provider.init) {
      await this.provider.init({
        capabilities: this.resolved,
        logger: this.logger,
      });
    }
  }

  capabilities(): CapabilityReport {
    return buildCapabilityReport(
      this.provider.name,
      this.provider,
      this.resolved,
      this.overrides,
    );
  }

  on<T = unknown>(
    taskName: string,
    handler: TaskHandler<T>,
    options: Partial<TaskConfig<T>> = {},
  ): this {
    if (this.tasks.has(taskName)) {
      throw new Error(`Task '${taskName}' is already registered`);
    }

    if (options.queue && !this.registeredQueues.has(options.queue)) {
      this.registeredQueues.add(options.queue);
    }

    this.tasks.set(taskName, {
      name: taskName,
      handler: handler as TaskHandler,
      priority: options.priority ?? Priority.DEFAULT,
      queue: options.queue,
      softTimeout: options.softTimeout,
      hardTimeout: options.hardTimeout,
      maxRetries: options.maxRetries ?? this.config.maxDeliveries,
      rateLimit: options.rateLimit,
    });

    return this;
  }

  async enqueue(
    taskName: string,
    payload: unknown,
    options: EnqueueOptions = {},
  ): Promise<string | null> {
    await this.providerReady;

    if (options.dedupe) {
      const dedupe = requireCapability(
        this.provider.name,
        this.resolved,
        'dedupe',
      );
      const claimed = await dedupe.claim(
        options.dedupe.key,
        options.dedupe.ttl ?? 3_600_000,
      );
      if (!claimed) return null;
    }

    const queue = options.queue ?? options.priority ?? Priority.DEFAULT;
    const job = {
      queue,
      taskName,
      payload,
      enqueuedAt: Date.now(),
      meta: {
        attempts: options.attempts,
        backoff: options.backoff,
        timeout: options.timeout,
      },
    };

    if (options.delay && options.delay > 0) {
      const delays = requireCapability(
        this.provider.name,
        this.resolved,
        'delays',
      );
      return delays.schedule(job, Date.now() + options.delay);
    }

    return this.resolved.jobs.publish(job);
  }

  async schedule(
    taskName: string,
    payload: unknown,
    delayMs: number,
    options: EnqueueOptions = {},
  ): Promise<string | null> {
    return this.enqueue(taskName, payload, { ...options, delay: delayMs });
  }

  async publish(topic: string, payload: unknown): Promise<string> {
    await this.providerReady;
    const topics = requireCapability(
      this.provider.name,
      this.resolved,
      'topics',
    );
    return topics.publish(topic, payload);
  }

  subscribe(
    topic: string,
    handler: TopicHandler,
    options: { group?: string; from?: 'latest' | 'earliest' } = {},
  ): this {
    const sub: PendingTopicSub = {
      topic,
      handler,
      group: options.group,
      from: options.from ?? 'latest',
    };
    this.pendingTopicSubs.push(sub);
    if (this.running) {
      this.startTopicSub(sub).catch((err) => {
        this.logger.error('Failed to start topic subscription', {
          error: String(err),
        });
      });
    }
    return this;
  }

  /**
   * Start the worker. Blocks until stop() is called (master behavior).
   * A second concurrent start() throws.
   */
  async start(): Promise<void> {
    if (this.running) {
      throw new Error('Worker is already running');
    }

    await this.providerReady;

    if (this.pendingTopicSubs.length > 0) {
      requireCapability(this.provider.name, this.resolved, 'topics');
    }
    for (const req of this.resolved.jobs.requires ?? []) {
      requireCapability(this.provider.name, this.resolved, req);
    }

    this.running = true;
    this.logger.info(`Worker starting: ${this.config.workerId}`);
    this.logger.info(`\n${formatCapabilityReport(this.capabilities())}`);

    const queues = this.getQueueNames();
    await this.resolved.jobs.ensureQueues(queues);

    this.jobSubscription = await this.resolved.jobs.consume(
      {
        queues,
        group: this.config.consumerGroup,
        consumerId: this.config.workerId,
        prefetch: Math.max(this.config.prefetch, this.config.concurrency),
        idleTimeout: this.config.idleTimeout,
      },
      (d) => this.onJobDelivery(d),
    );

    for (const sub of this.pendingTopicSubs) {
      await this.startTopicSub(sub);
    }

    // One promote loop in the worker only (not in producers / provider.schedule)
    if (this.resolved.delays && this.redisProvider) {
      this.promoteTimer = setInterval(() => {
        this.redisProvider!.promoteCrossProvider().catch(() => {});
      }, 1000);
    }

    this.setupSignalHandlers();
    this.logger.info('Worker started');

    this.startDone = new Promise<void>((resolve) => {
      this.stopResolve = resolve;
    });
    await this.startDone;
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.logger.info('Worker stopping...');
    this.running = false;

    if (this.promoteTimer) {
      clearInterval(this.promoteTimer);
      this.promoteTimer = null;
    }

    // Stop subscriptions without waiting on in-flight handlers indefinitely.
    if (this.jobSubscription) {
      await this.jobSubscription.stop();
      this.jobSubscription = null;
    }
    for (const s of this.topicSubscriptions) {
      await s.stop();
    }
    this.topicSubscriptions = [];

    if (this.activeTasks.size > 0) {
      this.logger.info(
        `Waiting for ${this.activeTasks.size} active tasks (grace: ${this.config.gracePeriod}ms)`,
      );
      await Promise.race([
        Promise.all(this.activeTasks),
        Bun.sleep(this.config.gracePeriod),
      ]);
      if (this.activeTasks.size > 0) {
        this.logger.warn(
          `Force exiting with ${this.activeTasks.size} unfinished tasks`,
        );
      }
    }

    await this.provider.close();
    this.logger.info('Worker stopped');

    const resolve = this.stopResolve;
    this.stopResolve = null;
    this.startDone = null;
    resolve?.();
  }

  private async startTopicSub(sub: PendingTopicSub): Promise<void> {
    const topics = requireCapability(
      this.provider.name,
      this.resolved,
      'topics',
    );
    const subscription = await topics.subscribe(
      {
        topic: sub.topic,
        group: sub.group,
        consumerId: this.config.workerId,
        from: sub.from,
      },
      async (m) => {
        await sub.handler(m.payload, {
          id: m.id,
          topic: m.topic,
          publishedAt: m.publishedAt,
        });
      },
    );
    this.topicSubscriptions.push(subscription);
  }

  /**
   * Queues this worker consumes.
   * Config queues replace the default priority set, but dynamic queues from
   * `on(..., { queue })` are always appended so those tasks are still consumed.
   */
  private getQueueNames(): string[] {
    if (this.config.queues && this.config.queues.length > 0) {
      const fromConfig = [...this.config.queues]
        .sort((a, b) => a.priority - b.priority)
        .map((q) => q.name);
      const configSet = new Set(fromConfig);
      const extras = [...this.registeredQueues].filter((n) => !configSet.has(n));
      return [...fromConfig, ...extras];
    }
    const names = [
      Priority.URGENT,
      Priority.DEFAULT,
      Priority.LOW,
      ...this.registeredQueues,
    ];
    return [...new Set(names)];
  }

  private async onJobDelivery(delivery: JobDelivery): Promise<void> {
    const task = this.tasks.get(delivery.taskName);
    if (!task) {
      this.logger.warn(`Unknown task: ${delivery.taskName}`);
      await delivery.ack();
      return;
    }

    const run = this.executeDelivery(delivery, task);
    this.activeTasks.add(run);
    try {
      await run;
    } finally {
      this.activeTasks.delete(run);
    }
  }

  private async executeDelivery(
    delivery: JobDelivery,
    task: TaskConfig,
  ): Promise<void> {
    try {
      const hardTimeout =
        delivery.meta.timeout && delivery.meta.timeout > 0
          ? delivery.meta.timeout
          : task.hardTimeout;

      let result: void | WorkflowInstruction;
      if (hardTimeout && hardTimeout > 0) {
        result = await Promise.race([
          task.handler(delivery.payload),
          new Promise<never>((_, reject) =>
            setTimeout(
              () =>
                reject(new HardTimeout(`Task exceeded ${hardTimeout}ms`)),
              hardTimeout,
            ),
          ),
        ]);
      } else {
        result = await task.handler(delivery.payload);
      }

      if (result && typeof result === 'object' && 'next' in result) {
        const instruction = result as WorkflowInstruction;
        try {
          if (instruction.delay && instruction.delay > 0) {
            await this.schedule(
              instruction.next,
              instruction.payload,
              instruction.delay,
            );
          } else {
            await this.enqueue(instruction.next, instruction.payload);
          }
        } catch (err) {
          this.logger.error('Chaining failed', { error: String(err) });
          throw err;
        }
      }

      await delivery.ack();
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      this.logger.error(`Task failed: ${task.name}`, { error });

      const max =
        delivery.meta.attempts ??
        task.maxRetries ??
        this.config.maxDeliveries;

      if (delivery.deliveryCount > max) {
        await delivery.deadLetter({ error });
        return;
      }

      const delayMs = delivery.meta.backoff
        ? computeBackoff(delivery.meta.backoff, delivery.deliveryCount)
        : this.config.idleTimeout;

      await delivery.retry({ delayMs, error });
    }
  }

  private setupSignalHandlers(): void {
    const handleSignal = async (signal: string) => {
      this.logger.info(`Received ${signal}`);
      await this.stop();
      process.exit(0);
    };
    process.on('SIGTERM', () => handleSignal('SIGTERM'));
    process.on('SIGINT', () => handleSignal('SIGINT'));
    process.on('SIGQUIT', () => handleSignal('SIGQUIT'));
  }
}

void Queue;
