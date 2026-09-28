import { HardTimeout } from './exceptions';
import { Logger, createLogger, LogLevel, type LoggerConfig } from './logger';
import { ScriptRegistry } from './script-registry';
import { Queue } from './queue';
import { Stream } from './stream';
import { Reclaimer } from './reclaimer';
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
import type { BackstageProvider, MessageRef } from './provider/types';
import { RedisStreamsProvider } from './provider/redis';

export interface WorkerOptions extends WorkerConfig {
  /**
   * Optional transport provider. Omit to use Redis Streams (default) from
   * host/port/password/db. Pass RabbitMQProvider or KafkaProvider to opt in
   * to another transport — existing Redis callers need no change.
   */
  provider?: BackstageProvider;
}

export class Worker {
  private config: Required<WorkerConfig>;
  private _provider: BackstageProvider;
  private _redis: RedisClient | null = null;
  /** @internal kept for queue-override tests / Stream façade */
  private stream: Stream | null = null;
  private reclaimer: Reclaimer;
  private logger: Logger;

  public scripts: ScriptRegistry | null = null;

  private tasks: Map<string, TaskConfig> = new Map();
  private running: boolean = false;
  private activeTasks: Set<Promise<void>> = new Set();
  private registeredQueues: Queue[] = [];
  private usingCustomQueues: boolean = false;
  private reclaimerInterval: Timer | null = null;
  private schedulerInterval: Timer | null = null;
  private ackFlushInterval: Timer | null = null;

  private pendingAcks: MessageRef[] = [];
  private readonly ACK_BATCH_SIZE = 100;
  private readonly ACK_FLUSH_INTERVAL = 50;

  get provider(): BackstageProvider {
    return this._provider;
  }

  get redis(): RedisClient {
    if (this._redis) return this._redis;
    if (this._provider instanceof RedisStreamsProvider) {
      return this._provider.getClient();
    }
    throw new Error(
      'worker.redis is only available when using RedisStreamsProvider',
    );
  }

  get workerId(): string {
    return this.config.workerId;
  }

  constructor(config: WorkerOptions = {}, loggerConfig?: LoggerConfig) {
    const workerId = config.workerId || getDefaultWorkerId();
    const { provider: injectedProvider, ...rest } = config;
    this.config = {
      ...DEFAULT_WORKER_CONFIG,
      ...rest,
      workerId,
    };

    if (injectedProvider) {
      this._provider = injectedProvider;
      if (injectedProvider instanceof RedisStreamsProvider) {
        injectedProvider.setConsumerGroup(this.config.consumerGroup);
        this._redis = injectedProvider.getClient();
        this.scripts = new ScriptRegistry(this._redis);
        this.stream = new Stream(this._redis, this.config.consumerGroup, {
          queues: this.config.queues,
        });
      }
    } else {
      const redisProvider = new RedisStreamsProvider({
        host: this.config.host,
        port: this.config.port,
        password: this.config.password,
        db: this.config.db,
        consumerGroup: this.config.consumerGroup,
      });
      this._provider = redisProvider;
      this._redis = redisProvider.getClient();
      this.scripts = new ScriptRegistry(this._redis);
      this.stream = new Stream(this._redis, this.config.consumerGroup, {
        queues: this.config.queues,
      });
    }

    if (this.config.queues && this.config.queues.length > 0) {
      this.usingCustomQueues = true;
      this.registeredQueues = [...this.config.queues];
    }

    this.reclaimer = new Reclaimer(
      this._provider,
      this.config.consumerGroup,
      this.config.workerId,
      this.config.idleTimeout,
      this.config.maxDeliveries,
    );

    this.logger = createLogger({
      level: LogLevel.INFO,
      ...loggerConfig,
    });
  }

  on<T = unknown>(
    taskName: string,
    handler: TaskHandler<T>,
    options: Partial<TaskConfig<T>> = {},
  ): this {
    if (this.tasks.has(taskName)) {
      throw new Error(`Task '${taskName}' is already registered`);
    }

    if (options.queue) {
      this.ensureQueueRegistered(options.queue);
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
    return this._provider.publish(taskName, payload, options);
  }

  async schedule(
    taskName: string,
    payload: unknown,
    delayMs: number,
    options: Omit<EnqueueOptions, 'delay'> = {},
  ): Promise<string | null> {
    return this._provider.publish(taskName, payload, {
      ...options,
      delay: delayMs,
    });
  }

  async start(): Promise<void> {
    if (this.running) {
      throw new Error('Worker is already running');
    }

    this.logger.info(`Starting worker: ${this.config.workerId}`);
    this.logger.info(`Provider: ${this._provider.name}`);
    this.logger.info(`Consumer group: ${this.config.consumerGroup}`);
    this.logger.info(`Registered tasks: ${this.tasks.size}`);

    const queues = this.getQueueNames();
    await this._provider.ensureQueues(queues);
    await this.reclaimer.initialize(queues.map((q) => `backstage:${q}`));

    this.running = true;
    this.setupSignalHandlers();

    this.reclaimerInterval = setInterval(
      () => this.runReclaimer(),
      this.config.reclaimerInterval,
    );

    this.schedulerInterval = setInterval(
      () => this._provider.promoteDueScheduled(),
      1000,
    );

    this.ackFlushInterval = setInterval(
      () => this.flushAcks(),
      this.ACK_FLUSH_INTERVAL,
    );

    await this.processLoop();
  }

  async stop(): Promise<void> {
    if (!this.running) return;

    this.logger.info('Stopping worker...');
    this.running = false;

    if (this.reclaimerInterval) {
      clearInterval(this.reclaimerInterval);
      this.reclaimerInterval = null;
    }
    if (this.schedulerInterval) {
      clearInterval(this.schedulerInterval);
      this.schedulerInterval = null;
    }
    if (this.ackFlushInterval) {
      clearInterval(this.ackFlushInterval);
      this.ackFlushInterval = null;
    }

    await this.flushAcks();

    if (this.activeTasks.size > 0) {
      this.logger.info(`Waiting for ${this.activeTasks.size} active tasks...`);

      const gracePeriodPromise = new Promise<void>((resolve) =>
        setTimeout(resolve, this.config.gracePeriod),
      );

      await Promise.race([Promise.all(this.activeTasks), gracePeriodPromise]);

      if (this.activeTasks.size > 0) {
        this.logger.warn(
          `Force exiting with ${this.activeTasks.size} unfinished tasks`,
        );
      }
    }

    await this._provider.close();
    this.logger.info('Worker stopped');
  }

  getQueueNames(): string[] {
    if (this.usingCustomQueues && this.registeredQueues.length > 0) {
      return [...this.registeredQueues]
        .sort((a, b) => a.priority - b.priority)
        .map((q) => q.name);
    }
    return [Priority.URGENT, Priority.DEFAULT, Priority.LOW];
  }

  private ensureQueueRegistered(queueName: string): void {
    if (this.registeredQueues.some((q) => q.name === queueName)) return;
    const queue = new Queue(queueName);
    this.registeredQueues.push(queue);
    this.usingCustomQueues = true;
    this.stream?.addQueue(queue).catch((err) => {
      this.logger.error(`Failed to register queue '${queueName}'`, {
        error: String(err),
      });
    });
    this._provider.ensureQueues([queueName]).catch((err) => {
      this.logger.error(`Failed to ensure queue '${queueName}'`, {
        error: String(err),
      });
    });
  }

  private async processLoop(): Promise<void> {
    const queues = this.getQueueNames();
    const prefetch = Math.max(this.config.prefetch, this.config.concurrency);
    let gotMessagesLastTime = false;

    while (this.running) {
      try {
        if (this.activeTasks.size >= this.config.concurrency) {
          await Promise.race(this.activeTasks);
          continue;
        }

        const available = this.config.concurrency - this.activeTasks.size;
        const count = Math.min(prefetch, available);

        const messages = await this._provider.consume({
          queues,
          consumerGroup: this.config.consumerGroup,
          consumerId: this.config.workerId,
          maxMessages: count,
          blockMs: gotMessagesLastTime ? undefined : this.config.blockTimeout,
        });

        if (messages.length === 0) {
          gotMessagesLastTime = false;
          continue;
        }

        gotMessagesLastTime = true;
        for (const message of messages) {
          this.handleMessage(message);
        }
      } catch (err) {
        if (this.running) {
          const errorMsg = err instanceof Error ? err.message : String(err);
          const stack = err instanceof Error ? err.stack : undefined;
          this.logger.error('Error in process loop', {
            error: errorMsg,
            stack,
          });
          await Bun.sleep(1000);
        }
      }
    }
  }

  private handleMessage(message: MessageRef): void {
    const task = this.tasks.get(message.taskName);

    if (!task) {
      this.logger.warn(`Unknown task: ${message.taskName}`);
      this.queueAck(message);
      return;
    }

    const taskPromise = this.executeTask(message, task);
    this.activeTasks.add(taskPromise);
    taskPromise.finally(() => this.activeTasks.delete(taskPromise));
  }

  private async executeTask(
    message: MessageRef,
    task: TaskConfig,
  ): Promise<void> {
    try {
      this.logger.debug(`Executing task: ${task.name}`);

      const hardTimeout = message.timeout ?? task.hardTimeout;
      let result: void | WorkflowInstruction;

      if (hardTimeout && hardTimeout > 0) {
        result = await Promise.race([
          task.handler(message.payload),
          new Promise<never>((_, reject) =>
            setTimeout(
              () => reject(new HardTimeout(`Task exceeded ${hardTimeout}ms`)),
              hardTimeout,
            ),
          ),
        ]);
      } else {
        result = await task.handler(message.payload);
      }

      if (result && typeof result === 'object' && 'next' in result) {
        const instruction = result as WorkflowInstruction;

        if (instruction.delay && instruction.delay > 0) {
          await this.schedule(
            instruction.next,
            instruction.payload,
            instruction.delay,
          );
        } else {
          await this.enqueue(instruction.next, instruction.payload);
        }

        this.logger.debug(
          `Chained to: ${instruction.next}` +
            (instruction.delay ? ` (delay: ${instruction.delay}ms)` : ''),
        );
      }

      this.queueAck(message);
      this.logger.debug(`Completed: ${task.name}`);
    } catch (err) {
      this.logger.error(`Task failed: ${task.name}`, { error: String(err) });
    }
  }

  private queueAck(message: MessageRef): void {
    this.pendingAcks.push(message);
    if (this.pendingAcks.length >= this.ACK_BATCH_SIZE) {
      const batch = this.pendingAcks;
      this.pendingAcks = [];
      this.flushAckBatch(batch).catch((err) => {
        this.logger.error('Failed to flush ACKs', { error: String(err) });
      });
    }
  }

  private async flushAcks(): Promise<void> {
    if (this.pendingAcks.length === 0) return;
    const batch = this.pendingAcks;
    this.pendingAcks = [];
    await this.flushAckBatch(batch);
  }

  private async flushAckBatch(messages: MessageRef[]): Promise<void> {
    if (messages.length === 0) return;
    if (this.config.deleteOnAck && this._provider.ackAndForget) {
      await this._provider.ackAndForget(messages);
    } else {
      await this._provider.ack(messages);
    }
  }

  private async runReclaimer(): Promise<void> {
    try {
      const queues = this.getQueueNames();
      const claimed = await this._provider.reclaimIdle({
        queues,
        consumerGroup: this.config.consumerGroup,
        consumerId: this.config.workerId,
        idleMs: this.config.idleTimeout,
      });

      for (const message of claimed) {
        if (message.deliveryCount > this.config.maxDeliveries) {
          await this._provider.deadLetter(message, {
            originalId: message.id,
            deliveryCount: message.deliveryCount,
          });
        } else {
          this.handleMessage(message);
        }
      }
    } catch (err) {
      this.logger.error('Reclaimer error', { error: String(err) });
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
