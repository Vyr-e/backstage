/**
 * Backstage SDK - Worker
 */

import { Stream } from './stream';
import { Reclaimer } from './reclaimer';
import { Logger, createLogger, LogLevel, type LoggerConfig } from './logger';
import {
  Priority,
  getDeadLetterKey,
  getDefaultWorkerId,
  parseFields,
  type RedisClient,
  type WorkerConfig,
  type TaskConfig,
  type TaskHandler,
  type StreamMessage,
  type WorkflowInstruction,
  type EnqueueOptions,
  DEFAULT_WORKER_CONFIG,
} from './types';

export class Worker {
  private config: Required<WorkerConfig>;
  private _redis: RedisClient;
  private stream: Stream;
  private reclaimer: Reclaimer;
  private logger: Logger;

  private tasks: Map<string, TaskConfig> = new Map();
  private running: boolean = false;
  private activeTasks: Set<Promise<void>> = new Set();
  private reclaimerInterval: Timer | null = null;
  private schedulerInterval: Timer | null = null;

  /** Redis client used by this worker */
  get redis(): RedisClient {
    return this._redis;
  }

  /** Unique identifier for this worker */
  get workerId(): string {
    return this.config.workerId;
  }

  constructor(config: WorkerConfig = {}, loggerConfig?: LoggerConfig) {
    const workerId = config.workerId || getDefaultWorkerId();
    this.config = {
      ...DEFAULT_WORKER_CONFIG,
      ...config,
      workerId,
    };

    const redisUrl = this.buildRedisUrl();
    this._redis = new Bun.RedisClient(redisUrl);

    this.stream = new Stream(this._redis, this.config.consumerGroup);
    this.reclaimer = new Reclaimer(
      this._redis,
      this.config.consumerGroup,
      this.config.workerId,
      this.config.idleTimeout,
      this.config.maxDeliveries
    );

    this.logger = createLogger({
      level: LogLevel.INFO,
      ...loggerConfig,
    });
  }

  private buildRedisUrl(): string {
    const { host, port, password, db } = this.config;
    if (password) {
      return `redis://:${password}@${host}:${port}/${db}`;
    }
    return `redis://${host}:${port}/${db}`;
  }

  on<T = unknown>(
    taskName: string,
    handler: TaskHandler<T>,
    options: Partial<TaskConfig<T>> = {}
  ): this {
    if (this.tasks.has(taskName)) {
      throw new Error(`Task '${taskName}' is already registered`);
    }

    this.tasks.set(taskName, {
      name: taskName,
      handler: handler as TaskHandler,
      priority: options.priority ?? Priority.DEFAULT,
      maxRetries: options.maxRetries ?? this.config.maxDeliveries,
      rateLimit: options.rateLimit,
    });

    return this;
  }

  async enqueue(
    taskName: string,
    payload: unknown,
    options: EnqueueOptions = {}
  ): Promise<string | null> {
    return this.stream.enqueue(taskName, payload, options);
  }

  async schedule(
    taskName: string,
    payload: unknown,
    delayMs: number,
    options: Omit<EnqueueOptions, 'delay'> = {}
  ): Promise<string | null> {
    return this.stream.enqueue(taskName, payload, {
      ...options,
      delay: delayMs,
    });
  }

  async start(): Promise<void> {
    if (this.running) {
      throw new Error('Worker is already running');
    }

    this.logger.info(`Starting worker: ${this.config.workerId}`);
    this.logger.info(`Consumer group: ${this.config.consumerGroup}`);
    this.logger.info(`Registered tasks: ${this.tasks.size}`);

    await this.stream.initialize();
    await this.reclaimer.initialize(this.stream.getStreamKeys());

    this.running = true;
    this.setupSignalHandlers();

    this.reclaimerInterval = setInterval(
      () => this.runReclaimer(),
      this.config.reclaimerInterval
    );

    this.schedulerInterval = setInterval(
      () => this.stream.processScheduledTasks(),
      1000
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

    if (this.activeTasks.size > 0) {
      this.logger.info(`Waiting for ${this.activeTasks.size} active tasks...`);

      const gracePeriodPromise = new Promise<void>((resolve) =>
        setTimeout(resolve, this.config.gracePeriod)
      );

      await Promise.race([Promise.all(this.activeTasks), gracePeriodPromise]);

      if (this.activeTasks.size > 0) {
        this.logger.warn(
          `Force exiting with ${this.activeTasks.size} unfinished tasks`
        );
      }
    }

    this.logger.info('Worker stopped');
  }

  private async processLoop(): Promise<void> {
    const streamKeys = this.stream.getStreamKeys();
    const streamIds = streamKeys.map(() => '>');

    while (this.running) {
      try {
        // Backpressure: wait if at concurrency limit
        if (this.activeTasks.size >= this.config.concurrency) {
          await Promise.race(this.activeTasks);
          continue;
        }

        // Calculate how many to fetch based on remaining capacity
        const available = this.config.concurrency - this.activeTasks.size;
        const count = Math.min(this.config.prefetch, available);

        const result = await this._redis.send('XREADGROUP', [
          'GROUP',
          this.config.consumerGroup,
          this.config.workerId,
          'COUNT',
          String(count),
          'BLOCK',
          String(this.config.blockTimeout),
          'STREAMS',
          ...streamKeys,
          ...streamIds,
        ]);

        if (!result || !Array.isArray(result) || result.length === 0) {
          continue;
        }

        for (const streamEntry of result) {
          if (!Array.isArray(streamEntry) || streamEntry.length < 2) continue;

          const [, messages] = streamEntry as [string, unknown[]];
          if (!Array.isArray(messages)) continue;

          for (const msgEntry of messages) {
            if (!Array.isArray(msgEntry) || msgEntry.length < 2) continue;

            const [msgId, fields] = msgEntry as [string, unknown[]];
            const message = this.parseMessage(msgId, fields);
            if (message) {
              // Fire and forget - don't await, allows concurrency
              this.handleMessage(streamEntry[0] as string, message);
            }
          }
        }
      } catch (err) {
        if (this.running) {
          this.logger.error('Error in process loop', { error: String(err) });
          await Bun.sleep(1000);
        }
      }
    }
  }

  private parseMessage(id: string, fields: unknown[]): StreamMessage | null {
    try {
      const data = parseFields(fields);

      return {
        id,
        taskName: data.taskName || '',
        payload: JSON.parse(data.payload || 'null'),
        deliveryCount: 1,
        enqueuedAt: parseInt(data.enqueuedAt || '0', 10) || Date.now(),
      };
    } catch (err) {
      this.logger.error('Error parsing message', { error: String(err) });
      return null;
    }
  }

  private async handleMessage(
    streamKey: string,
    message: StreamMessage
  ): Promise<void> {
    const task = this.tasks.get(message.taskName);

    if (!task) {
      this.logger.warn(`Unknown task: ${message.taskName}`);
      await this.ack(streamKey, message.id);
      return;
    }

    const taskPromise = this.executeTask(streamKey, message, task);
    this.activeTasks.add(taskPromise);
    taskPromise.finally(() => this.activeTasks.delete(taskPromise));
  }

  private async executeTask(
    streamKey: string,
    message: StreamMessage,
    task: TaskConfig
  ): Promise<void> {
    try {
      this.logger.debug(`Executing task: ${task.name}`);

      const result = await task.handler(message.payload);

      if (result && typeof result === 'object' && 'next' in result) {
        const instruction = result as WorkflowInstruction;

        if (instruction.delay && instruction.delay > 0) {
          await this.schedule(
            instruction.next,
            instruction.payload,
            instruction.delay
          );
        } else {
          await this.enqueue(instruction.next, instruction.payload);
        }

        this.logger.debug(
          `Chained to: ${instruction.next}` +
            (instruction.delay ? ` (delay: ${instruction.delay}ms)` : '')
        );
      }

      await this.ack(streamKey, message.id);
      this.logger.debug(`Completed: ${task.name}`);
    } catch (err) {
      this.logger.error(`Task failed: ${task.name}`, { error: String(err) });
    }
  }

  private async ack(streamKey: string, messageId: string): Promise<void> {
    await this._redis.send('XACK', [
      streamKey,
      this.config.consumerGroup,
      messageId,
    ]);
  }

  private async runReclaimer(): Promise<void> {
    try {
      const streamKeys = this.stream.getStreamKeys();

      for (const streamKey of streamKeys) {
        const claimed = await this.reclaimer.reclaimIdleMessages(streamKey);

        for (const message of claimed) {
          if (message.deliveryCount > this.config.maxDeliveries) {
            await this.moveToDeadLetter(streamKey, message);
          } else {
            await this.handleMessage(streamKey, message);
          }
        }
      }
    } catch (err) {
      this.logger.error('Reclaimer error', { error: String(err) });
    }
  }

  private async moveToDeadLetter(
    streamKey: string,
    message: StreamMessage
  ): Promise<void> {
    let priority = Priority.DEFAULT;
    if (streamKey.includes(Priority.URGENT)) {
      priority = Priority.URGENT;
    } else if (streamKey.includes(Priority.LOW)) {
      priority = Priority.LOW;
    }

    const deadLetterKey = getDeadLetterKey(priority);

    this.logger.warn(
      `Moving to dead-letter: ${message.taskName} (${message.id})`
    );

    await this._redis.send('XADD', [
      deadLetterKey,
      '*',
      'taskName',
      message.taskName,
      'payload',
      JSON.stringify(message.payload),
      'enqueuedAt',
      String(message.enqueuedAt),
      'originalId',
      message.id,
      'deliveryCount',
      String(message.deliveryCount),
      'deadLetteredAt',
      String(Date.now()),
    ]);

    await this.ack(streamKey, message.id);
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
