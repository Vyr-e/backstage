/**
 * Backstage SDK - Worker
 */

import { Stream } from './stream';
import { Queue } from './queue';
import { Reclaimer } from './reclaimer';
import { SoftTimeout, HardTimeout } from './exceptions';
import { Logger, createLogger, LogLevel, type LoggerConfig } from './logger';
import { ScriptRegistry } from './script-registry';
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

/**
 * Main Worker class that processes tasks from Redis Streams.
 *
 * @example
 * ```typescript
 * const worker = new Worker({
 *   host: 'localhost',
 *   consumerGroup: 'my-app'
 * });
 *
 * worker.on('send-email', async (job) => {
 *   await sendEmail(job.to, job.body);
 * });
 *
 * await worker.start();
 * ```
 */
export class Worker {
  private config: Required<WorkerConfig>;
  private _redis: RedisClient;
  private stream: Stream;
  private reclaimer: Reclaimer;
  private logger: Logger;

  /**
   * Registry for managing and running Lua scripts.
   */
  public scripts: ScriptRegistry;

  private tasks: Map<string, TaskConfig> = new Map();
  private running: boolean = false;
  private activeTasks: Set<Promise<void>> = new Set();
  private registeredQueues: Set<string> = new Set();
  private reclaimerInterval: Timer | null = null;
  private schedulerInterval: Timer | null = null;
  private ackFlushInterval: Timer | null = null;

  // Batched ACK queues: streamKey -> messageIds[]
  private pendingAcks: Map<string, string[]> = new Map();
  private readonly ACK_BATCH_SIZE = 100;
  private readonly ACK_FLUSH_INTERVAL = 50; // ms

  /**
   * Redis client used by this worker.
   */
  get redis(): RedisClient {
    return this._redis;
  }

  /**
   * Unique identifier for this worker instance.
   */
  get workerId(): string {
    return this.config.workerId;
  }

  /**
   * Create a new Worker instance.
   *
   * @param config Worker configuration options
   * @param loggerConfig Optional logger configuration
   */
  constructor(config: WorkerConfig = {}, loggerConfig?: LoggerConfig) {
    const workerId = config.workerId || getDefaultWorkerId();
    this.config = {
      ...DEFAULT_WORKER_CONFIG,
      ...config,
      workerId,
    };

    const redisUrl = this.buildRedisUrl();
    this._redis = new Bun.RedisClient(redisUrl);

    this.stream = new Stream(this._redis, this.config.consumerGroup, {
      queues: this.config.queues,
    });

    // Track explicitly configured queues
    if (this.config.queues) {
      for (const q of this.config.queues) {
        this.registeredQueues.add(q.name);
      }
    }

    this.reclaimer = new Reclaimer(
      this._redis,
      this.config.consumerGroup,
      this.config.workerId,
      this.config.idleTimeout,
      this.config.maxDeliveries,
    );
    this.scripts = new ScriptRegistry(this._redis);

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

  /**
   * Register a handler for a task.
   *
   * @param taskName - Unique name of the task
   * @param handler - Function to execute when task is received
   * @param options - Task-specific configuration (priority, retries, etc.)
   * @returns This worker instance for chaining
   *
   * @example
   * ```typescript
   * worker.on('resize-image', async (payload) => {
   *   await resize(payload.src, payload.width);
   * }, { priority: Priority.URGENT });
   * ```
   */
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
      this.stream.addQueue(new Queue(options.queue)).catch((err) => {
        this.logger.error(`Failed to register queue '${options.queue}'`, {
          error: String(err),
        });
      });
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

  /**
   * Enqueue a task for immediate processing.
   *
   * @param taskName - Name of the task to enqueue
   * @param payload - Data to pass to the task handler
   * @param options - Enqueue options (priority, dedupe, etc.)
   * @returns The Redis Stream message ID
   *
   * @example
   * ```typescript
   * await worker.enqueue('send-email', {
   *   to: 'user@example.com',
   *   subject: 'Welcome'
   * });
   * ```
   */
  async enqueue(
    taskName: string,
    payload: unknown,
    options: EnqueueOptions = {},
  ): Promise<string | null> {
    return this.stream.enqueue(taskName, payload, options);
  }

  /**
   * Schedule a task to run after a delay.
   *
   * @param taskName - Name of the task to schedule
   * @param payload - Data to pass to the task handler
   * @param delayMs - Delay in milliseconds
   * @param options - Enqueue options (excluding delay)
   * @returns The scheduled task ID
   *
   * @example
   * ```typescript
   * // Run execution in 1 hour
   * await worker.schedule('cleanup-logs', {}, 3600 * 1000);
   * ```
   */
  async schedule(
    taskName: string,
    payload: unknown,
    delayMs: number,
    options: Omit<EnqueueOptions, 'delay'> = {},
  ): Promise<string | null> {
    return this.stream.enqueue(taskName, payload, {
      ...options,
      delay: delayMs,
    });
  }

  /**
   * Start the worker.
   * Connects to Redis, initializes streams, and begins the processing loop.
   */
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
      this.config.reclaimerInterval,
    );

    this.schedulerInterval = setInterval(
      () => this.stream.processScheduledTasks(),
      1000,
    );

    // Start batched ACK flushing
    this.ackFlushInterval = setInterval(
      () => this.flushAcks(),
      this.ACK_FLUSH_INTERVAL,
    );

    await this.processLoop();
  }

  /**
   * Stop the worker gracefully.
   * Stops accepting new tasks and waits for active tasks to complete within the grace period.
   */
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

    // Flush any remaining ACKs
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

    this.logger.info('Worker stopped');
  }

  private async processLoop(): Promise<void> {
    const streamKeys = this.stream.getStreamKeys();

    // For high throughput, prefetch up to concurrency level
    const prefetch = Math.max(this.config.prefetch, this.config.concurrency);

    // Use '>' to read new (undelivered) messages
    // The consumer group's start ID (set to '0' at creation) determines
    // which messages are considered "new" - all messages from stream start
    const streamIds = streamKeys.map(() => '>');

    // Track if we got messages last iteration (for BLOCK vs NOBLOCK decision)
    let gotMessagesLastTime = false;

    while (this.running) {
      try {
        // Backpressure: wait if at concurrency limit
        if (this.activeTasks.size >= this.config.concurrency) {
          await Promise.race(this.activeTasks);
          continue;
        }

        // Calculate how many to fetch based on remaining capacity
        const available = this.config.concurrency - this.activeTasks.size;
        const count = Math.min(prefetch, available);

        // Build XREADGROUP command
        // Use NOBLOCK when actively processing for max throughput
        // Only BLOCK when idle (no messages last time) to save CPU
        const xreadArgs: string[] = [
          'GROUP',
          this.config.consumerGroup,
          this.config.workerId,
          'COUNT',
          String(count),
        ];

        if (!gotMessagesLastTime) {
          // Idle - block and wait for messages
          xreadArgs.push('BLOCK', String(this.config.blockTimeout));
        }
        // else: NOBLOCK - return immediately if no messages

        xreadArgs.push('STREAMS', ...streamKeys, ...streamIds);

        const result = await this._redis.send('XREADGROUP', xreadArgs);

        // Bun's Redis client returns results in object format: {streamKey: [[msgId, fields], ...]}
        // Standard Redis returns: [[streamKey, [[msgId, fields], ...]]]
        // Handle both formats for compatibility
        if (!result || typeof result !== 'object') {
          gotMessagesLastTime = false;
          continue;
        }

        gotMessagesLastTime = true;

        // Handle Bun's object format: {streamKey: messages[]}
        const entries = Array.isArray(result)
          ? (result as [string, unknown[]][]) // Standard format
          : (Object.entries(result) as [string, unknown[]][]); // Bun object format

        for (const [streamKey, messages] of entries) {
          if (!Array.isArray(messages) || messages.length === 0) continue;

          for (const msgEntry of messages) {
            if (!Array.isArray(msgEntry) || msgEntry.length < 2) continue;

            const [msgId, fields] = msgEntry as [string, unknown[]];
            const message = this.parseMessage(msgId, fields);
            if (message) {
              // Fire and forget - adds to activeTasks and starts execution
              this.handleMessage(streamKey, message);
            }
          }
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
    message: StreamMessage,
  ): Promise<void> {
    const task = this.tasks.get(message.taskName);

    if (!task) {
      this.logger.warn(`Unknown task: ${message.taskName}`);
      this.queueAck(streamKey, message.id);
      return;
    }

    const taskPromise = this.executeTask(streamKey, message, task);
    this.activeTasks.add(taskPromise);
    taskPromise.finally(() => this.activeTasks.delete(taskPromise));
  }

  private async executeTask(
    streamKey: string,
    message: StreamMessage,
    task: TaskConfig,
  ): Promise<void> {
    try {
      this.logger.debug(`Executing task: ${task.name}`);

      // Determine effective timeouts
      // 1. Message-specific timeout (highest priority)
      // 2. Task-specific hard timeout
      // 3. Queue-specific hard timeout (if applicable - would need lookup)
      // 4. No timeout

      // TODO: We could look up the queue's hardTimeout if available

      const hardTimeout = task.hardTimeout;

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

      // Queue ACK for batched processing (much faster than individual ACKs)
      this.queueAck(streamKey, message.id);

      this.logger.debug(`Completed: ${task.name}`);
    } catch (err) {
      this.logger.error(`Task failed: ${task.name}`, { error: String(err) });
    }
  }

  /**
   * Queue a message ACK for batched processing.
   * ACKs are flushed periodically or when batch size is reached.
   */
  private queueAck(streamKey: string, messageId: string): void {
    let pending = this.pendingAcks.get(streamKey);
    if (!pending) {
      pending = [];
      this.pendingAcks.set(streamKey, pending);
    }
    pending.push(messageId);

    // Flush immediately if batch size reached
    if (pending.length >= this.ACK_BATCH_SIZE) {
      this.flushStreamAcks(streamKey, pending).catch((err) => {
        this.logger.error('Failed to flush ACKs', { error: String(err) });
      });
      this.pendingAcks.set(streamKey, []);
    }
  }

  /**
   * Flush all pending ACKs across all streams.
   */
  private async flushAcks(): Promise<void> {
    const flushPromises: Promise<void>[] = [];

    for (const [streamKey, messageIds] of this.pendingAcks) {
      if (messageIds.length > 0) {
        flushPromises.push(this.flushStreamAcks(streamKey, messageIds));
        this.pendingAcks.set(streamKey, []);
      }
    }

    await Promise.all(flushPromises);
  }

  /**
   * Flush ACKs for a specific stream (batched XACK).
   */
  private async flushStreamAcks(
    streamKey: string,
    messageIds: string[],
  ): Promise<void> {
    if (messageIds.length === 0) return;

    // XACK supports multiple message IDs in a single call
    await this._redis.send('XACK', [
      streamKey,
      this.config.consumerGroup,
      ...messageIds,
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
    message: StreamMessage,
  ): Promise<void> {
    let priority = Priority.DEFAULT;
    if (streamKey.includes(Priority.URGENT)) {
      priority = Priority.URGENT;
    } else if (streamKey.includes(Priority.LOW)) {
      priority = Priority.LOW;
    }

    const deadLetterKey = getDeadLetterKey(priority);

    this.logger.warn(
      `Moving to dead-letter: ${message.taskName} (${message.id})`,
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

    this.queueAck(streamKey, message.id);
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
