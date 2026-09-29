/**
 * Backstage SDK - Stream Abstraction
 *
 * Thin compatibility wrapper over RedisStreamsProvider.
 * Types and behavior match the pre-provider Redis path.
 */

import {
  Priority,
  STREAM_PREFIX,
  type RedisClient,
  type EnqueueOptions,
} from './types';
import { Queue } from './queue';
import { RedisStreamsProvider } from './provider/redis';

export interface StreamConfig {
  prefix?: string;
  defaultPriority?: Priority;
  /** Queues to subscribe to. If provided, these REPLACE the default priority queues. */
  queues?: Queue[];
}

/**
 * Manages Redis Stream interactions for task enqueueing and processing.
 * Delegates transport to RedisStreamsProvider.
 */
export class Stream {
  private redis: RedisClient;
  private consumerGroup: string;
  private prefix: string;
  private defaultPriority: Priority;
  private customQueues: Queue[];
  private provider: RedisStreamsProvider;
  private ready: Promise<void>;

  constructor(
    redis: RedisClient,
    consumerGroup: string,
    config: StreamConfig = {},
  ) {
    this.redis = redis;
    this.consumerGroup = consumerGroup;
    this.prefix = config.prefix ?? STREAM_PREFIX;
    this.defaultPriority = config.defaultPriority ?? Priority.DEFAULT;
    this.customQueues = config.queues ? [...config.queues] : [];
    this.provider = new RedisStreamsProvider({
      redis,
      prefix: this.prefix,
    });
    this.ready = this.provider.init({
      capabilities: {
        jobs: this.provider.jobs,
        delays: this.provider.delays,
        dedupe: this.provider.dedupe,
        topics: this.provider.topics,
      },
      logger: {
        info() {},
        warn() {},
        error() {},
        debug() {},
      } as any,
    });
  }

  async initialize(): Promise<void> {
    await this.ready;
    if (this.customQueues.length > 0) {
      for (const queue of this.customQueues) {
        await this.createConsumerGroup(queue.streamKey);
      }
    } else {
      for (const priority of [Priority.URGENT, Priority.DEFAULT, Priority.LOW]) {
        await this.createConsumerGroup(`${this.prefix}:${priority}`);
      }
    }
  }

  private async createConsumerGroup(streamKey: string): Promise<void> {
    try {
      await this.redis.send('XGROUP', [
        'CREATE',
        streamKey,
        this.consumerGroup,
        '0',
        'MKSTREAM',
      ]);
    } catch (err: unknown) {
      if (err instanceof Error && !err.message.includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  async enqueue(
    taskName: string,
    payload: unknown,
    options: EnqueueOptions = {},
  ): Promise<string | null> {
    await this.ready;

    if (options.dedupe) {
      const claimed = await this.provider.dedupe.claim(
        options.dedupe.key,
        options.dedupe.ttl ?? 3_600_000,
      );
      if (!claimed) return null;
    }

    const queue =
      options.queue ?? options.priority ?? this.defaultPriority;
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
      return this.provider.delays.schedule(job, Date.now() + options.delay);
    }

    return this.provider.jobs.publish(job);
  }

  async processScheduledTasks(): Promise<number> {
    await this.ready;
    return this.provider.promoteCrossProvider();
  }

  getStreamKeys(): string[] {
    if (this.customQueues.length > 0) {
      const sorted = [...this.customQueues].sort(
        (a, b) => a.priority - b.priority,
      );
      return sorted.map((q) => q.streamKey);
    }
    return [
      `${this.prefix}:${Priority.URGENT}`,
      `${this.prefix}:${Priority.DEFAULT}`,
      `${this.prefix}:${Priority.LOW}`,
    ];
  }

  getPrefix(): string {
    return this.prefix;
  }

  /** Escape hatch: underlying RedisStreamsProvider. */
  getProvider(): RedisStreamsProvider {
    return this.provider;
  }

  async addQueue(queue: Queue): Promise<void> {
    if (this.customQueues.some((q) => q.name === queue.name)) {
      return;
    }
    await this.createConsumerGroup(queue.streamKey);
    this.customQueues.push(queue);
  }
}
