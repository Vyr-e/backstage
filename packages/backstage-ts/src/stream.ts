import { Priority, type RedisClient, type EnqueueOptions } from './types';
import { Queue } from './queue';
import { RedisStreamsProvider } from './provider/redis';
import { wireStreamKey } from './wire';

export interface StreamConfig {
  prefix?: string;
  defaultPriority?: Priority;
  /** Queues to subscribe to. If provided, these REPLACE the default priority queues. */
  queues?: Queue[];
}

/**
 * Redis Streams façade kept for back-compat.
 * New code should prefer RedisStreamsProvider / Worker({ provider }).
 */
export class Stream {
  private provider: RedisStreamsProvider;
  private defaultPriority: Priority;
  /** @internal exposed for existing tests */
  customQueues: Queue[];

  constructor(
    redis: RedisClient,
    consumerGroup: string,
    config: StreamConfig = {},
  ) {
    this.defaultPriority = config.defaultPriority ?? Priority.DEFAULT;
    this.customQueues = config.queues ? [...config.queues] : [];
    this.provider = new RedisStreamsProvider({
      redis,
      consumerGroup,
      prefix: config.prefix,
      defaultPriority: this.defaultPriority,
    });
  }

  async initialize(): Promise<void> {
    await this.provider.ensureQueues(this.getQueueNames());
  }

  async enqueue(
    taskName: string,
    payload: unknown,
    options: EnqueueOptions = {},
  ): Promise<string | null> {
    return this.provider.publish(taskName, payload, options);
  }

  async processScheduledTasks(): Promise<number> {
    return this.provider.promoteDueScheduled();
  }

  getStreamKeys(): string[] {
    return this.getQueueNames().map((q) => wireStreamKey(q));
  }

  getQueueNames(): string[] {
    if (this.customQueues.length > 0) {
      const sorted = [...this.customQueues].sort(
        (a, b) => a.priority - b.priority,
      );
      return sorted.map((q) => q.name);
    }
    return [Priority.URGENT, Priority.DEFAULT, Priority.LOW];
  }

  getPrefix(): string {
    return 'backstage';
  }

  getProvider(): RedisStreamsProvider {
    return this.provider;
  }

  async addQueue(queue: Queue): Promise<void> {
    if (this.customQueues.some((q) => q.name === queue.name)) {
      return;
    }
    await this.provider.ensureQueues([queue.name]);
    this.customQueues.push(queue);
  }
}
