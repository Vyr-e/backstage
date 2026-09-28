import type { StreamMessage, RedisClient } from './types';
import { Logger, createLogger, LogLevel, type LoggerConfig } from './logger';
import type { Worker } from './worker';
import type { BackstageProvider } from './provider/types';
import { RedisStreamsProvider } from './provider/redis';

export interface BroadcastConfig {
  worker?: Worker;
  provider?: BackstageProvider;
  redis?: RedisClient;
  workerId?: string;
  consumerIdleThreshold?: number;
  startPosition?: 'latest' | 'beginning';
  loggerConfig?: LoggerConfig;
}

/**
 * Thin façade over BackstageProvider broadcast methods.
 */
export class Broadcast {
  private provider: BackstageProvider;
  private workerId: string;
  private logger: Logger;
  private consumerIdleThreshold: number;
  private startPosition: 'latest' | 'beginning';

  constructor(config: BroadcastConfig) {
    if (config.worker) {
      this.provider = config.worker.provider;
      this.workerId = config.worker.workerId;
    } else if (config.provider && config.workerId) {
      this.provider = config.provider;
      this.workerId = config.workerId;
    } else if (config.redis && config.workerId) {
      this.provider = new RedisStreamsProvider({ redis: config.redis });
      this.workerId = config.workerId;
    } else {
      throw new Error(
        'Broadcast requires a worker, a provider+workerId, or redis+workerId',
      );
    }

    this.consumerIdleThreshold = config.consumerIdleThreshold ?? 60 * 60 * 1000;
    this.startPosition = config.startPosition ?? 'latest';
    this.logger = createLogger({
      level: LogLevel.INFO,
      ...config.loggerConfig,
    });
  }

  async initialize(): Promise<void> {
    await this.provider.ensureBroadcast(this.workerId, this.startPosition);
    this.logger.debug(`Broadcast ready for worker: ${this.workerId}`);
  }

  async send(taskName: string, payload: unknown): Promise<string> {
    return this.provider.broadcast(taskName, payload);
  }

  async read(blockMs: number = 0): Promise<StreamMessage[]> {
    const messages = await this.provider.consumeBroadcast({
      consumerIdentity: this.workerId,
      maxMessages: 10,
      blockMs,
    });
    return messages.map((m) => ({
      id: m.id,
      taskName: m.taskName,
      payload: m.payload,
      deliveryCount: m.deliveryCount,
      enqueuedAt: m.enqueuedAt,
    }));
  }

  async ack(messageId: string): Promise<void> {
    await this.provider.ackBroadcast(this.workerId, [messageId]);
  }

  async cleanup(): Promise<number> {
    if (!this.provider.cleanupBroadcastGhosts) return 0;
    const deleted = await this.provider.cleanupBroadcastGhosts(
      this.consumerIdleThreshold,
    );
    if (deleted > 0) {
      this.logger.info(`Deleted ${deleted} stale broadcast consumer groups`);
    }
    return deleted;
  }
}
