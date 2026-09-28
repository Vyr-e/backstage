import type { StreamMessage, RedisClient } from './types';
import { Logger, createLogger, LogLevel, type LoggerConfig } from './logger';
import type { BackstageProvider } from './provider/types';
import { RedisStreamsProvider } from './provider/redis';
import { queueFromStreamKey } from './wire';

/**
 * Thin façade over BackstageProvider.reclaimIdle for back-compat.
 */
export class Reclaimer {
  private provider: BackstageProvider;
  private consumerGroup: string;
  private consumerId: string;
  private idleTimeout: number;
  private maxDeliveries: number;
  private logger: Logger;
  private queues: string[] = [];

  constructor(
    redisOrProvider: RedisClient | BackstageProvider,
    consumerGroup: string,
    consumerId: string,
    idleTimeout: number,
    maxDeliveries: number,
    loggerConfig?: LoggerConfig,
  ) {
    if (
      redisOrProvider &&
      typeof (redisOrProvider as BackstageProvider).reclaimIdle === 'function'
    ) {
      this.provider = redisOrProvider as BackstageProvider;
    } else {
      this.provider = new RedisStreamsProvider({
        redis: redisOrProvider as RedisClient,
        consumerGroup,
      });
    }
    this.consumerGroup = consumerGroup;
    this.consumerId = consumerId;
    this.idleTimeout = idleTimeout;
    this.maxDeliveries = maxDeliveries;
    this.logger = createLogger({
      level: LogLevel.INFO,
      isScheduler: false,
      ...loggerConfig,
    });
  }

  async initialize(streamKeys: string[]): Promise<void> {
    this.queues = streamKeys.map((k) => queueFromStreamKey(k));
    this.logger.debug(
      `Initialized for ${streamKeys.length} streams, idle timeout: ${this.idleTimeout}ms`,
    );
  }

  async reclaimIdleMessages(streamKey: string): Promise<StreamMessage[]> {
    const queue = queueFromStreamKey(streamKey);
    const claimed = await this.provider.reclaimIdle({
      queues: [queue],
      consumerGroup: this.consumerGroup,
      consumerId: this.consumerId,
      idleMs: this.idleTimeout,
      maxCount: 10,
    });

    return claimed.map((m) => ({
      id: m.id,
      taskName: m.taskName,
      payload: m.payload,
      deliveryCount: m.deliveryCount,
      enqueuedAt: m.enqueuedAt,
    }));
  }
}
