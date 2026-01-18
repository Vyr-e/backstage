/**
 * Backstage SDK - Reclaimer
 */

import { type StreamMessage, type RedisClient, parseFields } from './types';
import { Logger, createLogger, LogLevel, type LoggerConfig } from './logger';

/**
 * Responsible for recovering idle messages from other consumers.
 * Uses XPENDING and XCLAIM to re-assign tasks that have timed out.
 */
export class Reclaimer {
  private redis: RedisClient;
  private consumerGroup: string;
  private consumerId: string;
  private idleTimeout: number;
  private maxDeliveries: number;
  private logger: Logger;

  /**
   * Create a new Reclaimer instance.
   *
   * @param redis - Redis client
   * @param consumerGroup - Consumer group name
   * @param consumerId - ID of this consumer (the one claiming the tasks)
   * @param idleTimeout - Milliseconds a task must be idle before it can be claimed
   * @param maxDeliveries - Max delivery attempts (currently unused in reclaimer logic but available)
   * @param loggerConfig - Logger configuration
   */
  constructor(
    redis: RedisClient,
    consumerGroup: string,
    consumerId: string,
    idleTimeout: number,
    maxDeliveries: number,
    loggerConfig?: LoggerConfig,
  ) {
    this.redis = redis;
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

  /**
   * Initialize the reclaimer logic.
   *
   * @param streamKeys - List of stream keys to monitor
   */
  async initialize(streamKeys: string[]): Promise<void> {
    this.logger.debug(
      `Initialized for ${streamKeys.length} streams, idle timeout: ${this.idleTimeout}ms`,
    );
  }

  /**
   * Check for and claim idle messages from a specific stream.
   *
   * @param streamKey - The stream to check
   * @returns Array of claimed messages that are now owned by this consumer
   */
  async reclaimIdleMessages(streamKey: string): Promise<StreamMessage[]> {
    const claimed: StreamMessage[] = [];

    try {
      const pending = await this.redis.send('XPENDING', [
        streamKey,
        this.consumerGroup,
        'IDLE',
        String(this.idleTimeout),
        '-',
        '+',
        '10',
      ]);

      if (!pending || !Array.isArray(pending) || pending.length === 0) {
        return claimed;
      }

      for (const entry of pending) {
        if (!Array.isArray(entry) || entry.length < 4) continue;

        const [messageId, , , deliveryCount] = entry as [
          string,
          string,
          number,
          number,
        ];

        try {
          const result = await this.redis.send('XCLAIM', [
            streamKey,
            this.consumerGroup,
            this.consumerId,
            String(this.idleTimeout),
            messageId,
          ]);

          if (result && Array.isArray(result) && result.length > 0) {
            const firstResult = result[0];
            if (Array.isArray(firstResult) && firstResult.length >= 2) {
              const [claimedId, fields] = firstResult as [string, unknown[]];

              if (fields && Array.isArray(fields)) {
                const message = this.parseMessage(
                  claimedId,
                  fields,
                  deliveryCount,
                );
                if (message) {
                  this.logger.debug(
                    `Claimed ${messageId} (delivery #${deliveryCount})`,
                  );
                  claimed.push(message);
                }
              }
            }
          }
        } catch {
          // Skip failed claims
        }
      }
    } catch {
      // Skip errors checking pending
    }

    return claimed;
  }

  private parseMessage(
    id: string,
    fields: unknown[],
    deliveryCount: number,
  ): StreamMessage | null {
    try {
      const data = parseFields(fields);

      return {
        id,
        taskName: data.taskName || '',
        payload: JSON.parse(data.payload || 'null'),
        deliveryCount,
        enqueuedAt: parseInt(data.enqueuedAt || '0', 10) || Date.now(),
      };
    } catch {
      return null;
    }
  }
}
