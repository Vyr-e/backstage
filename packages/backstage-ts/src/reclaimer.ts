/**
 * Backstage SDK - Reclaimer
 */

import {
  type StreamMessage,
  type RedisClient,
  parseFields,
  type BackoffConfig,
} from './types';
import { computeBackoff } from './compute-backoff';
import type { RedisStreamsProvider } from './provider/redis';
import { Logger, createLogger, LogLevel, type LoggerConfig } from './logger';

/**
 * Responsible for recovering idle messages from other consumers.
 * Uses XPENDING and XCLAIM to re-assign tasks that have timed out.
 */
export class Reclaimer {
  /**
   * Build a Reclaimer over a RedisStreamsProvider (compat wrapper).
   */
  static fromProvider(
    provider: RedisStreamsProvider,
    consumerGroup: string,
    consumerId: string,
    idleTimeout: number,
    maxDeliveries: number,
    loggerConfig?: LoggerConfig,
  ): Reclaimer {
    return new Reclaimer(
      provider.redis,
      consumerGroup,
      consumerId,
      idleTimeout,
      maxDeliveries,
      loggerConfig,
    );
  }

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
      // Get pending messages that are idle > idleTimeout (min threshold)
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

        const [messageId, consumer, idleTime, deliveryCount] = entry as [
          string,
          string,
          number,
          number,
        ];

        // Fetch message details to check backoff
        // We use XRANGE to peek without claiming
        const messageDetails = await this.redis.send('XRANGE', [
          streamKey,
          messageId,
          messageId,
          'COUNT',
          '1',
        ]);

        if (
          !messageDetails ||
          !Array.isArray(messageDetails) ||
          messageDetails.length === 0
        ) {
          continue;
        }

        const [, fields] = messageDetails[0] as [string, unknown[]];
        const data = parseFields(fields);

        // Parse backoff config
        let backoff: BackoffConfig | undefined;
        if (data.backoff) {
          try {
            backoff = JSON.parse(data.backoff);
          } catch {}
        }

        if (backoff) {
          const requiredWait = this.calculateBackoff(backoff, deliveryCount);
          if (idleTime < requiredWait) {
            // Not ready yet
            continue;
          }
        }

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
              const [claimedId, claimedFields] = firstResult as [
                string,
                unknown[],
              ];

              if (claimedFields && Array.isArray(claimedFields)) {
                const message = this.parseMessage(
                  claimedId,
                  claimedFields,
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
        } catch (err) {
          this.logger.debug(`Failed to claim ${messageId}`, {
            error: String(err),
          });
        }
      }
    } catch (err) {
      this.logger.error('Error checking pending messages', {
        error: String(err),
        streamKey,
      });
    }

    return claimed;
  }

  private calculateBackoff(config: BackoffConfig, attempts: number): number {
    return computeBackoff(config, attempts);
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
