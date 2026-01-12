/**
 * Backstage SDK - Broadcast
 */

import {
  BROADCAST_STREAM,
  type StreamMessage,
  type RedisClient,
  parseFields,
} from './types';
import { Logger, createLogger, LogLevel, type LoggerConfig } from './logger';
import type { Worker } from './worker';

export interface BroadcastConfig {
  /** Worker instance to extract redis and workerId from */
  worker?: Worker;
  /** Redis client (required if worker not provided) */
  redis?: RedisClient;
  /** Worker ID (required if worker not provided) */
  workerId?: string;
  /** Idle threshold for consumer cleanup in milliseconds (default: 1 hour) */
  consumerIdleThreshold?: number;
  /** Logger configuration */
  loggerConfig?: LoggerConfig;
}

export class Broadcast {
  private redis: RedisClient;
  private workerId: string;
  private consumerGroup: string;
  private logger: Logger;
  private consumerIdleThreshold: number;

  constructor(config: BroadcastConfig) {
    // Extract redis and workerId from worker or use provided values
    if (config.worker) {
      this.redis = config.worker.redis;
      this.workerId = config.worker.workerId;
    } else if (config.redis && config.workerId) {
      this.redis = config.redis;
      this.workerId = config.workerId;
    } else {
      throw new Error(
        'Broadcast requires either a worker instance or both redis and workerId'
      );
    }

    this.consumerGroup = `broadcast-${this.workerId}`;
    this.consumerIdleThreshold = config.consumerIdleThreshold ?? 60 * 60 * 1000; // 1 hour
    this.logger = createLogger({
      level: LogLevel.INFO,
      ...config.loggerConfig,
    });
  }

  async initialize(): Promise<void> {
    try {
      await this.redis.send('XGROUP', [
        'CREATE',
        BROADCAST_STREAM,
        this.consumerGroup,
        '0',
        'MKSTREAM',
      ]);
      this.logger.debug(`Created consumer group: ${this.consumerGroup}`);
    } catch (err: unknown) {
      if (err instanceof Error && !err.message.includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  async send(taskName: string, payload: unknown): Promise<string> {
    const messageId = await this.redis.send('XADD', [
      BROADCAST_STREAM,
      '*',
      'taskName',
      taskName,
      'payload',
      JSON.stringify(payload),
      'enqueuedAt',
      String(Date.now()),
    ]);

    return messageId as string;
  }

  async read(blockMs: number = 0): Promise<StreamMessage[]> {
    const messages: StreamMessage[] = [];

    try {
      const result = await this.redis.send('XREADGROUP', [
        'GROUP',
        this.consumerGroup,
        this.workerId,
        'COUNT',
        '10',
        'BLOCK',
        String(blockMs),
        'STREAMS',
        BROADCAST_STREAM,
        '>',
      ]);

      if (!result || !Array.isArray(result) || result.length === 0) {
        return messages;
      }

      for (const streamEntry of result) {
        if (!Array.isArray(streamEntry) || streamEntry.length < 2) continue;

        const [, streamMessages] = streamEntry as [string, unknown[]];
        if (!Array.isArray(streamMessages)) continue;

        for (const msgEntry of streamMessages) {
          if (!Array.isArray(msgEntry) || msgEntry.length < 2) continue;

          const [msgId, fields] = msgEntry as [string, unknown[]];
          const message = this.parseMessage(msgId, fields);
          if (message) {
            messages.push(message);
          }
        }
      }
    } catch {
      // Skip read errors
    }

    return messages;
  }

  async ack(messageId: string): Promise<void> {
    await this.redis.send('XACK', [
      BROADCAST_STREAM,
      this.consumerGroup,
      messageId,
    ]);
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
    } catch {
      return null;
    }
  }

  /**
   * Clean up ghost consumer groups.
   * Checks consumer idle time, not just consumer count (which includes ghosts).
   */
  async cleanup(): Promise<number> {
    let deleted = 0;

    try {
      const groups = await this.redis.send('XINFO', [
        'GROUPS',
        BROADCAST_STREAM,
      ]);
      if (!groups || !Array.isArray(groups)) return deleted;

      for (const group of groups) {
        const info = this.parseRedisInfo(group);
        const groupName = info.name as string;

        // Never delete our own group
        if (groupName === this.consumerGroup) continue;

        // Check if all consumers in this group are idle (ghosts)
        const shouldDelete = await this.isGroupIdle(groupName);
        if (shouldDelete) {
          await this.redis.send('XGROUP', [
            'DESTROY',
            BROADCAST_STREAM,
            groupName,
          ]);
          this.logger.info(`Deleted stale consumer group: ${groupName}`);
          deleted++;
        }
      }
    } catch {
      // Skip cleanup errors
    }

    return deleted;
  }

  /**
   * Check if all consumers in a group are idle (ghosts).
   */
  private async isGroupIdle(groupName: string): Promise<boolean> {
    try {
      const consumers = await this.redis.send('XINFO', [
        'CONSUMERS',
        BROADCAST_STREAM,
        groupName,
      ]);

      if (!consumers || !Array.isArray(consumers) || consumers.length === 0) {
        // No consumers = definitely stale
        return true;
      }

      for (const consumer of consumers) {
        const info = this.parseRedisInfo(consumer);
        const idle = (info.idle as number) ?? 0;

        // If any consumer is active (not idle beyond threshold), don't delete
        if (idle < this.consumerIdleThreshold) {
          return false;
        }
      }

      // All consumers are ghosts (idle beyond threshold)
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Parse Redis XINFO key-value array into object.
   */
  private parseRedisInfo(data: unknown): Record<string, unknown> {
    const info: Record<string, unknown> = {};
    if (Array.isArray(data)) {
      for (let i = 0; i < data.length; i += 2) {
        const key = data[i];
        const value = data[i + 1];
        if (typeof key === 'string') {
          info[key] = value;
        }
      }
    }
    return info;
  }
}
