/**
 * Backstage SDK - Stream Abstraction
 */

import {
  Priority,
  getStreamKey,
  getScheduledKey,
  STREAM_PREFIX,
  type RedisClient,
  type StreamMessageData,
  type EnqueueOptions,
} from './types';

/**
 * Lua script for atomic scheduled task processing.
 * Prevents race conditions when multiple schedulers are running.
 *
 * KEYS[1]: Scheduled ZSET key
 * ARGV[1]: Current timestamp (cutoff)
 * ARGV[2]: Stream key prefix
 * ARGV[3]: Default priority name
 */
const PROCESS_SCHEDULED_LUA = `
local zsetKey = KEYS[1]
local cutoff = tonumber(ARGV[1])
local prefix = ARGV[2]
local defaultPriority = ARGV[3]

local tasks = redis.call('ZRANGEBYSCORE', zsetKey, '-inf', cutoff)
local processed = 0

for _, taskData in ipairs(tasks) do
    local ok, task = pcall(cjson.decode, taskData)
    if ok and task then
        local priority = task.priority or defaultPriority
        local streamKey = prefix .. ':' .. priority
        
        redis.call('XADD', streamKey, '*',
            'taskName', task.taskName or '',
            'payload', task.payload or '{}',
            'enqueuedAt', tostring(task.enqueuedAt or 0)
        )
        
        redis.call('ZREM', zsetKey, taskData)
        processed = processed + 1
    end
end

return processed
`;

export interface StreamConfig {
  prefix?: string;
  defaultPriority?: Priority;
}

export class Stream {
  private redis: RedisClient;
  private consumerGroup: string;
  private prefix: string;
  private defaultPriority: Priority;

  constructor(
    redis: RedisClient,
    consumerGroup: string,
    config: StreamConfig = {}
  ) {
    this.redis = redis;
    this.consumerGroup = consumerGroup;
    this.prefix = config.prefix ?? STREAM_PREFIX;
    this.defaultPriority = config.defaultPriority ?? Priority.DEFAULT;
  }

  async initialize(): Promise<void> {
    const priorities = [Priority.URGENT, Priority.DEFAULT, Priority.LOW];

    for (const priority of priorities) {
      const streamKey = `${this.prefix}:${priority}`;
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
  }

  async enqueue(
    taskName: string,
    payload: unknown,
    options: EnqueueOptions = {}
  ): Promise<string> {
    const priority = options.priority ?? this.defaultPriority;
    const delay = options.delay;

    const messageData: StreamMessageData = {
      taskName,
      payload: JSON.stringify(payload),
      enqueuedAt: Date.now(),
    };

    if (delay && delay > 0) {
      const executeAt = Date.now() + delay;
      const scheduledKey = `${this.prefix}:scheduled`;
      const data = JSON.stringify({ ...messageData, priority });

      await this.redis.send('ZADD', [scheduledKey, String(executeAt), data]);
      return `scheduled:${executeAt}`;
    }

    const streamKey = `${this.prefix}:${priority}`;
    const messageId = await this.redis.send('XADD', [
      streamKey,
      '*',
      'taskName',
      messageData.taskName,
      'payload',
      messageData.payload,
      'enqueuedAt',
      String(messageData.enqueuedAt),
    ]);

    return messageId as string;
  }

  /**
   * Process scheduled tasks atomically using Lua script.
   * Safe for multiple scheduler instances.
   */
  async processScheduledTasks(): Promise<number> {
    const scheduledKey = `${this.prefix}:scheduled`;
    const now = Date.now();

    const result = await this.redis.send('EVAL', [
      PROCESS_SCHEDULED_LUA,
      '1',
      scheduledKey,
      String(now),
      this.prefix,
      this.defaultPriority,
    ]);

    return (result as number) ?? 0;
  }

  getStreamKeys(): string[] {
    return [
      `${this.prefix}:${Priority.URGENT}`,
      `${this.prefix}:${Priority.DEFAULT}`,
      `${this.prefix}:${Priority.LOW}`,
    ];
  }

  getPrefix(): string {
    return this.prefix;
  }
}
