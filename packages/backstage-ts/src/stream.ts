/**
 * Backstage SDK - Stream Abstraction
 */

import {
  Priority,
  STREAM_PREFIX,
  type RedisClient,
  type StreamMessageData,
  type EnqueueOptions,
} from './types';
import { Queue } from './queue';

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
        local streamKey = task.streamKey or (prefix .. ':' .. (task.priority or defaultPriority))
        
        local args = {streamKey, '*', 'taskName', task.taskName or '', 'payload', task.payload or '{}', 'enqueuedAt', tostring(task.enqueuedAt or 0)}
        
        if task.attempts then
            table.insert(args, 'attempts')
            table.insert(args, tostring(task.attempts))
        end
        if task.backoff then
            table.insert(args, 'backoff')
            table.insert(args, task.backoff)
        end
        if task.timeout then
            table.insert(args, 'timeout')
            table.insert(args, tostring(task.timeout))
        end
        
        redis.call('XADD', unpack(args))
        redis.call('ZREM', zsetKey, taskData)
        processed = processed + 1
    end
end

return processed
`;

export interface StreamConfig {
  prefix?: string;
  defaultPriority?: Priority;
  /** Custom queues to initialize (in addition to default priority queues) */
  queues?: Queue[];
}

export class Stream {
  private redis: RedisClient;
  private consumerGroup: string;
  private prefix: string;
  private defaultPriority: Priority;
  private customQueues: Queue[];

  constructor(
    redis: RedisClient,
    consumerGroup: string,
    config: StreamConfig = {}
  ) {
    this.redis = redis;
    this.consumerGroup = consumerGroup;
    this.prefix = config.prefix ?? STREAM_PREFIX;
    this.defaultPriority = config.defaultPriority ?? Priority.DEFAULT;
    this.customQueues = config.queues ?? [];
  }

  async initialize(): Promise<void> {
    // Initialize default priority queues
    const priorities = [Priority.URGENT, Priority.DEFAULT, Priority.LOW];

    for (const priority of priorities) {
      const streamKey = `${this.prefix}:${priority}`;
      await this.createConsumerGroup(streamKey);
    }

    // Initialize custom queues
    for (const queue of this.customQueues) {
      await this.createConsumerGroup(queue.streamKey);
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

  /**
   * Enqueue a task for processing.
   * @returns Message ID, or null if deduplicated
   */
  async enqueue(
    taskName: string,
    payload: unknown,
    options: EnqueueOptions = {}
  ): Promise<string | null> {
    // Handle deduplication
    if (options.dedupe) {
      const dedupeKey = `${this.prefix}:dedupe:${options.dedupe.key}`;
      const ttlSeconds = Math.ceil((options.dedupe.ttl ?? 3600000) / 1000);
      const set = await this.redis.send('SET', [
        dedupeKey,
        '1',
        'NX',
        'EX',
        String(ttlSeconds),
      ]);
      if (!set) {
        return null; // Duplicate, skip
      }
    }

    // Determine stream key
    let streamKey: string;
    if (options.queue) {
      // Custom queue name provided
      streamKey = `${this.prefix}:${options.queue}`;
    } else {
      // Use priority (default or specified)
      const priority = options.priority ?? this.defaultPriority;
      streamKey = `${this.prefix}:${priority}`;
    }

    const messageData: StreamMessageData = {
      taskName,
      payload: JSON.stringify(payload),
      enqueuedAt: Date.now(),
    };

    const delay = options.delay;

    if (delay && delay > 0) {
      const executeAt = Date.now() + delay;
      const scheduledKey = `${this.prefix}:scheduled`;
      const data = JSON.stringify({
        ...messageData,
        streamKey, // Store the target stream key
        priority: options.priority ?? this.defaultPriority,
        attempts: options.attempts,
        backoff: options.backoff ? JSON.stringify(options.backoff) : undefined,
        timeout: options.timeout,
      });

      await this.redis.send('ZADD', [scheduledKey, String(executeAt), data]);
      return `scheduled:${executeAt}`;
    }

    // Build XADD arguments with optional job metadata
    const xaddArgs: string[] = [
      streamKey,
      '*',
      'taskName',
      messageData.taskName,
      'payload',
      messageData.payload,
      'enqueuedAt',
      String(messageData.enqueuedAt),
    ];

    // Add optional job metadata
    if (options.attempts !== undefined) {
      xaddArgs.push('attempts', String(options.attempts));
    }
    if (options.backoff) {
      xaddArgs.push('backoff', JSON.stringify(options.backoff));
    }
    if (options.timeout !== undefined) {
      xaddArgs.push('timeout', String(options.timeout));
    }

    const messageId = await this.redis.send('XADD', xaddArgs);
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

  /**
   * Get all stream keys in priority order.
   * Default queues first, then custom queues sorted by priority.
   */
  getStreamKeys(): string[] {
    const defaultKeys = [
      `${this.prefix}:${Priority.URGENT}`,
      `${this.prefix}:${Priority.DEFAULT}`,
      `${this.prefix}:${Priority.LOW}`,
    ];

    // Sort custom queues by priority (lower = higher priority)
    const sortedCustom = [...this.customQueues].sort(
      (a, b) => a.priority - b.priority
    );
    const customKeys = sortedCustom.map((q) => q.streamKey);

    return [...defaultKeys, ...customKeys];
  }

  getPrefix(): string {
    return this.prefix;
  }

  /**
   * Add a custom queue at runtime.
   */
  async addQueue(queue: Queue): Promise<void> {
    await this.createConsumerGroup(queue.streamKey);
    this.customQueues.push(queue);
  }
}
