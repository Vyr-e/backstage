import {
  Priority,
  type RedisClient,
  type BackoffConfig,
} from '../../types';
import {
  WIRE_PREFIX,
  WIRE_DEFAULT_CONSUMER_GROUP,
  WIRE_SCHEDULED_KEY,
  WIRE_BROADCAST_STREAM,
  WIRE_FIELD,
  wireStreamKey,
  wireDeadLetterKey,
  wireDedupeKey,
  wireBroadcastGroup,
} from '../../wire';
import type {
  BackstageProvider,
  MessageRef,
  PublishOptions,
  ProviderCapabilities,
  ConsumeArgs,
  ReclaimIdleArgs,
  DeadLetterMeta,
} from '../types';
import { PROCESS_SCHEDULED_LUA } from './lua';
import {
  parseStreamEntries,
  fieldsToMessageRef,
  parseRedisInfo,
  calculateBackoff,
} from './parse';

export interface RedisStreamsProviderConfig {
  redis?: RedisClient;
  url?: string;
  host?: string;
  port?: number;
  password?: string;
  db?: number;
  consumerGroup?: string;
  prefix?: string;
  defaultPriority?: Priority;
}

function buildRedisUrl(config: RedisStreamsProviderConfig): string {
  if (config.url) return config.url;
  const host = config.host ?? 'localhost';
  const port = config.port ?? 6379;
  const password = config.password ?? '';
  const db = config.db ?? 0;
  if (password) return `redis://:${password}@${host}:${port}/${db}`;
  return `redis://${host}:${port}/${db}`;
}

export class RedisStreamsProvider implements BackstageProvider {
  readonly name = 'redis-streams';
  readonly capabilities: ProviderCapabilities = {
    durable: true,
    broadcast: true,
    scheduling: true,
    retries: true,
    deduplication: true,
  };

  private redis: RedisClient;
  private ownsClient: boolean;
  private consumerGroup: string;
  private prefix: string;
  private defaultPriority: Priority;
  private ensuredQueues = new Set<string>();

  constructor(config: RedisStreamsProviderConfig = {}) {
    this.ownsClient = !config.redis;
    this.redis = config.redis ?? new Bun.RedisClient(buildRedisUrl(config));
    this.consumerGroup =
      config.consumerGroup ?? WIRE_DEFAULT_CONSUMER_GROUP;
    this.prefix = config.prefix ?? WIRE_PREFIX;
    this.defaultPriority = config.defaultPriority ?? Priority.DEFAULT;
  }

  getClient(): RedisClient {
    return this.redis;
  }

  async ensureQueues(queues: string[]): Promise<void> {
    for (const queue of queues) {
      if (this.ensuredQueues.has(queue)) continue;
      await this.createConsumerGroup(wireStreamKey(queue));
      this.ensuredQueues.add(queue);
    }
  }

  async close(): Promise<void> {
    if (this.ownsClient) {
      try {
        this.redis.close?.();
      } catch {
        /* ignore */
      }
    }
  }

  async publish(
    taskName: string,
    payload: unknown,
    opts: PublishOptions = {},
  ): Promise<string | null> {
    if (opts.dedupe) {
      const dedupeKey = wireDedupeKey(opts.dedupe.key);
      const ttlSeconds = Math.ceil((opts.dedupe.ttl ?? 3600000) / 1000);
      const set = await this.redis.send('SET', [
        dedupeKey,
        '1',
        'NX',
        'EX',
        String(ttlSeconds),
      ]);
      if (!set) return null;
    }

    const queue = opts.queue ?? opts.priority ?? this.defaultPriority;
    const streamKey = `${this.prefix}:${queue}`;
    const enqueuedAt = Date.now();
    const payloadStr = JSON.stringify(payload);

    const delay = opts.delay;
    if (delay && delay > 0) {
      const executeAt = Date.now() + delay;
      const data = JSON.stringify({
        taskName,
        payload: payloadStr,
        enqueuedAt,
        streamKey,
        priority: opts.priority ?? this.defaultPriority,
        attempts: opts.attempts,
        backoff: opts.backoff ? JSON.stringify(opts.backoff) : undefined,
        timeout: opts.timeout,
      });
      await this.redis.send('ZADD', [
        WIRE_SCHEDULED_KEY,
        String(executeAt),
        data,
      ]);
      return `scheduled:${executeAt}`;
    }

    const xaddArgs: string[] = [
      streamKey,
      '*',
      WIRE_FIELD.taskName,
      taskName,
      WIRE_FIELD.payload,
      payloadStr,
      WIRE_FIELD.enqueuedAt,
      String(enqueuedAt),
    ];
    if (opts.attempts !== undefined) {
      xaddArgs.push(WIRE_FIELD.attempts, String(opts.attempts));
    }
    if (opts.backoff) {
      xaddArgs.push(WIRE_FIELD.backoff, JSON.stringify(opts.backoff));
    }
    if (opts.timeout !== undefined) {
      xaddArgs.push(WIRE_FIELD.timeout, String(opts.timeout));
    }

    const messageId = await this.redis.send('XADD', xaddArgs);
    return messageId as string;
  }

  async consume(args: ConsumeArgs): Promise<MessageRef[]> {
    if (args.queues.length === 0) return [];
    const streamKeys = args.queues.map((q) => wireStreamKey(q));
    const streamIds = streamKeys.map(() => '>');

    const xreadArgs: string[] = [
      'GROUP',
      args.consumerGroup,
      args.consumerId,
      'COUNT',
      String(args.maxMessages),
    ];
    if (args.blockMs !== undefined && args.blockMs > 0) {
      xreadArgs.push('BLOCK', String(args.blockMs));
    }
    xreadArgs.push('STREAMS', ...streamKeys, ...streamIds);

    const result = await this.redis.send('XREADGROUP', xreadArgs);
    const messages: MessageRef[] = [];
    for (const entry of parseStreamEntries(result)) {
      for (const msg of entry.messages) {
        const ref = fieldsToMessageRef(msg.id, entry.streamKey, msg.fields, 1);
        if (ref) messages.push(ref);
      }
    }
    return messages;
  }

  async ack(messages: MessageRef[]): Promise<void> {
    if (messages.length === 0) return;
    const byQueue = groupByQueue(messages);
    for (const [queue, msgs] of byQueue) {
      await this.redis.send('XACK', [
        wireStreamKey(queue),
        this.consumerGroup,
        ...msgs.map((m) => m.id),
      ]);
    }
  }

  async ackAndForget(messages: MessageRef[]): Promise<void> {
    if (messages.length === 0) return;
    await this.ack(messages);
    const byQueue = groupByQueue(messages);
    for (const [queue, msgs] of byQueue) {
      await this.redis.send('XDEL', [
        wireStreamKey(queue),
        ...msgs.map((m) => m.id),
      ]);
    }
  }

  async reclaimIdle(args: ReclaimIdleArgs): Promise<MessageRef[]> {
    const claimed: MessageRef[] = [];
    const maxCount = args.maxCount ?? 10;

    for (const queue of args.queues) {
      const streamKey = wireStreamKey(queue);
      try {
        const pending = await this.redis.send('XPENDING', [
          streamKey,
          args.consumerGroup,
          'IDLE',
          String(args.idleMs),
          '-',
          '+',
          String(maxCount),
        ]);

        if (!pending || !Array.isArray(pending) || pending.length === 0) {
          continue;
        }

        for (const entry of pending) {
          if (!Array.isArray(entry) || entry.length < 4) continue;
          const [messageId, , idleTime, deliveryCount] = entry as [
            string,
            string,
            number,
            number,
          ];

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
          const peek = fieldsToMessageRef(
            messageId,
            streamKey,
            fields,
            deliveryCount,
          );
          if (!peek) continue;

          if (peek.backoff) {
            const requiredWait = calculateBackoff(
              peek.backoff,
              deliveryCount,
            );
            if (idleTime < requiredWait) continue;
          }

          try {
            const result = await this.redis.send('XCLAIM', [
              streamKey,
              args.consumerGroup,
              args.consumerId,
              String(args.idleMs),
              messageId,
            ]);
            if (result && Array.isArray(result) && result.length > 0) {
              const first = result[0];
              if (Array.isArray(first) && first.length >= 2) {
                const [claimedId, claimedFields] = first as [
                  string,
                  unknown[],
                ];
                const msg = fieldsToMessageRef(
                  claimedId,
                  streamKey,
                  claimedFields,
                  deliveryCount,
                );
                if (msg) claimed.push(msg);
              }
            }
          } catch {
            /* claim race */
          }
        }
      } catch {
        /* stream may not exist */
      }
    }
    return claimed;
  }

  async deadLetter(
    message: MessageRef,
    meta: DeadLetterMeta,
  ): Promise<void> {
    const deadLetterKey = wireDeadLetterKey(message.queue);
    await this.redis.send('XADD', [
      deadLetterKey,
      '*',
      WIRE_FIELD.taskName,
      message.taskName,
      WIRE_FIELD.payload,
      JSON.stringify(message.payload),
      WIRE_FIELD.enqueuedAt,
      String(message.enqueuedAt),
      WIRE_FIELD.originalId,
      meta.originalId,
      WIRE_FIELD.deliveryCount,
      String(meta.deliveryCount),
      WIRE_FIELD.deadLetteredAt,
      String(Date.now()),
      ...(meta.error ? ['error', meta.error] : []),
    ]);
    await this.ack([message]);
  }

  async promoteDueScheduled(nowMs?: number): Promise<number> {
    const now = nowMs ?? Date.now();
    const result = await this.redis.send('EVAL', [
      PROCESS_SCHEDULED_LUA,
      '1',
      WIRE_SCHEDULED_KEY,
      String(now),
      this.prefix,
      this.defaultPriority,
    ]);
    return (result as number) ?? 0;
  }

  async ensureBroadcast(
    consumerIdentity: string,
    start: 'latest' | 'beginning',
  ): Promise<void> {
    const group = wireBroadcastGroup(consumerIdentity);
    try {
      await this.redis.send('XGROUP', [
        'CREATE',
        WIRE_BROADCAST_STREAM,
        group,
        start === 'beginning' ? '0' : '$',
        'MKSTREAM',
      ]);
    } catch (err: unknown) {
      if (err instanceof Error && !err.message.includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  async broadcast(taskName: string, payload: unknown): Promise<string> {
    const messageId = await this.redis.send('XADD', [
      WIRE_BROADCAST_STREAM,
      '*',
      WIRE_FIELD.taskName,
      taskName,
      WIRE_FIELD.payload,
      JSON.stringify(payload),
      WIRE_FIELD.enqueuedAt,
      String(Date.now()),
    ]);
    return messageId as string;
  }

  async consumeBroadcast(args: {
    consumerIdentity: string;
    maxMessages: number;
    blockMs?: number;
  }): Promise<MessageRef[]> {
    const group = wireBroadcastGroup(args.consumerIdentity);
    const xreadArgs: string[] = [
      'GROUP',
      group,
      args.consumerIdentity,
      'COUNT',
      String(args.maxMessages),
      'BLOCK',
      String(args.blockMs ?? 0),
      'STREAMS',
      WIRE_BROADCAST_STREAM,
      '>',
    ];
    try {
      const result = await this.redis.send('XREADGROUP', xreadArgs);
      const messages: MessageRef[] = [];
      for (const entry of parseStreamEntries(result)) {
        for (const msg of entry.messages) {
          const ref = fieldsToMessageRef(
            msg.id,
            entry.streamKey,
            msg.fields,
            1,
          );
          if (ref) {
            ref.queue = 'broadcast';
            messages.push(ref);
          }
        }
      }
      return messages;
    } catch {
      return [];
    }
  }

  async ackBroadcast(
    consumerIdentity: string,
    ids: string[],
  ): Promise<void> {
    if (ids.length === 0) return;
    const group = wireBroadcastGroup(consumerIdentity);
    await this.redis.send('XACK', [
      WIRE_BROADCAST_STREAM,
      group,
      ...ids,
    ]);
  }

  async cleanupBroadcastGhosts(idleMs: number): Promise<number> {
    let deleted = 0;
    try {
      const groups = await this.redis.send('XINFO', [
        'GROUPS',
        WIRE_BROADCAST_STREAM,
      ]);
      if (!groups || !Array.isArray(groups)) return deleted;

      for (const group of groups) {
        const info = parseRedisInfo(group);
        const groupName = info.name as string;
        if (!groupName?.startsWith('broadcast-')) continue;

        const shouldDelete = await this.isBroadcastGroupIdle(
          groupName,
          idleMs,
        );
        if (shouldDelete) {
          await this.redis.send('XGROUP', [
            'DESTROY',
            WIRE_BROADCAST_STREAM,
            groupName,
          ]);
          deleted++;
        }
      }
    } catch {
      /* ignore */
    }
    return deleted;
  }

  private async isBroadcastGroupIdle(
    groupName: string,
    idleMs: number,
  ): Promise<boolean> {
    try {
      const consumers = await this.redis.send('XINFO', [
        'CONSUMERS',
        WIRE_BROADCAST_STREAM,
        groupName,
      ]);
      if (!consumers || !Array.isArray(consumers) || consumers.length === 0) {
        return true;
      }
      for (const consumer of consumers) {
        const info = parseRedisInfo(consumer);
        const idle = (info.idle as number) ?? 0;
        if (idle < idleMs) return false;
      }
      return true;
    } catch {
      return false;
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

  /** Override consumer group used for ack (matches Worker config). */
  setConsumerGroup(group: string): void {
    this.consumerGroup = group;
  }
}

function groupByQueue(messages: MessageRef[]): Map<string, MessageRef[]> {
  const map = new Map<string, MessageRef[]>();
  for (const m of messages) {
    let list = map.get(m.queue);
    if (!list) {
      list = [];
      map.set(m.queue, list);
    }
    list.push(m);
  }
  return map;
}
