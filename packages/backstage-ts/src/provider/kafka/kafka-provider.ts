import {
  Kafka,
  type Consumer,
  type Producer,
  type KafkaConfig,
  type EachMessagePayload,
  logLevel,
} from 'kafkajs';
import type { BackoffConfig } from '../../types';
import type {
  BackstageProvider,
  MessageRef,
  PublishOptions,
  ProviderCapabilities,
  ConsumeArgs,
  ReclaimIdleArgs,
  DeadLetterMeta,
} from '../types';

export interface KafkaProviderConfig {
  brokers?: string[];
  clientId?: string;
  ssl?: boolean;
  sasl?: KafkaConfig['sasl'];
  prefix?: string;
  /** Consumer session timeout ms */
  sessionTimeout?: number;
}

interface PendingEntry {
  message: MessageRef;
  topic: string;
  partition: number;
  offset: string;
  claimedAt: number;
}

interface DelayedTask {
  id: string;
  executeAt: number;
  queue: string;
  taskName: string;
  payload: unknown;
  attempts?: number;
  backoff?: BackoffConfig;
  timeout?: number;
  enqueuedAt: number;
}

function calculateBackoff(config: BackoffConfig, deliveryCount: number): number {
  const retries = Math.max(0, deliveryCount - 1);
  if (config.type === 'fixed') return config.delay;
  if (config.type === 'exponential') {
    const delay = config.delay * Math.pow(2, retries - 1);
    return Math.min(delay, config.maxDelay ?? 3600000);
  }
  return 0;
}

/**
 * Kafka provider using topics per logical queue (`{prefix}.{queue}`),
 * consumer groups for work sharing, unique groups for broadcast fan-out,
 * and an in-provider delayed table for scheduling (Kafka has no native delay).
 */
export class KafkaProvider implements BackstageProvider {
  readonly name = 'kafka';
  readonly capabilities: ProviderCapabilities = {
    durable: true,
    broadcast: true,
    scheduling: true,
    retries: true,
    deduplication: true,
  };

  private prefix: string;
  private kafka: Kafka;
  private producer: Producer | null = null;
  private consumers = new Map<string, Consumer>();
  private connected = false;
  private pending = new Map<string, PendingEntry>();
  private dedupe = new Map<string, number>();
  private delayed: DelayedTask[] = [];
  private buffers = new Map<string, MessageRef[]>();
  private broadcastBuffers = new Map<string, MessageRef[]>();
  private ensuredTopics = new Set<string>();

  constructor(config: KafkaProviderConfig = {}) {
    this.prefix = config.prefix ?? 'backstage';
    this.kafka = new Kafka({
      clientId: config.clientId ?? 'backstage',
      brokers: config.brokers ?? ['localhost:9092'],
      ssl: config.ssl,
      sasl: config.sasl,
      logLevel: logLevel.ERROR,
    });
  }

  private topicFor(queue: string): string {
    return `${this.prefix}.${queue}`;
  }

  private dlqTopic(queue: string): string {
    return `${this.prefix}.${queue}.dead-letter`;
  }

  private broadcastTopic(): string {
    return `${this.prefix}.broadcast`;
  }

  private async getProducer(): Promise<Producer> {
    if (!this.producer) {
      this.producer = this.kafka.producer();
      await this.producer.connect();
      this.connected = true;
    }
    return this.producer;
  }

  private bufferKey(group: string, consumerId: string): string {
    return `${group}::${consumerId}`;
  }

  async ensureQueues(queues: string[]): Promise<void> {
    // Topics are auto-created on first produce/consume when broker allows it.
    for (const q of queues) {
      this.ensuredTopics.add(this.topicFor(q));
      this.ensuredTopics.add(this.dlqTopic(q));
    }
    await this.getProducer();
  }

  async close(): Promise<void> {
    for (const c of this.consumers.values()) {
      try {
        await c.disconnect();
      } catch {
        /* ignore */
      }
    }
    this.consumers.clear();
    if (this.producer) {
      try {
        await this.producer.disconnect();
      } catch {
        /* ignore */
      }
      this.producer = null;
    }
    this.connected = false;
    this.pending.clear();
    this.buffers.clear();
    this.broadcastBuffers.clear();
  }

  async publish(
    taskName: string,
    payload: unknown,
    opts: PublishOptions = {},
  ): Promise<string | null> {
    if (opts.dedupe) {
      const now = Date.now();
      this.gcDedupe(now);
      const expires = this.dedupe.get(opts.dedupe.key);
      if (expires && expires > now) return null;
      this.dedupe.set(opts.dedupe.key, now + (opts.dedupe.ttl ?? 3600000));
    }

    const queue = String(opts.queue ?? opts.priority ?? 'default');
    const enqueuedAt = Date.now();
    const id = `${enqueuedAt}-${Math.random().toString(36).slice(2, 10)}`;

    const delay = opts.delay;
    if (delay && delay > 0) {
      this.delayed.push({
        id,
        executeAt: enqueuedAt + delay,
        queue,
        taskName,
        payload,
        attempts: opts.attempts,
        backoff: opts.backoff,
        timeout: opts.timeout,
        enqueuedAt,
      });
      return `scheduled:${enqueuedAt + delay}`;
    }

    const producer = await this.getProducer();
    await producer.send({
      topic: this.topicFor(queue),
      messages: [
        {
          key: taskName,
          value: JSON.stringify({
            taskName,
            payload,
            enqueuedAt,
            attempts: opts.attempts,
            backoff: opts.backoff,
            timeout: opts.timeout,
          }),
          headers: {
            messageId: id,
            deliveryCount: '1',
          },
        },
      ],
    });
    return id;
  }

  async consume(args: ConsumeArgs): Promise<MessageRef[]> {
    const key = this.bufferKey(args.consumerGroup, args.consumerId);
    await this.ensureConsumer(args);

    const buf = this.buffers.get(key) ?? [];
    this.buffers.set(key, buf);

    if (buf.length === 0 && args.blockMs && args.blockMs > 0) {
      const deadline = Date.now() + args.blockMs;
      while (buf.length === 0 && Date.now() < deadline) {
        await Bun.sleep(25);
      }
    }

    const out = buf.splice(0, args.maxMessages);
    for (const m of out) {
      // pending already set in eachMessage handler
    }
    return out;
  }

  private async ensureConsumer(args: ConsumeArgs): Promise<void> {
    const key = this.bufferKey(args.consumerGroup, args.consumerId);
    if (this.consumers.has(key)) return;

    const consumer = this.kafka.consumer({
      groupId: args.consumerGroup,
      sessionTimeout: 30000,
    });
    await consumer.connect();
    const topics = args.queues.map((q) => this.topicFor(q));
    await consumer.subscribe({ topics, fromBeginning: false });

    const buf = this.buffers.get(key) ?? [];
    this.buffers.set(key, buf);

    await consumer.run({
      autoCommit: false,
      eachMessage: async (payload: EachMessagePayload) => {
        const ref = this.payloadToRef(payload);
        this.pending.set(ref.id, {
          message: ref,
          topic: payload.topic,
          partition: payload.partition,
          offset: payload.message.offset,
          claimedAt: Date.now(),
        });
        buf.push(ref);
      },
    });

    this.consumers.set(key, consumer);
  }

  async ack(messages: MessageRef[]): Promise<void> {
    // Group by consumer is implicit; commit offsets per partition
    const byTopicPartition = new Map<
      string,
      { topic: string; partition: number; offset: string }
    >();

    for (const m of messages) {
      const entry = this.pending.get(m.id);
      if (!entry) continue;
      const k = `${entry.topic}:${entry.partition}`;
      const existing = byTopicPartition.get(k);
      // Commit the highest offset+1
      if (
        !existing ||
        BigInt(entry.offset) >= BigInt(existing.offset)
      ) {
        byTopicPartition.set(k, {
          topic: entry.topic,
          partition: entry.partition,
          offset: (BigInt(entry.offset) + 1n).toString(),
        });
      }
      this.pending.delete(m.id);
    }

    // Commit via any consumer that shares the group — use first work consumer
    for (const consumer of this.consumers.values()) {
      try {
        await consumer.commitOffsets(
          [...byTopicPartition.values()].map((o) => ({
            topic: o.topic,
            partition: o.partition,
            offset: o.offset,
          })),
        );
        break;
      } catch {
        /* try next */
      }
    }
  }

  async ackAndForget(messages: MessageRef[]): Promise<void> {
    await this.ack(messages);
  }

  async reclaimIdle(args: ReclaimIdleArgs): Promise<MessageRef[]> {
    const claimed: MessageRef[] = [];
    const now = Date.now();
    const maxCount = args.maxCount ?? 10;

    for (const [, entry] of this.pending) {
      if (claimed.length >= maxCount) break;
      if (!args.queues.includes(entry.message.queue)) continue;

      const idle = now - entry.claimedAt;
      if (idle < args.idleMs) continue;

      if (entry.message.backoff) {
        const wait = calculateBackoff(
          entry.message.backoff,
          entry.message.deliveryCount,
        );
        if (idle < wait) continue;
      }

      entry.message.deliveryCount += 1;
      entry.claimedAt = now;
      claimed.push({ ...entry.message });
    }
    return claimed;
  }

  async deadLetter(
    message: MessageRef,
    meta: DeadLetterMeta,
  ): Promise<void> {
    const producer = await this.getProducer();
    await producer.send({
      topic: this.dlqTopic(message.queue),
      messages: [
        {
          key: message.taskName,
          value: JSON.stringify({
            taskName: message.taskName,
            payload: message.payload,
            enqueuedAt: message.enqueuedAt,
            originalId: meta.originalId,
            deliveryCount: meta.deliveryCount,
            deadLetteredAt: Date.now(),
            error: meta.error,
          }),
        },
      ],
    });
    await this.ack([message]);
  }

  async promoteDueScheduled(nowMs?: number): Promise<number> {
    const now = nowMs ?? Date.now();
    const due = this.delayed.filter((t) => t.executeAt <= now);
    this.delayed = this.delayed.filter((t) => t.executeAt > now);

    for (const task of due) {
      await this.publish(task.taskName, task.payload, {
        queue: task.queue,
        attempts: task.attempts,
        backoff: task.backoff,
        timeout: task.timeout,
      });
    }
    return due.length;
  }

  async ensureBroadcast(
    consumerIdentity: string,
    start: 'latest' | 'beginning',
  ): Promise<void> {
    const groupId = `broadcast-${consumerIdentity}`;
    if (this.consumers.has(groupId)) return;

    const consumer = this.kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({
      topic: this.broadcastTopic(),
      fromBeginning: start === 'beginning',
    });

    const buf: MessageRef[] = [];
    this.broadcastBuffers.set(consumerIdentity, buf);

    await consumer.run({
      autoCommit: false,
      eachMessage: async (payload: EachMessagePayload) => {
        const ref = this.payloadToRef(payload);
        ref.queue = 'broadcast';
        this.pending.set(ref.id, {
          message: ref,
          topic: payload.topic,
          partition: payload.partition,
          offset: payload.message.offset,
          claimedAt: Date.now(),
        });
        buf.push(ref);
      },
    });

    this.consumers.set(groupId, consumer);
  }

  async broadcast(taskName: string, payload: unknown): Promise<string> {
    const producer = await this.getProducer();
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    await producer.send({
      topic: this.broadcastTopic(),
      messages: [
        {
          key: taskName,
          value: JSON.stringify({
            taskName,
            payload,
            enqueuedAt: Date.now(),
          }),
          headers: { messageId: id },
        },
      ],
    });
    return id;
  }

  async consumeBroadcast(args: {
    consumerIdentity: string;
    maxMessages: number;
    blockMs?: number;
  }): Promise<MessageRef[]> {
    await this.ensureBroadcast(args.consumerIdentity, 'latest');
    const buf = this.broadcastBuffers.get(args.consumerIdentity) ?? [];
    this.broadcastBuffers.set(args.consumerIdentity, buf);

    if (buf.length === 0 && args.blockMs && args.blockMs > 0) {
      const deadline = Date.now() + args.blockMs;
      while (buf.length === 0 && Date.now() < deadline) {
        await Bun.sleep(25);
      }
    }
    return buf.splice(0, args.maxMessages);
  }

  async ackBroadcast(
    consumerIdentity: string,
    ids: string[],
  ): Promise<void> {
    await this.ack(
      ids.map((id) => ({
        id,
        queue: 'broadcast',
        taskName: '',
        payload: null,
        enqueuedAt: 0,
        deliveryCount: 1,
      })),
    );
  }

  async cleanupBroadcastGhosts(_idleMs: number): Promise<number> {
    return 0;
  }

  private payloadToRef(payload: EachMessagePayload): MessageRef {
    const raw = payload.message.value?.toString() ?? '{}';
    const parsed = JSON.parse(raw) as {
      taskName: string;
      payload: unknown;
      enqueuedAt: number;
      attempts?: number;
      backoff?: BackoffConfig;
      timeout?: number;
    };
    const headers = payload.message.headers ?? {};
    const messageId =
      headers.messageId?.toString() ??
      `${payload.partition}-${payload.message.offset}`;
    const deliveryCount = parseInt(
      headers.deliveryCount?.toString() ?? '1',
      10,
    );
    const queue = payload.topic.startsWith(`${this.prefix}.`)
      ? payload.topic.slice(this.prefix.length + 1).replace(/\.dead-letter$/, '')
      : payload.topic;

    return {
      id: messageId,
      queue,
      taskName: parsed.taskName,
      payload: parsed.payload,
      enqueuedAt: parsed.enqueuedAt,
      deliveryCount,
      attempts: parsed.attempts,
      backoff: parsed.backoff,
      timeout: parsed.timeout,
    };
  }

  private gcDedupe(now: number): void {
    for (const [k, exp] of this.dedupe) {
      if (exp <= now) this.dedupe.delete(k);
    }
  }
}
