import amqp, { type Channel, type ChannelModel, type Message } from 'amqplib';
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

export interface RabbitMQProviderConfig {
  url?: string;
  hostname?: string;
  port?: number;
  username?: string;
  password?: string;
  vhost?: string;
  prefetch?: number;
  prefix?: string;
}

interface PendingEntry {
  message: MessageRef;
  amqpMsg: Message;
  claimedAt: number;
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
 * RabbitMQ provider using durable queues, per-message TTL+DLX for delays,
 * fanout exchange for broadcast, and DLX for dead-letter.
 *
 * Failure semantics match Redis: unacked messages stay pending; reclaimIdle
 * re-offers them after idle+backoff. Broker also requeues on consumer death.
 */
export class RabbitMQProvider implements BackstageProvider {
  readonly name = 'rabbitmq';
  readonly capabilities: ProviderCapabilities = {
    durable: true,
    broadcast: true,
    scheduling: true,
    retries: true,
    deduplication: true,
  };

  private config: RabbitMQProviderConfig;
  private prefix: string;
  private conn: ChannelModel | null = null;
  private channel: Channel | null = null;
  private connected = false;
  private pending = new Map<string, PendingEntry>();
  private dedupe = new Map<string, number>();
  private broadcastQueues = new Map<string, string>();
  private ensured = new Set<string>();

  constructor(config: RabbitMQProviderConfig = {}) {
    this.config = config;
    this.prefix = config.prefix ?? 'backstage';
  }

  private workQueue(queue: string): string {
    return `${this.prefix}.${queue}`;
  }

  private delayedQueue(queue: string): string {
    return `${this.prefix}.${queue}.delayed`;
  }

  private dlqName(queue: string): string {
    return `${this.prefix}.${queue}.dead-letter`;
  }

  private broadcastExchange(): string {
    return `${this.prefix}.broadcast`;
  }

  private async connect(): Promise<Channel> {
    if (this.channel && this.connected) return this.channel;

    const url =
      this.config.url ??
      `amqp://${this.config.username ?? 'guest'}:${this.config.password ?? 'guest'}@${this.config.hostname ?? 'localhost'}:${this.config.port ?? 5672}${this.config.vhost ?? '/'}`;

    this.conn = await amqp.connect(url);
    this.channel = await this.conn.createChannel();
    await this.channel.prefetch(this.config.prefetch ?? 50);
    this.connected = true;

    this.conn.on('error', () => {
      this.connected = false;
    });
    this.conn.on('close', () => {
      this.connected = false;
      this.channel = null;
      this.conn = null;
    });

    return this.channel;
  }

  async ensureQueues(queues: string[]): Promise<void> {
    const ch = await this.connect();
    for (const queue of queues) {
      if (this.ensured.has(queue)) continue;

      const work = this.workQueue(queue);
      const delayed = this.delayedQueue(queue);
      const dlq = this.dlqName(queue);

      await ch.assertQueue(dlq, { durable: true });
      await ch.assertQueue(work, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': '',
          'x-dead-letter-routing-key': dlq,
        },
      });
      // Delayed queue: expired messages DLX into the work queue
      await ch.assertQueue(delayed, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': '',
          'x-dead-letter-routing-key': work,
        },
      });

      this.ensured.add(queue);
    }
  }

  async close(): Promise<void> {
    try {
      await this.channel?.close();
    } catch {
      /* ignore */
    }
    try {
      await this.conn?.close();
    } catch {
      /* ignore */
    }
    this.channel = null;
    this.conn = null;
    this.connected = false;
    this.pending.clear();
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
      const ttl = opts.dedupe.ttl ?? 3600000;
      this.dedupe.set(opts.dedupe.key, now + ttl);
    }

    const ch = await this.connect();
    const queue = String(opts.queue ?? opts.priority ?? 'default');
    await this.ensureQueues([queue]);

    const enqueuedAt = Date.now();
    const id = `${enqueuedAt}-${Math.random().toString(36).slice(2, 10)}`;
    const body = Buffer.from(
      JSON.stringify({
        taskName,
        payload,
        enqueuedAt,
        attempts: opts.attempts,
        backoff: opts.backoff,
        timeout: opts.timeout,
      }),
    );

    const headers: Record<string, unknown> = {
      taskName,
      enqueuedAt,
      deliveryCount: 1,
    };
    if (opts.attempts !== undefined) headers.attempts = opts.attempts;
    if (opts.backoff) headers.backoff = JSON.stringify(opts.backoff);
    if (opts.timeout !== undefined) headers.timeout = opts.timeout;

    const delay = opts.delay;
    if (delay && delay > 0) {
      ch.sendToQueue(this.delayedQueue(queue), body, {
        persistent: true,
        messageId: id,
        expiration: String(delay),
        headers,
        contentType: 'application/json',
      });
      return `scheduled:${enqueuedAt + delay}`;
    }

    ch.sendToQueue(this.workQueue(queue), body, {
      persistent: true,
      messageId: id,
      headers,
      contentType: 'application/json',
    });
    return id;
  }

  async consume(args: ConsumeArgs): Promise<MessageRef[]> {
    const ch = await this.connect();
    await this.ensureQueues(args.queues);
    const out: MessageRef[] = [];
    const deadline = Date.now() + (args.blockMs ?? 0);
    const max = args.maxMessages;

    while (out.length < max) {
      let got = false;
      for (const queue of args.queues) {
        if (out.length >= max) break;
        const msg = await ch.get(this.workQueue(queue), { noAck: false });
        if (!msg) continue;
        got = true;
        const ref = this.toMessageRef(queue, msg);
        this.pending.set(ref.id, {
          message: ref,
          amqpMsg: msg,
          claimedAt: Date.now(),
        });
        out.push(ref);
      }
      if (got) continue;
      if (!args.blockMs || Date.now() >= deadline) break;
      await Bun.sleep(Math.min(50, Math.max(0, deadline - Date.now())));
    }
    return out;
  }

  async ack(messages: MessageRef[]): Promise<void> {
    const ch = await this.connect();
    for (const m of messages) {
      const entry = this.pending.get(m.id);
      if (entry) {
        ch.ack(entry.amqpMsg);
        this.pending.delete(m.id);
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

    for (const [id, entry] of this.pending) {
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
    const ch = await this.connect();
    await this.ensureQueues([message.queue]);
    const body = Buffer.from(
      JSON.stringify({
        taskName: message.taskName,
        payload: message.payload,
        enqueuedAt: message.enqueuedAt,
        originalId: meta.originalId,
        deliveryCount: meta.deliveryCount,
        deadLetteredAt: Date.now(),
        error: meta.error,
      }),
    );
    ch.sendToQueue(this.dlqName(message.queue), body, {
      persistent: true,
      messageId: `dlq-${meta.originalId}`,
      contentType: 'application/json',
    });

    const entry = this.pending.get(message.id);
    if (entry) {
      ch.ack(entry.amqpMsg);
      this.pending.delete(message.id);
    }
  }

  async promoteDueScheduled(_nowMs?: number): Promise<number> {
    // Delays use per-message TTL + DLX; RabbitMQ promotes automatically.
    return 0;
  }

  async ensureBroadcast(
    consumerIdentity: string,
    _start: 'latest' | 'beginning',
  ): Promise<void> {
    const ch = await this.connect();
    const exchange = this.broadcastExchange();
    await ch.assertExchange(exchange, 'fanout', { durable: true });
    const q = await ch.assertQueue(`${this.prefix}.broadcast.${consumerIdentity}`, {
      exclusive: false,
      durable: false,
      autoDelete: true,
    });
    await ch.bindQueue(q.queue, exchange, '');
    this.broadcastQueues.set(consumerIdentity, q.queue);
  }

  async broadcast(taskName: string, payload: unknown): Promise<string> {
    const ch = await this.connect();
    const exchange = this.broadcastExchange();
    await ch.assertExchange(exchange, 'fanout', { durable: true });
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    const body = Buffer.from(
      JSON.stringify({
        taskName,
        payload,
        enqueuedAt: Date.now(),
      }),
    );
    ch.publish(exchange, '', body, {
      messageId: id,
      contentType: 'application/json',
    });
    return id;
  }

  async consumeBroadcast(args: {
    consumerIdentity: string;
    maxMessages: number;
    blockMs?: number;
  }): Promise<MessageRef[]> {
    const ch = await this.connect();
    let queueName = this.broadcastQueues.get(args.consumerIdentity);
    if (!queueName) {
      await this.ensureBroadcast(args.consumerIdentity, 'latest');
      queueName = this.broadcastQueues.get(args.consumerIdentity)!;
    }

    const out: MessageRef[] = [];
    const deadline = Date.now() + (args.blockMs ?? 0);

    while (out.length < args.maxMessages) {
      const msg = await ch.get(queueName, { noAck: false });
      if (!msg) {
        if (!args.blockMs || Date.now() >= deadline) break;
        await Bun.sleep(Math.min(50, Math.max(0, deadline - Date.now())));
        continue;
      }
      const parsed = JSON.parse(msg.content.toString()) as {
        taskName: string;
        payload: unknown;
        enqueuedAt: number;
      };
      const id = msg.properties.messageId ?? `${Date.now()}`;
      const ref: MessageRef = {
        id,
        queue: 'broadcast',
        taskName: parsed.taskName,
        payload: parsed.payload,
        enqueuedAt: parsed.enqueuedAt,
        deliveryCount: 1,
      };
      this.pending.set(id, {
        message: ref,
        amqpMsg: msg,
        claimedAt: Date.now(),
      });
      out.push(ref);
    }
    return out;
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
    // Auto-delete broadcast queues clean up when consumers disconnect.
    return 0;
  }

  private toMessageRef(queue: string, msg: Message): MessageRef {
    const parsed = JSON.parse(msg.content.toString()) as {
      taskName: string;
      payload: unknown;
      enqueuedAt: number;
      attempts?: number;
      backoff?: BackoffConfig;
      timeout?: number;
    };
    const headers = (msg.properties.headers ?? {}) as Record<string, unknown>;
    const deliveryCount =
      (typeof headers.deliveryCount === 'number'
        ? headers.deliveryCount
        : undefined) ??
      (msg.fields.redelivered ? 2 : 1);

    let backoff = parsed.backoff;
    if (!backoff && typeof headers.backoff === 'string') {
      try {
        backoff = JSON.parse(headers.backoff);
      } catch {
        /* ignore */
      }
    }

    return {
      id: msg.properties.messageId ?? `${Date.now()}-${Math.random()}`,
      queue,
      taskName: parsed.taskName,
      payload: parsed.payload,
      enqueuedAt: parsed.enqueuedAt,
      deliveryCount,
      attempts: parsed.attempts ?? (headers.attempts as number | undefined),
      backoff,
      timeout: parsed.timeout ?? (headers.timeout as number | undefined),
    };
  }

  private gcDedupe(now: number): void {
    for (const [k, exp] of this.dedupe) {
      if (exp <= now) this.dedupe.delete(k);
    }
  }
}
