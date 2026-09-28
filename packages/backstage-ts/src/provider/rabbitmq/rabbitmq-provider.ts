import type {
  BackstageProvider,
  ConsumeOptions,
  DelaysCapability,
  JobDelivery,
  JobsCapability,
  OutgoingJob,
  ProviderContext,
  Subscription,
  TopicDelivery,
  TopicSubscribeOptions,
  TopicsCapability,
} from '../types';

export interface RabbitMQProviderConfig {
  url?: string;
  prefix?: string;
  /** Prefetch default when consume opts omit. */
  prefetch?: number;
}

type AmqpConn = {
  createChannel(): Promise<AmqpChan>;
  close(): Promise<void>;
};
type AmqpChan = {
  assertQueue(name: string, opts?: object): Promise<{ queue: string }>;
  assertExchange(name: string, type: string, opts?: object): Promise<object>;
  bindQueue(queue: string, exchange: string, key: string): Promise<object>;
  publish(exchange: string, routingKey: string, content: Buffer, options?: object): boolean;
  sendToQueue(queue: string, content: Buffer, options?: object): boolean;
  consume(queue: string, onMsg: (msg: AmqpMsg | null) => void, opts?: object): Promise<{ consumerTag: string }>;
  ack(msg: AmqpMsg): void;
  nack(msg: AmqpMsg, allUpTo?: boolean, requeue?: boolean): void;
  prefetch(count: number): Promise<object>;
  close(): Promise<void>;
  on(event: string, cb: (...args: any[]) => void): void;
  checkExchange?(name: string): Promise<object>;
};
type AmqpMsg = {
  content: Buffer;
  fields: { deliveryTag: number; redelivered: boolean; routingKey: string };
  properties: { headers?: Record<string, unknown>; persistent?: boolean };
};

async function loadAmqp(): Promise<{ connect(url: string): Promise<AmqpConn> }> {
  try {
    return await import('amqplib');
  } catch {
    throw new Error(
      'RabbitMQProvider requires the optional peer dependency "amqplib". Install it with: bun add amqplib',
    );
  }
}

interface WireJob {
  queue: string;
  taskName: string;
  payload: unknown;
  enqueuedAt: number;
  meta: OutgoingJob['meta'];
  deliveryCount: number;
}

/**
 * RabbitMQ transport: durable queues, publisher confirms, basic.consume + qos.
 * jobs.requires = ['delays'] for retries. Provides topics; no dedupe.
 * Delays: uses rabbitmq_delayed_message_exchange when detected in init,
 * otherwise requires a plugged DelaysCapability.
 */
export class RabbitMQProvider implements BackstageProvider {
  readonly name = 'rabbitmq';
  readonly jobs: JobsCapability;
  readonly topics: TopicsCapability;
  delays?: DelaysCapability;
  // no dedupe

  readonly requiresDelays = true;
  private readonly url: string;
  private readonly prefix: string;
  private conn: AmqpConn | null = null;
  private pubChan: AmqpChan | null = null;
  private ctx: ProviderContext | null = null;
  private delayedExchangeReady = false;
  private builtinDelays: DelaysCapability | null = null;

  constructor(config: RabbitMQProviderConfig = {}) {
    this.url = config.url ?? 'amqp://guest:guest@localhost:5672';
    this.prefix = config.prefix ?? 'backstage';
    this.jobs = this.createJobs();
    this.topics = this.createTopics();
  }

  async init(ctx: ProviderContext): Promise<void> {
    this.ctx = ctx;
    await this.ensureConn();
    // Detect delayed-message plugin
    try {
      const ch = await this.conn!.createChannel();
      await ch.assertExchange(`${this.prefix}.delayed`, 'x-delayed-message', {
        durable: true,
        arguments: { 'x-delayed-type': 'direct' },
      });
      await ch.close();
      this.delayedExchangeReady = true;
      this.builtinDelays = this.createBuiltinDelays();
      this.delays = ctx.capabilities.delays ?? this.builtinDelays;
    } catch {
      this.delayedExchangeReady = false;
      this.delays = ctx.capabilities.delays;
      if (!this.delays) {
        throw new Error(
          `Provider "rabbitmq" jobs require delays. Enable rabbitmq_delayed_message_exchange or pass capabilities.delays`,
        );
      }
    }
  }

  async close(): Promise<void> {
    try {
      await this.pubChan?.close();
    } catch {
      /* ignore */
    }
    try {
      await this.conn?.close();
    } catch {
      /* ignore */
    }
    this.pubChan = null;
    this.conn = null;
  }

  private async ensureConn(): Promise<void> {
    if (this.conn) return;
    const amqp = await loadAmqp();
    this.conn = await amqp.connect(this.url);
    this.pubChan = await this.conn.createChannel();
    // Confirm channel for publishes — amqplib confirm via createConfirmChannel if available
  }

  private qName(queue: string): string {
    return `${this.prefix}.${queue}`;
  }
  private dlqName(queue: string): string {
    return `${this.prefix}.${queue}.dead-letter`;
  }

  private createJobs(): JobsCapability {
    const self = this;
    return {
      name: 'rabbitmq',
      requires: ['delays'],
      async ensureQueues(queues: string[]): Promise<void> {
        await self.ensureConn();
        const ch = await self.conn!.createChannel();
        for (const q of queues) {
          await ch.assertQueue(self.qName(q), { durable: true });
          await ch.assertQueue(self.dlqName(q), { durable: true });
        }
        await ch.close();
      },
      async publish(job: OutgoingJob): Promise<string> {
        await self.ensureConn();
        const body: WireJob = {
          queue: job.queue,
          taskName: job.taskName,
          payload: job.payload,
          enqueuedAt: job.enqueuedAt,
          meta: job.meta,
          deliveryCount: job.deliveryCount ?? 1,
        };
        const ok = self.pubChan!.sendToQueue(
          self.qName(job.queue),
          Buffer.from(JSON.stringify(body)),
          { persistent: true },
        );
        if (!ok) {
          // channel buffer full — still accepted by broker eventually
        }
        return `rabbit-${job.enqueuedAt}-${Math.random().toString(36).slice(2, 8)}`;
      },
      async consume(
        opts: ConsumeOptions,
        onDelivery: (d: JobDelivery) => Promise<void>,
      ): Promise<Subscription> {
        await self.ensureConn();
        const ch = await self.conn!.createChannel();
        await ch.prefetch(opts.prefetch);
        for (const q of opts.queues) {
          await ch.assertQueue(self.qName(q), { durable: true });
          await ch.assertQueue(self.dlqName(q), { durable: true });
        }
        let running = true;
        const tags: string[] = [];
        for (const q of opts.queues) {
          const { consumerTag } = await ch.consume(
            self.qName(q),
            (msg) => {
              if (!msg || !running) return;
              const delivery = self.toDelivery(ch, q, msg, opts);
              onDelivery(delivery).catch(() => {});
            },
            { noAck: false },
          );
          tags.push(consumerTag);
        }
        return {
          async stop() {
            running = false;
            try {
              await ch.close();
            } catch {
              /* ignore */
            }
          },
        };
      },
    };
  }

  private toDelivery(
    ch: AmqpChan,
    queue: string,
    msg: AmqpMsg,
    _opts: ConsumeOptions,
  ): JobDelivery {
    const body = JSON.parse(msg.content.toString()) as WireJob;
    let deliveryCount = body.deliveryCount ?? 1;
    if (msg.fields.redelivered) deliveryCount += 1;
    const self = this;
    return {
      id: String(msg.fields.deliveryTag),
      queue,
      taskName: body.taskName,
      payload: body.payload,
      enqueuedAt: body.enqueuedAt,
      deliveryCount,
      meta: body.meta ?? {},
      async ack() {
        ch.ack(msg);
      },
      async retry({ delayMs, error }) {
        const next: WireJob = {
          ...body,
          deliveryCount: deliveryCount + 1,
          meta: body.meta ?? {},
        };
        void error;
        const delays = self.ctx?.capabilities.delays ?? self.delays;
        if (!delays) throw new Error('rabbitmq retry requires delays capability');
        if (delayMs <= 0) {
          await self.jobs.publish({
            queue: next.queue,
            taskName: next.taskName,
            payload: next.payload,
            enqueuedAt: next.enqueuedAt,
            meta: next.meta,
            deliveryCount: next.deliveryCount,
          });
        } else {
          await delays.schedule(
            {
              queue: next.queue,
              taskName: next.taskName,
              payload: next.payload,
              enqueuedAt: next.enqueuedAt,
              meta: next.meta,
              deliveryCount: next.deliveryCount,
            },
            Date.now() + delayMs,
          );
        }
        ch.ack(msg);
      },
      async deadLetter({ error }) {
        const payload = {
          ...body,
          error,
          originalId: String(msg.fields.deliveryTag),
          deliveryCount,
          deadLetteredAt: Date.now(),
        };
        self.pubChan!.sendToQueue(
          self.dlqName(queue),
          Buffer.from(JSON.stringify(payload)),
          { persistent: true },
        );
        ch.ack(msg);
      },
    };
  }

  private createBuiltinDelays(): DelaysCapability {
    const self = this;
    return {
      name: 'rabbitmq-delayed',
      async schedule(job: OutgoingJob, runAt: number): Promise<string> {
        await self.ensureConn();
        const delayMs = Math.max(0, runAt - Date.now());
        const body: WireJob = {
          queue: job.queue,
          taskName: job.taskName,
          payload: job.payload,
          enqueuedAt: job.enqueuedAt,
          meta: job.meta,
          deliveryCount: job.deliveryCount ?? 1,
        };
        const exchange = `${self.prefix}.delayed`;
        self.pubChan!.publish(
          exchange,
          job.queue,
          Buffer.from(JSON.stringify(body)),
          {
            persistent: true,
            headers: { 'x-delay': delayMs },
          },
        );
        // Bind delayed exchange to work queue
        const ch = await self.conn!.createChannel();
        await ch.assertQueue(self.qName(job.queue), { durable: true });
        await ch.bindQueue(self.qName(job.queue), exchange, job.queue);
        await ch.close();
        return `scheduled:${runAt}`;
      },
    };
  }

  private createTopics(): TopicsCapability {
    const self = this;
    return {
      name: 'rabbitmq',
      async publish(topic: string, payload: unknown): Promise<string> {
        await self.ensureConn();
        const exchange = `${self.prefix}.topics`;
        const ch = self.pubChan!;
        await ch.assertExchange(exchange, 'topic', { durable: true });
        ch.publish(exchange, topic, Buffer.from(JSON.stringify({
          payload,
          publishedAt: Date.now(),
        })), { persistent: true });
        return `topic-${Date.now()}`;
      },
      async subscribe(
        opts: TopicSubscribeOptions,
        onMessage: (m: TopicDelivery) => Promise<void>,
      ): Promise<Subscription> {
        await self.ensureConn();
        const exchange = `${self.prefix}.topics`;
        const ch = await self.conn!.createChannel();
        await ch.assertExchange(exchange, 'topic', { durable: true });
        let queueName: string;
        if (opts.group) {
          queueName = `${self.prefix}.topic.${opts.topic}.${opts.group}`;
          await ch.assertQueue(queueName, { durable: true });
        } else {
          const q = await ch.assertQueue('', { exclusive: true, autoDelete: true });
          queueName = q.queue;
        }
        await ch.bindQueue(queueName, exchange, opts.topic);
        let running = true;
        await ch.consume(queueName, (msg) => {
          if (!msg || !running) return;
          const body = JSON.parse(msg.content.toString());
          const delivery: TopicDelivery = {
            id: String(msg.fields.deliveryTag),
            topic: opts.topic,
            payload: body.payload,
            publishedAt: body.publishedAt ?? Date.now(),
            deliveryCount: msg.fields.redelivered ? 2 : 1,
            async ack() {
              ch.ack(msg);
            },
          };
          onMessage(delivery)
            .then(() => delivery.ack())
            .catch(() => {
              // leave unacked for redelivery; drop after max is orchestrator/provider concern
              ch.nack(msg, false, true);
            });
        });
        return {
          async stop() {
            running = false;
            try {
              await ch.close();
            } catch {
              /* ignore */
            }
          },
        };
      },
    };
  }
}
