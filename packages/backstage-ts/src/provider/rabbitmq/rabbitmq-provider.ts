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
  /** Max topic delivery attempts before drop. Default 5. */
  maxDeliveries?: number;
}

type AmqpConn = {
  createChannel(): Promise<AmqpChan>;
  createConfirmChannel(): Promise<AmqpConfirmChan>;
  close(): Promise<void>;
  on(event: string, cb: (...args: any[]) => void): void;
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
type AmqpConfirmChan = AmqpChan & {
  waitForConfirms(): Promise<void>;
};
type AmqpMsg = {
  content: Buffer;
  fields: { deliveryTag: number; redelivered: boolean; routingKey: string };
  properties: { headers?: Record<string, unknown>; persistent?: boolean };
};

async function loadAmqp(): Promise<{
  connect(url: string): Promise<AmqpConn>;
}> {
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

  readonly requiresDelays = true;
  private readonly url: string;
  private readonly prefix: string;
  private readonly maxDeliveries: number;
  private conn: AmqpConn | null = null;
  private pubChan: AmqpConfirmChan | null = null;
  private ctx: ProviderContext | null = null;
  private delayedExchangeReady = false;
  private builtinDelays: DelaysCapability | null = null;
  private boundQueues = new Set<string>();
  private closed = false;
  private connectPromise: Promise<void> | null = null;

  constructor(config: RabbitMQProviderConfig = {}) {
    this.url = config.url ?? 'amqp://guest:guest@localhost:5672';
    this.prefix = config.prefix ?? 'backstage';
    this.maxDeliveries = config.maxDeliveries ?? 5;
    this.jobs = this.createJobs();
    this.topics = this.createTopics();
  }

  async init(ctx: ProviderContext): Promise<void> {
    this.ctx = ctx;
    this.closed = false;
    await this.ensureConn();
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
    this.closed = true;
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
    if (this.conn && this.pubChan) return;
    if (this.connectPromise) {
      await this.connectPromise;
      return;
    }
    this.connectPromise = (async () => {
      const amqp = await loadAmqp();
      const conn = await amqp.connect(this.url);
      conn.on('error', () => {
        this.conn = null;
        this.pubChan = null;
      });
      conn.on('close', () => {
        this.conn = null;
        this.pubChan = null;
      });
      const pubChan = await conn.createConfirmChannel();
      pubChan.on('error', () => {
        this.pubChan = null;
      });
      pubChan.on('close', () => {
        this.pubChan = null;
      });
      this.conn = conn;
      this.pubChan = pubChan;
    })();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = null;
    }
  }

  private async publishConfirmed(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: object = {},
  ): Promise<void> {
    await this.ensureConn();
    const ch = this.pubChan!;
    const ok = exchange
      ? ch.publish(exchange, routingKey, content, options)
      : ch.sendToQueue(routingKey, content, options);
    void ok;
    await ch.waitForConfirms();
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
        const exchange = `${self.prefix}.delayed`;
        for (const q of queues) {
          await ch.assertQueue(self.qName(q), { durable: true });
          await ch.assertQueue(self.dlqName(q), { durable: true });
          if (self.delayedExchangeReady) {
            await ch.bindQueue(self.qName(q), exchange, q);
            self.boundQueues.add(q);
          }
        }
        await ch.close();
      },
      async publish(job: OutgoingJob): Promise<string> {
        const body: WireJob = {
          queue: job.queue,
          taskName: job.taskName,
          payload: job.payload,
          enqueuedAt: job.enqueuedAt,
          meta: job.meta,
          deliveryCount: job.deliveryCount ?? 1,
        };
        await self.publishConfirmed('', self.qName(job.queue), Buffer.from(JSON.stringify(body)), {
          persistent: true,
        });
        return `rabbit-${job.enqueuedAt}-${Math.random().toString(36).slice(2, 8)}`;
      },
      async consume(
        opts: ConsumeOptions,
        onDelivery: (d: JobDelivery) => Promise<void>,
      ): Promise<Subscription> {
        let running = true;
        let stopResolve: (() => void) | null = null;
        const stopped = new Promise<void>((r) => {
          stopResolve = r;
        });

        const loop = (async () => {
          let backoff = 1000;
          while (running && !self.closed) {
            try {
              await self.ensureConn();
              const ch = await self.conn!.createChannel();
              await ch.prefetch(opts.prefetch);
              for (const q of opts.queues) {
                await ch.assertQueue(self.qName(q), { durable: true });
                await ch.assertQueue(self.dlqName(q), { durable: true });
              }
              let channelDead = false;
              ch.on('error', () => {
                channelDead = true;
              });
              ch.on('close', () => {
                channelDead = true;
              });
              const tags: string[] = [];
              for (const q of opts.queues) {
                const { consumerTag } = await ch.consume(
                  self.qName(q),
                  (msg) => {
                    if (!msg || !running) return;
                    const delivery = self.toDelivery(ch, q, msg);
                    onDelivery(delivery).catch(() => {});
                  },
                  { noAck: false },
                );
                tags.push(consumerTag);
              }
              backoff = 1000;
              while (running && !channelDead && !self.closed) {
                await Bun.sleep(200);
              }
              try {
                await ch.close();
              } catch {
                /* ignore */
              }
              if (running && !self.closed) {
                self.ctx?.logger.warn('rabbitmq channel/connection lost; reconnecting');
                self.conn = null;
                self.pubChan = null;
              }
            } catch (err) {
              if (!running || self.closed) break;
              self.ctx?.logger.warn('rabbitmq consume reconnect', {
                error: String(err),
              });
              self.conn = null;
              self.pubChan = null;
              await Bun.sleep(backoff);
              backoff = Math.min(backoff * 2, 30_000);
            }
          }
          stopResolve?.();
        })();

        return {
          async stop() {
            running = false;
            await Promise.race([stopped, Bun.sleep(2000)]);
            await loop.catch(() => {});
          },
        };
      },
    };
  }

  private toDelivery(ch: AmqpChan, queue: string, msg: AmqpMsg): JobDelivery {
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
        void error;
        const next: WireJob = {
          ...body,
          deliveryCount: deliveryCount + 1,
          meta: body.meta ?? {},
        };
        const delays = self.delays ?? self.ctx?.capabilities.delays;
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
        await self.publishConfirmed(
          '',
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
        // Bind once in ensureQueues; only bind here if queue was not prepared.
        if (!self.boundQueues.has(job.queue)) {
          const ch = await self.conn!.createChannel();
          await ch.assertQueue(self.qName(job.queue), { durable: true });
          await ch.bindQueue(self.qName(job.queue), exchange, job.queue);
          await ch.close();
          self.boundQueues.add(job.queue);
        }
        await self.publishConfirmed(
          exchange,
          job.queue,
          Buffer.from(JSON.stringify(body)),
          {
            persistent: true,
            headers: { 'x-delay': delayMs },
          },
        );
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
        await self.pubChan!.assertExchange(exchange, 'topic', { durable: true });
        await self.publishConfirmed(
          exchange,
          topic,
          Buffer.from(
            JSON.stringify({
              payload,
              publishedAt: Date.now(),
              deliveryCount: 1,
            }),
          ),
          { persistent: true },
        );
        return `topic-${Date.now()}`;
      },
      async subscribe(
        opts: TopicSubscribeOptions,
        onMessage: (m: TopicDelivery) => Promise<void>,
      ): Promise<Subscription> {
        let running = true;
        let stopResolve: (() => void) | null = null;
        const stopped = new Promise<void>((r) => {
          stopResolve = r;
        });

        const loop = (async () => {
          let backoff = 1000;
          while (running && !self.closed) {
            try {
              await self.ensureConn();
              const exchange = `${self.prefix}.topics`;
              const ch = await self.conn!.createChannel();
              await ch.assertExchange(exchange, 'topic', { durable: true });
              let queueName: string;
              if (opts.group) {
                queueName = `${self.prefix}.topic.${opts.topic}.${opts.group}`;
                await ch.assertQueue(queueName, { durable: true });
              } else {
                const q = await ch.assertQueue('', {
                  exclusive: true,
                  autoDelete: true,
                });
                queueName = q.queue;
              }
              await ch.bindQueue(queueName, exchange, opts.topic);
              let channelDead = false;
              ch.on('error', () => {
                channelDead = true;
              });
              ch.on('close', () => {
                channelDead = true;
              });
              await ch.consume(queueName, (msg) => {
                if (!msg || !running) return;
                const body = JSON.parse(msg.content.toString());
                const count = Number(body.deliveryCount ?? 1);
                const delivery: TopicDelivery = {
                  id: String(msg.fields.deliveryTag),
                  topic: opts.topic,
                  payload: body.payload,
                  publishedAt: body.publishedAt ?? Date.now(),
                  deliveryCount: count,
                  async ack() {
                    ch.ack(msg);
                  },
                };
                onMessage(delivery)
                  .then(() => delivery.ack())
                  .catch(async (err) => {
                    if (count >= self.maxDeliveries) {
                      self.ctx?.logger.error(
                        `Topic handler failed after ${count} deliveries; dropping`,
                        {
                          topic: opts.topic,
                          error: err instanceof Error ? err.message : String(err),
                        },
                      );
                      ch.ack(msg);
                      return;
                    }
                    try {
                      // Retry to this subscriber's queue only (default exchange),
                      // not the topic exchange — other fan-out subscribers must
                      // not receive the retry copy.
                      await self.publishConfirmed(
                        '',
                        queueName,
                        Buffer.from(
                          JSON.stringify({
                            payload: body.payload,
                            publishedAt: body.publishedAt ?? Date.now(),
                            deliveryCount: count + 1,
                          }),
                        ),
                        { persistent: true },
                      );
                      ch.ack(msg);
                    } catch {
                      // leave unacked for broker redelivery
                    }
                  });
              });
              backoff = 1000;
              while (running && !channelDead && !self.closed) {
                await Bun.sleep(200);
              }
              try {
                await ch.close();
              } catch {
                /* ignore */
              }
              if (running && !self.closed) {
                self.conn = null;
                self.pubChan = null;
              }
            } catch (err) {
              if (!running || self.closed) break;
              self.conn = null;
              self.pubChan = null;
              await Bun.sleep(backoff);
              backoff = Math.min(backoff * 2, 30_000);
              void err;
            }
          }
          stopResolve?.();
        })();

        return {
          async stop() {
            running = false;
            await Promise.race([stopped, Bun.sleep(2000)]);
            await loop.catch(() => {});
          },
        };
      },
    };
  }
}
