import type {
  BackstageProvider,
  ConsumeOptions,
  JobDelivery,
  JobsCapability,
  OutgoingJob,
  ProviderContext,
  Subscription,
  TopicDelivery,
  TopicSubscribeOptions,
  TopicsCapability,
} from '../types';

export interface KafkaProviderConfig {
  brokers?: string[];
  clientId?: string;
  prefix?: string;
  /** Max topic delivery attempts before drop. Default 5. */
  maxDeliveries?: number;
  /**
   * Partitions for topics created via ensureQueues / admin.
   * Default 3. Use 1 for single-broker test clusters.
   */
  partitions?: number;
  /**
   * Replication factor for topics created via ensureQueues / admin.
   * Default 3. Use 1 for single-broker test clusters.
   */
  replicationFactor?: number;
}

type KafkaMsg = {
  topic: string;
  partition: number;
  offset: string;
  value: Buffer | null;
  headers?: Record<string, Buffer | undefined>;
};

/**
 * Kafka transport: acks:all idempotent producer, contiguous offset commits,
 * jobs.requires=['delays']. No delays/dedupe built-in — plug them in.
 *
 * Limits / non-goals: does **not** use transactional producers and does
 * **not** rely on log compaction. Topic creation defaults to 3 partitions
 * and replicationFactor 3 (override via config; tests should use 1/1).
 */
export class KafkaProvider implements BackstageProvider {
  readonly name = 'kafka';
  readonly jobs: JobsCapability;
  readonly topics: TopicsCapability;

  private readonly brokers: string[];
  private readonly clientId: string;
  private readonly prefix: string;
  private readonly maxDeliveries: number;
  private readonly partitions: number;
  private readonly replicationFactor: number;
  private ctx: ProviderContext | null = null;
  private Kafka!: any;
  private kafka: any;
  private producer: any;
  private closed = false;
  private readonly topicReady = new Map<string, Promise<void>>();

  constructor(config: KafkaProviderConfig = {}) {
    this.brokers = config.brokers ?? ['localhost:9092'];
    this.clientId = config.clientId ?? 'backstage';
    this.prefix = config.prefix ?? 'backstage';
    this.maxDeliveries = config.maxDeliveries ?? 5;
    this.partitions = config.partitions ?? 3;
    this.replicationFactor = config.replicationFactor ?? 3;
    this.jobs = this.createJobs();
    this.topics = this.createTopics();
  }

  async init(ctx: ProviderContext): Promise<void> {
    this.ctx = ctx;
    this.closed = false;
    try {
      const mod = await import('kafkajs');
      this.Kafka = mod.Kafka;
    } catch {
      throw new Error(
        'KafkaProvider requires the optional peer dependency "kafkajs". Install it with: bun add kafkajs',
      );
    }
    this.kafka = new this.Kafka({
      clientId: this.clientId,
      brokers: this.brokers,
      retry: { retries: 8 },
    });
    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 5,
    });
    await this.producer.connect();
  }

  async close(): Promise<void> {
    this.closed = true;
    try {
      await this.producer?.disconnect();
    } catch {
      /* ignore */
    }
  }

  /** Create topics that don't exist yet and wait until they have leaders. */
  private async createTopicsAndWait(names: string[]): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({
        topics: names.map((topic) => ({
          topic,
          numPartitions: this.partitions,
          replicationFactor: this.replicationFactor,
        })),
        waitForLeaders: true,
      });
      // Extra metadata wait — waitForLeaders can still race with produce.
      const deadline = Date.now() + 15_000;
      while (Date.now() < deadline) {
        let meta: any;
        try {
          meta = await admin.fetchTopicMetadata({ topics: names });
        } catch {
          // UNKNOWN_TOPIC_OR_PARTITION while the broker is still creating it.
          await Bun.sleep(100);
          continue;
        }
        const ready = names.every((n) =>
          meta.topics.some((t: any) => t.name === n && !t.error && t.partitions?.length),
        );
        if (ready) break;
        await Bun.sleep(100);
      }
    } finally {
      await admin.disconnect();
    }
  }

  /**
   * On brokers without auto.create.topics.enable a topic exists only if we
   * create it. Cached per topic; a failure is retried on the next call.
   */
  private ensureTopic(name: string): Promise<void> {
    let ready = this.topicReady.get(name);
    if (!ready) {
      ready = this.createTopicsAndWait([name]).catch((err) => {
        this.topicReady.delete(name);
        throw err;
      });
      this.topicReady.set(name, ready);
    }
    return ready;
  }

  /**
   * Pin a group that has never committed to the topic's current end, so
   * 'latest' means "after subscribe() returned", not "whenever the group
   * finally joined". Groups with commits keep their offsets. Best effort:
   * if the group is already active, Kafka's own 'latest' applies.
   */
  private async pinNewGroupToEnd(groupId: string, topic: string): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const committed = await admin.fetchOffsets({ groupId, topics: [topic] });
      const partitions = committed[0]?.partitions ?? [];
      if (partitions.some((p: { offset: string }) => p.offset !== '-1')) return;
      const ends = await admin.fetchTopicOffsets(topic);
      await admin.setOffsets({
        groupId,
        topic,
        partitions: ends.map((e: { partition: number; high: string }) => ({
          partition: e.partition,
          offset: e.high,
        })),
      });
    } catch {
      // Active group or old broker: fall back to Kafka's 'latest'.
    } finally {
      await admin.disconnect();
    }
  }

  private topicForQueue(queue: string): string {
    return `${this.prefix}.${queue}`;
  }
  private dlqTopic(queue: string): string {
    return `${this.prefix}.${queue}.dead-letter`;
  }

  private createJobs(): JobsCapability {
    const self = this;
    return {
      name: 'kafka',
      requires: ['delays'],
      async ensureQueues(queues: string[]): Promise<void> {
        await self.createTopicsAndWait(
          queues.flatMap((q) => [self.topicForQueue(q), self.dlqTopic(q)]),
        );
      },
      async publish(job: OutgoingJob): Promise<string> {
        const topic = self.topicForQueue(job.queue);
        const value = JSON.stringify({
          taskName: job.taskName,
          payload: job.payload,
          enqueuedAt: job.enqueuedAt,
          meta: job.meta,
          deliveryCount: job.deliveryCount ?? 1,
        });
        await self.ensureTopic(topic);
        const result = await self.producer.send({
          topic,
          acks: -1,
          messages: [{ value }],
        });
        const r0 = result?.[0];
        return r0
          ? `${r0.topicName}:${r0.partition}:${r0.baseOffset}`
          : `kafka-${Date.now()}`;
      },
      async consume(
        opts: ConsumeOptions,
        onDelivery: (d: JobDelivery) => Promise<void>,
      ): Promise<Subscription> {
        let running = true;
        let stopResolve!: () => void;
        const stopped = new Promise<void>((r) => {
          stopResolve = r;
        });

        const loop = (async () => {
          let backoff = 1000;
          while (running && !self.closed) {
            const consumer = self.kafka.consumer({
              groupId: opts.group,
              maxInFlightRequests: opts.prefetch,
            });
            try {
              await consumer.connect();
              const topics = opts.queues.map((q) => self.topicForQueue(q));
              for (const t of topics) {
                await consumer.subscribe({ topic: t, fromBeginning: true });
              }

              const trackers = new Map<string, ContiguousOffsetTracker>();
              let inFlight = 0;
              const pending = new Set<Promise<void>>();

              await consumer.run({
                autoCommit: false,
                partitionsConsumedConcurrently: Math.max(1, opts.prefetch),
                eachMessage: async ({
                  topic,
                  partition,
                  message,
                }: {
                  topic: string;
                  partition: number;
                  message: KafkaMsg;
                }) => {
                  if (!running) return;
                  while (inFlight >= opts.prefetch) {
                    await Bun.sleep(5);
                    if (!running) return;
                  }
                  inFlight++;
                  const key = `${topic}:${partition}`;
                  let tracker = trackers.get(key);
                  if (!tracker) {
                    tracker = new ContiguousOffsetTracker();
                    trackers.set(key, tracker);
                  }
                  const offset = message.offset;
                  tracker.markUnsettled(offset);
                  const qName =
                    opts.queues.find((q) => self.topicForQueue(q) === topic) ??
                    topic;

                  let body: any = {};
                  try {
                    body = JSON.parse(message.value?.toString() || '{}');
                  } catch {
                    body = {};
                  }
                  const deliveryCount = body.deliveryCount ?? 1;
                  let settled = false;
                  const settle = async () => {
                    if (settled) return;
                    settled = true;
                    tracker!.markSettled(offset);
                    const commitTo = tracker!.contiguousCommitOffset();
                    if (commitTo !== null) {
                      await consumer.commitOffsets([
                        {
                          topic,
                          partition,
                          offset: String(BigInt(commitTo) + 1n),
                        },
                      ]);
                    }
                    inFlight--;
                  };

                  const delivery: JobDelivery = {
                    id: `${topic}:${partition}:${offset}`,
                    queue: qName,
                    taskName: body.taskName ?? '',
                    payload: body.payload,
                    enqueuedAt: body.enqueuedAt ?? Date.now(),
                    deliveryCount,
                    meta: body.meta ?? {},
                    async ack() {
                      await settle();
                    },
                    async retry({ delayMs, error }) {
                      void error;
                      const delays = self.ctx?.capabilities.delays;
                      if (!delays)
                        throw new Error('kafka retry requires delays capability');
                      await delays.schedule(
                        {
                          queue: qName,
                          taskName: body.taskName,
                          payload: body.payload,
                          enqueuedAt: body.enqueuedAt ?? Date.now(),
                          meta: body.meta ?? {},
                          deliveryCount: deliveryCount + 1,
                        },
                        Date.now() + Math.max(0, delayMs),
                      );
                      await settle();
                    },
                    async deadLetter({ error }) {
                      await self.producer.send({
                        topic: self.dlqTopic(qName),
                        acks: -1,
                        messages: [
                          {
                            value: JSON.stringify({
                              ...body,
                              error,
                              originalId: `${topic}:${partition}:${offset}`,
                              deliveryCount,
                              deadLetteredAt: Date.now(),
                            }),
                          },
                        ],
                      });
                      await settle();
                    },
                  };

                  const work = (async () => {
                    try {
                      await onDelivery(delivery);
                    } catch {
                      if (!settled) inFlight--;
                    }
                  })();
                  pending.add(work);
                  work.finally(() => pending.delete(work));
                  // Return without awaiting work so prefetch concurrency applies
                },
              });

              backoff = 1000;
              while (running && !self.closed) {
                await Bun.sleep(200);
              }
              await Promise.allSettled([...pending]);
              await consumer.disconnect().catch(() => {});
            } catch (err) {
              try {
                await consumer.disconnect();
              } catch {
                /* ignore */
              }
              if (!running || self.closed) break;
              self.ctx?.logger.warn('kafka consumer disconnected; reconnecting', {
                error: String(err),
              });
              await Bun.sleep(backoff);
              backoff = Math.min(backoff * 2, 30_000);
            }
          }
          stopResolve();
        })();

        return {
          async stop() {
            running = false;
            await Promise.race([stopped, Bun.sleep(5000)]);
            await loop.catch(() => {});
          },
        };
      },
    };
  }

  private createTopics(): TopicsCapability {
    const self = this;
    return {
      name: 'kafka',
      async publish(topic: string, payload: unknown): Promise<string> {
        const t = `${self.prefix}.topic.${topic}`;
        await self.ensureTopic(t);
        const result = await self.producer.send({
          topic: t,
          acks: -1,
          messages: [
            {
              value: JSON.stringify({
                payload,
                publishedAt: Date.now(),
                deliveryCount: 1,
              }),
            },
          ],
        });
        const r0 = result?.[0];
        return r0 ? `${r0.baseOffset}` : `topic-${Date.now()}`;
      },
      async subscribe(
        opts: TopicSubscribeOptions,
        onMessage: (m: TopicDelivery) => Promise<void>,
      ): Promise<Subscription> {
        let running = true;
        let stopResolve!: () => void;
        const stopped = new Promise<void>((r) => {
          stopResolve = r;
        });
        const topic = `${self.prefix}.topic.${opts.topic}`;
        const group = opts.group
          ? `${self.prefix}.grp.${opts.group}`
          : `${self.prefix}.sub.${opts.consumerId}`;
        await self.ensureTopic(topic);
        if (opts.from !== 'earliest') await self.pinNewGroupToEnd(group, topic);

        const loop = (async () => {
          let backoff = 1000;
          while (running && !self.closed) {
            const consumer = self.kafka.consumer({ groupId: group });
            try {
              await consumer.connect();
              await consumer.subscribe({
                topic,
                fromBeginning: opts.from === 'earliest',
              });
              await consumer.run({
                autoCommit: false,
                eachMessage: async ({
                  topic: t,
                  partition,
                  message,
                }: {
                  topic: string;
                  partition: number;
                  message: KafkaMsg;
                }) => {
                  if (!running) return;
                  const body = JSON.parse(message.value?.toString() || '{}');
                  const count = Number(body.deliveryCount ?? 1);
                  const delivery: TopicDelivery = {
                    id: message.offset,
                    topic: opts.topic,
                    payload: body.payload,
                    publishedAt: body.publishedAt ?? Date.now(),
                    deliveryCount: count,
                    async ack() {
                      await consumer.commitOffsets([
                        {
                          topic: t,
                          partition,
                          offset: String(BigInt(message.offset) + 1n),
                        },
                      ]);
                    },
                  };
                  try {
                    await onMessage(delivery);
                    await delivery.ack();
                  } catch (err) {
                    if (count >= self.maxDeliveries) {
                      self.ctx?.logger.error(
                        `Topic handler failed after ${count} deliveries; dropping`,
                        {
                          topic: opts.topic,
                          error:
                            err instanceof Error ? err.message : String(err),
                        },
                      );
                      await delivery.ack();
                      return;
                    }
                    try {
                      await self.producer.send({
                        topic,
                        acks: -1,
                        messages: [
                          {
                            value: JSON.stringify({
                              payload: body.payload,
                              publishedAt: body.publishedAt ?? Date.now(),
                              deliveryCount: count + 1,
                            }),
                          },
                        ],
                      });
                      await delivery.ack();
                    } catch {
                      // leave uncommitted for redelivery after rebalance
                    }
                  }
                },
              });
              backoff = 1000;
              while (running && !self.closed) {
                await Bun.sleep(200);
              }
              await consumer.disconnect().catch(() => {});
            } catch (err) {
              try {
                await consumer.disconnect();
              } catch {
                /* ignore */
              }
              if (!running || self.closed) break;
              await Bun.sleep(backoff);
              backoff = Math.min(backoff * 2, 30_000);
              void err;
            }
          }
          stopResolve();
        })();

        return {
          async stop() {
            running = false;
            await Promise.race([stopped, Bun.sleep(5000)]);
            await loop.catch(() => {});
          },
        };
      },
    };
  }
}

/** Commit only the highest contiguous settled offset (never skip unsettled). */
export class ContiguousOffsetTracker {
  private unsettled = new Set<string>();
  private settled = new Set<string>();
  private highestContiguous: bigint | null = null;

  markUnsettled(offset: string): void {
    this.unsettled.add(offset);
  }

  markSettled(offset: string): void {
    this.unsettled.delete(offset);
    this.settled.add(offset);
    this.recompute();
  }

  contiguousCommitOffset(): string | null {
    return this.highestContiguous === null
      ? null
      : String(this.highestContiguous);
  }

  private recompute(): void {
    if (this.settled.size === 0) return;
    const sorted = [...this.settled]
      .map(BigInt)
      .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    let cursor = this.highestContiguous;
    for (const off of sorted) {
      if (cursor === null) {
        const hasLowerUnsettled = [...this.unsettled].some(
          (u) => BigInt(u) < off,
        );
        if (hasLowerUnsettled) break;
        cursor = off;
        continue;
      }
      if (off === cursor + 1n) {
        cursor = off;
      } else if (off > cursor + 1n) {
        break;
      }
    }
    this.highestContiguous = cursor;
    if (cursor !== null) {
      for (const s of [...this.settled]) {
        if (BigInt(s) <= cursor) this.settled.delete(s);
      }
    }
  }
}
