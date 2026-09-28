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
 */
export class KafkaProvider implements BackstageProvider {
  readonly name = 'kafka';
  readonly jobs: JobsCapability;
  readonly topics: TopicsCapability;
  // no delays, no dedupe

  private readonly brokers: string[];
  private readonly clientId: string;
  private readonly prefix: string;
  private ctx: ProviderContext | null = null;
  private Kafka!: any;
  private kafka: any;
  private producer: any;

  constructor(config: KafkaProviderConfig = {}) {
    this.brokers = config.brokers ?? ['localhost:9092'];
    this.clientId = config.clientId ?? 'backstage';
    this.prefix = config.prefix ?? 'backstage';
    this.jobs = this.createJobs();
    this.topics = this.createTopics();
  }

  async init(ctx: ProviderContext): Promise<void> {
    this.ctx = ctx;
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
    });
    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 5,
    });
    await this.producer.connect();
  }

  async close(): Promise<void> {
    try {
      await this.producer?.disconnect();
    } catch {
      /* ignore */
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
        const admin = self.kafka.admin();
        await admin.connect();
        try {
          const topics = queues.flatMap((q) => [
            { topic: self.topicForQueue(q), numPartitions: 1, replicationFactor: 1 },
            { topic: self.dlqTopic(q), numPartitions: 1, replicationFactor: 1 },
          ]);
          await admin.createTopics({ topics, waitForLeaders: true });
        } finally {
          await admin.disconnect();
        }
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
        const result = await self.producer.send({
          topic,
          acks: -1,
          messages: [{ value }],
        });
        const r0 = result?.[0];
        return r0 ? `${r0.topicName}:${r0.partition}:${r0.baseOffset}` : `kafka-${Date.now()}`;
      },
      async consume(
        opts: ConsumeOptions,
        onDelivery: (d: JobDelivery) => Promise<void>,
      ): Promise<Subscription> {
        const consumer = self.kafka.consumer({ groupId: opts.group });
        await consumer.connect();
        const topics = opts.queues.map((q) => self.topicForQueue(q));
        for (const t of topics) {
          await consumer.subscribe({ topic: t, fromBeginning: true });
        }

        // Per-partition contiguous offset tracker
        const trackers = new Map<string, ContiguousOffsetTracker>();
        let running = true;
        let inFlight = 0;

        const run = consumer.run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }: { topic: string; partition: number; message: KafkaMsg }) => {
            if (!running) return;
            while (inFlight >= opts.prefetch) {
              await Bun.sleep(10);
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
            const queue = topic.startsWith(self.prefix + '.')
              ? topic.slice(self.prefix.length + 1).replace(/\.dead-letter$/, '')
              : topic;
            // strip queue name if it was prefix.queue
            const qName = opts.queues.find((q) => self.topicForQueue(q) === topic) ?? queue;

            let body: any = {};
            try {
              body = JSON.parse(message.value?.toString() || '{}');
            } catch {
              body = {};
            }
            let deliveryCount = body.deliveryCount ?? 1;

            const settle = async () => {
              tracker!.markSettled(offset);
              const commitTo = tracker!.contiguousCommitOffset();
              if (commitTo !== null) {
                await consumer.commitOffsets([
                  { topic, partition, offset: String(BigInt(commitTo) + 1n) },
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
                if (!delays) throw new Error('kafka retry requires delays capability');
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
            try {
              await onDelivery(delivery);
            } catch {
              inFlight--;
            }
          },
        });

        return {
          async stop() {
            running = false;
            await run.catch(() => {});
            await consumer.disconnect();
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
        const result = await self.producer.send({
          topic: t,
          acks: -1,
          messages: [{ value: JSON.stringify({ payload, publishedAt: Date.now() }) }],
        });
        const r0 = result?.[0];
        return r0 ? `${r0.baseOffset}` : `topic-${Date.now()}`;
      },
      async subscribe(
        opts: TopicSubscribeOptions,
        onMessage: (m: TopicDelivery) => Promise<void>,
      ): Promise<Subscription> {
        const group = opts.group
          ? `${self.prefix}.grp.${opts.group}`
          : `${self.prefix}.sub.${opts.consumerId}`;
        const topic = `${self.prefix}.topic.${opts.topic}`;
        const consumer = self.kafka.consumer({ groupId: group });
        await consumer.connect();
        await consumer.subscribe({
          topic,
          fromBeginning: opts.from === 'earliest',
        });
        let running = true;
        const run = consumer.run({
          eachMessage: async ({ message }: { message: KafkaMsg }) => {
            if (!running) return;
            const body = JSON.parse(message.value?.toString() || '{}');
            const delivery: TopicDelivery = {
              id: message.offset,
              topic: opts.topic,
              payload: body.payload,
              publishedAt: body.publishedAt ?? Date.now(),
              deliveryCount: 1,
              async ack() {
                /* kafkajs auto-commit in this path; fine for topics */
              },
            };
            try {
              await onMessage(delivery);
              await delivery.ack();
            } catch {
              /* leave for retry */
            }
          },
        });
        return {
          async stop() {
            running = false;
            await run.catch(() => {});
            await consumer.disconnect();
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
    return this.highestContiguous === null ? null : String(this.highestContiguous);
  }

  private recompute(): void {
    if (this.settled.size === 0) return;
    const sorted = [...this.settled].map(BigInt).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    let cursor = this.highestContiguous;
    for (const off of sorted) {
      if (cursor === null) {
        // start from lowest settled only if nothing unsettled below it
        const hasLowerUnsettled = [...this.unsettled].some((u) => BigInt(u) < off);
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
    // prune settled below contiguous
    if (cursor !== null) {
      for (const s of [...this.settled]) {
        if (BigInt(s) <= cursor) this.settled.delete(s);
      }
    }
  }
}
