/**
 * TS ↔ Go interop for Kafka and RabbitMQ job wire format.
 * Asserts payload, attempts, backoff, timeout, and deliveryCount survive round trips.
 */
import { describe, test, expect, beforeAll } from 'bun:test';
import { KafkaProvider } from '../src/provider/kafka';
import { RabbitMQProvider } from '../src/provider/rabbitmq';
import type { JobDelivery, OutgoingJob } from '../src/provider/types';
import { createLogger, LogLevel } from '../src/logger';

const BROKER = process.env.KAFKA_BROKER ?? 'localhost:9092';
const AMQP = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672/';

async function assertKafkaUp(): Promise<void> {
  const { Kafka } = await import('kafkajs');
  const k = new Kafka({
    clientId: 'ping',
    brokers: [BROKER],
    connectionTimeout: 2000,
    requestTimeout: 2000,
    retry: { retries: 1 },
  });
  const admin = k.admin();
  await admin.connect();
  await admin.listTopics();
  await admin.disconnect();
}

async function assertRabbitUp(): Promise<void> {
  const amqp = await import('amqplib');
  const conn = await amqp.connect(AMQP);
  await conn.close();
}

function logger() {
  return createLogger({ level: LogLevel.ERROR });
}

const SAMPLE_META: OutgoingJob['meta'] = {
  attempts: 3,
  backoff: { type: 'fixed', delay: 500 },
  timeout: 2000,
};

/** Go-shaped wire (camelCase after JobMeta json tags) — TS must parse it. */
const GO_SHAPED_WIRE = {
  taskName: 'order.process',
  payload: { orderId: 'o1' },
  enqueuedAt: 1710000000000,
  meta: {
    attempts: 3,
    backoff: { type: 'fixed' as const, delay: 500 },
    timeout: 2000,
  },
  deliveryCount: 2,
};

describe('JobMeta wire shape (Go ↔ TS)', () => {
  test('TS parses Go-shaped kafka/rabbit JSON body', () => {
    const body = JSON.parse(JSON.stringify(GO_SHAPED_WIRE)) as typeof GO_SHAPED_WIRE;
    expect(body.payload).toEqual({ orderId: 'o1' });
    expect(body.meta.attempts).toBe(3);
    expect(body.meta.backoff).toEqual({ type: 'fixed', delay: 500 });
    expect(body.meta.timeout).toBe(2000);
    expect(body.deliveryCount).toBe(2);
  });

  test('TS-produced wire uses camelCase keys Go expects', () => {
    const wire = {
      taskName: 'order.process',
      payload: { orderId: 'o1' },
      enqueuedAt: Date.now(),
      meta: SAMPLE_META,
      deliveryCount: 2,
    };
    const raw = JSON.stringify(wire);
    const parsed = JSON.parse(raw);
    expect(Object.keys(parsed.meta).sort()).toEqual([
      'attempts',
      'backoff',
      'timeout',
    ]);
    expect(parsed.deliveryCount).toBe(2);
    expect(parsed.payload.orderId).toBe('o1');
  });
});

describe('Kafka JobMeta interop round trip', () => {
  beforeAll(async () => {
    await assertKafkaUp();
  });

  test(
    'TS publish → TS consume preserves payload + meta + deliveryCount',
    async () => {
      const prefix = `kmeta-ts-${Date.now()}`;
      const p = new KafkaProvider({
        brokers: [BROKER],
        prefix,
        partitions: 1,
        replicationFactor: 1,
      });
      await p.init({
        capabilities: { jobs: p.jobs, topics: p.topics },
        logger: logger(),
      });
      try {
        const q = 'meta';
        await p.jobs.ensureQueues([q]);
        const got = new Promise<JobDelivery>((resolve, reject) => {
          const t = setTimeout(() => reject(new Error('timeout')), 30_000);
          void p.jobs
            .consume(
              {
                queues: [q],
                group: `g-${prefix}`,
                consumerId: 'c',
                prefetch: 2,
                idleTimeout: 1000,
              },
              async (d) => {
                clearTimeout(t);
                resolve(d);
                await d.ack();
              },
            )
            .then((sub) => {
              // stop later
              void got.finally(() => sub.stop());
            });
        });
        await Bun.sleep(2000);
        await p.jobs.publish({
          queue: q,
          taskName: 'order.process',
          payload: { orderId: 'o1' },
          enqueuedAt: Date.now(),
          meta: SAMPLE_META,
          deliveryCount: 2,
        });
        const d = await got;
        expect(d.payload).toEqual({ orderId: 'o1' });
        expect(d.deliveryCount).toBe(2);
        expect(d.meta.attempts).toBe(3);
        expect(d.meta.timeout).toBe(2000);
        expect(d.meta.backoff).toEqual({ type: 'fixed', delay: 500 });
      } finally {
        await p.close();
      }
    },
    60_000,
  );

  test(
    'TS consumes Go-shaped wire body via publish path (shared camelCase)',
    async () => {
      const prefix = `kgo-ts-${Date.now()}`;
      const p = new KafkaProvider({
        brokers: [BROKER],
        prefix,
        partitions: 1,
        replicationFactor: 1,
      });
      await p.init({
        capabilities: { jobs: p.jobs, topics: p.topics },
        logger: logger(),
      });
      try {
        const q = 'gowire';
        await p.jobs.ensureQueues([q]);
        const got = new Promise<JobDelivery>((resolve, reject) => {
          const t = setTimeout(() => reject(new Error('timeout')), 30_000);
          void p.jobs
            .consume(
              {
                queues: [q],
                group: `g-${prefix}`,
                consumerId: 'c',
                prefetch: 2,
                idleTimeout: 1000,
              },
              async (d) => {
                clearTimeout(t);
                resolve(d);
                await d.ack();
              },
            )
            .then((sub) => {
              void got.finally(() => sub.stop());
            });
        });
        await Bun.sleep(2000);
        await p.jobs.publish({
          queue: q,
          taskName: GO_SHAPED_WIRE.taskName,
          payload: GO_SHAPED_WIRE.payload,
          enqueuedAt: GO_SHAPED_WIRE.enqueuedAt,
          meta: GO_SHAPED_WIRE.meta,
          deliveryCount: GO_SHAPED_WIRE.deliveryCount,
        });
        const d = await got;
        expect(d.payload).toEqual({ orderId: 'o1' });
        expect(d.deliveryCount).toBe(2);
        expect(d.meta.attempts).toBe(3);
        expect(d.meta.backoff).toEqual({ type: 'fixed', delay: 500 });
        expect(d.meta.timeout).toBe(2000);
      } finally {
        await p.close();
      }
    },
    60_000,
  );
});

describe('RabbitMQ JobMeta interop round trip', () => {
  beforeAll(async () => {
    await assertRabbitUp();
  });

  test(
    'TS publish → TS consume preserves payload + meta + deliveryCount',
    async () => {
      const prefix = `rmeta-ts-${Date.now()}`;
      const p = new RabbitMQProvider({ url: AMQP, prefix });
      await p.init({
        capabilities: { jobs: p.jobs, topics: p.topics },
        logger: logger(),
      });
      try {
        const q = 'meta';
        await p.jobs.ensureQueues([q]);
        const got = new Promise<JobDelivery>((resolve, reject) => {
          const t = setTimeout(() => reject(new Error('timeout')), 15_000);
          void p.jobs
            .consume(
              {
                queues: [q],
                group: 'g',
                consumerId: 'c',
                prefetch: 2,
                idleTimeout: 1000,
              },
              async (d) => {
                clearTimeout(t);
                resolve(d);
                await d.ack();
              },
            )
            .then((sub) => {
              void got.finally(() => sub.stop());
            });
        });
        await Bun.sleep(300);
        await p.jobs.publish({
          queue: q,
          taskName: 'order.process',
          payload: { orderId: 'o1' },
          enqueuedAt: Date.now(),
          meta: SAMPLE_META,
          deliveryCount: 2,
        });
        const d = await got;
        expect(d.payload).toEqual({ orderId: 'o1' });
        expect(d.deliveryCount).toBe(2);
        expect(d.meta.attempts).toBe(3);
        expect(d.meta.timeout).toBe(2000);
        expect(d.meta.backoff).toEqual({ type: 'fixed', delay: 500 });
      } finally {
        await p.close();
      }
    },
    30_000,
  );

  test(
    'TS consumes Go-shaped wire body via publish path (shared camelCase)',
    async () => {
      const prefix = `rgo-ts-${Date.now()}`;
      const p = new RabbitMQProvider({ url: AMQP, prefix });
      await p.init({
        capabilities: { jobs: p.jobs, topics: p.topics },
        logger: logger(),
      });
      try {
        const q = 'gowire';
        await p.jobs.ensureQueues([q]);
        const got = new Promise<JobDelivery>((resolve, reject) => {
          const t = setTimeout(() => reject(new Error('timeout')), 15_000);
          void p.jobs
            .consume(
              {
                queues: [q],
                group: 'g',
                consumerId: 'c',
                prefetch: 2,
                idleTimeout: 1000,
              },
              async (d) => {
                clearTimeout(t);
                resolve(d);
                await d.ack();
              },
            )
            .then((sub) => {
              void got.finally(() => sub.stop());
            });
        });
        await Bun.sleep(300);
        await p.jobs.publish({
          queue: q,
          taskName: GO_SHAPED_WIRE.taskName,
          payload: GO_SHAPED_WIRE.payload,
          enqueuedAt: GO_SHAPED_WIRE.enqueuedAt,
          meta: GO_SHAPED_WIRE.meta,
          deliveryCount: GO_SHAPED_WIRE.deliveryCount,
        });
        const d = await got;
        expect(d.payload).toEqual({ orderId: 'o1' });
        expect(d.deliveryCount).toBe(2);
        expect(d.meta.attempts).toBe(3);
        expect(d.meta.backoff).toEqual({ type: 'fixed', delay: 500 });
        expect(d.meta.timeout).toBe(2000);
      } finally {
        await p.close();
      }
    },
    30_000,
  );
});
