import { describe, test, expect } from 'bun:test';
import { KafkaProvider, ContiguousOffsetTracker } from '../src/provider/kafka';
import { runProviderContract } from '../src/testing';
import { RedisStreamsProvider } from '../src/provider/redis';

async function assertKafkaUp(): Promise<void> {
  const { Kafka } = await import('kafkajs');
  const k = new Kafka({
    clientId: 'ping',
    brokers: [process.env.KAFKA_BROKER ?? 'localhost:9092'],
    connectionTimeout: 2000,
    requestTimeout: 2000,
    retry: { retries: 1 },
  });
  const admin = k.admin();
  await admin.connect();
  await admin.listTopics();
  await admin.disconnect();
}

describe('ContiguousOffsetTracker', () => {
  test('does not advance past an unsettled offset', () => {
    const t = new ContiguousOffsetTracker();
    t.markUnsettled('1');
    t.markUnsettled('2');
    t.markUnsettled('3');
    t.markSettled('1');
    expect(t.contiguousCommitOffset()).toBe('1');
    t.markSettled('3');
    expect(t.contiguousCommitOffset()).toBe('1');
    t.markSettled('2');
    expect(t.contiguousCommitOffset()).toBe('3');
  });
});

describe('KafkaProvider contract', () => {
  test(
    'passes shared contract suite with Redis delays against real broker',
    async () => {
      await assertKafkaUp();
      const prefix = `kafka-${Date.now()}`;
      const redisDelays = new RedisStreamsProvider({
        prefix: `${prefix}-delays`,
      });
      await redisDelays.init({
        capabilities: {
          jobs: redisDelays.jobs,
          delays: redisDelays.delays,
          dedupe: redisDelays.dedupe,
          topics: redisDelays.topics,
        },
        logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
      });

      await runProviderContract(
        async () => {
          const p = new KafkaProvider({
            brokers: [process.env.KAFKA_BROKER ?? 'localhost:9092'],
            prefix,
          });
          const originalInit = p.init.bind(p);
          p.init = async (ctx) => {
            await originalInit({
              ...ctx,
              capabilities: {
                ...ctx.capabilities,
                delays: redisDelays.delays,
                dedupe: redisDelays.dedupe,
              },
            });
            (p as any).delays = redisDelays.delays;
            (p as any).dedupe = redisDelays.dedupe;
          };
          return p;
        },
        { timeoutMs: 60_000 },
      );
      await redisDelays.close();
    },
    { timeout: 120_000 },
  );
});

describe('Kafka production fixes', () => {
  test('reconnect after broker kill mid-consume', async () => {
    await assertKafkaUp();
    const prefix = `krecon-${Date.now()}`;
    const redisDelays = new RedisStreamsProvider({ prefix: `${prefix}-d` });
    await redisDelays.init({
      capabilities: {
        jobs: redisDelays.jobs,
        delays: redisDelays.delays,
      },
      logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
    });
    const p = new KafkaProvider({
      brokers: [process.env.KAFKA_BROKER ?? 'localhost:9092'],
      prefix,
    });
    await p.init({
      capabilities: {
        jobs: p.jobs,
        topics: p.topics,
        delays: redisDelays.delays,
      },
      logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
    });
    const q = 'work';
    await p.jobs.ensureQueues([q]);
    const processed: string[] = [];
    const sub = await p.jobs.consume(
      {
        queues: [q],
        group: `g-${prefix}`,
        consumerId: 'c',
        prefetch: 2,
        idleTimeout: 1000,
      },
      async (d) => {
        processed.push((d.payload as any).id);
        await d.ack();
      },
    );
    await p.jobs.publish({
      queue: q,
      taskName: 't',
      payload: { id: 'before' },
      enqueuedAt: Date.now(),
      meta: {},
    });
    const t0 = Date.now();
    while (!processed.includes('before') && Date.now() - t0 < 20_000) await Bun.sleep(100);
    expect(processed).toContain('before');

    await Bun.$`sudo docker stop bs-kafka`.quiet();
    await Bun.sleep(2000);
    await Bun.$`sudo docker start bs-kafka`.quiet();
    await Bun.sleep(10000);

    let pubOk = false;
    for (let i = 0; i < 8; i++) {
      try {
        await p.jobs.publish({
          queue: q,
          taskName: 't',
          payload: { id: 'after' },
          enqueuedAt: Date.now(),
          meta: {},
        });
        pubOk = true;
        break;
      } catch {
        await Bun.sleep(2000);
      }
    }
    expect(pubOk).toBe(true);
    const t1 = Date.now();
    while (!processed.includes('after') && Date.now() - t1 < 45_000) await Bun.sleep(200);
    await sub.stop();
    await p.close();
    await redisDelays.close();
    expect(processed).toContain('after');
  }, 120_000);
});
