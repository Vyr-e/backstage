import { describe, test, expect } from 'bun:test';
import { KafkaProvider, ContiguousOffsetTracker } from '../src/provider/kafka';
import { runProviderContract } from '../src/testing';
import { RedisStreamsProvider } from '../src/provider/redis';

async function kafkaReachable(): Promise<boolean> {
  try {
    const { Kafka } = await import('kafkajs');
    const k = new Kafka({
      clientId: 'ping',
      brokers: [process.env.KAFKA_BROKER ?? 'localhost:9092'],
      connectionTimeout: 1000,
      requestTimeout: 1000,
      retry: { retries: 0 },
    });
    const admin = k.admin();
    await admin.connect();
    await admin.listTopics();
    await admin.disconnect();
    return true;
  } catch {
    return false;
  }
}

describe('ContiguousOffsetTracker', () => {
  test('does not advance past an unsettled offset', () => {
    const t = new ContiguousOffsetTracker();
    t.markUnsettled('1');
    t.markUnsettled('2');
    t.markUnsettled('3');
    t.markSettled('1');
    expect(t.contiguousCommitOffset()).toBe('1');
    t.markSettled('3'); // out of order — must not skip 2
    expect(t.contiguousCommitOffset()).toBe('1');
    t.markSettled('2');
    expect(t.contiguousCommitOffset()).toBe('3');
  });
});

describe('KafkaProvider contract', () => {
  test(
    'passes shared contract suite with Redis delays (skips if Kafka unreachable)',
    async () => {
      if (!(await kafkaReachable())) {
        console.log('SKIP: Kafka not reachable on localhost:9092');
        return;
      }
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
          // Plug Redis delays via a wrapper after init
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
        { timeoutMs: 45_000 },
      );
      await redisDelays.close();
    },
    { timeout: 90_000 },
  );
});
