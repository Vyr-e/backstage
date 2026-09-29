import { afterAll, describe, expect, test } from 'bun:test';
import { KafkaProvider } from '../src/provider/kafka';

// Needs a broker with auto.create.topics.enable=false (production default for
// many clusters). Point KAFKA_NOAUTO_BROKER at one; skipped when unreachable.
const broker = process.env.KAFKA_NOAUTO_BROKER ?? 'localhost:9094';

async function brokerUp(): Promise<boolean> {
  try {
    const { Kafka } = await import('kafkajs');
    const admin = new Kafka({
      clientId: 'ping',
      brokers: [broker],
      connectionTimeout: 2000,
      requestTimeout: 2000,
      retry: { retries: 0 },
    }).admin();
    await admin.connect();
    await admin.listTopics();
    await admin.disconnect();
    return true;
  } catch {
    return false;
  }
}

const up = await brokerUp();
const silent = { info() {}, warn() {}, error() {}, debug() {} } as any;

describe.skipIf(!up)('KafkaProvider topics without auto-create', () => {
  const provider = new KafkaProvider({
    brokers: [broker],
    prefix: `ktc-${Date.now()}`,
    partitions: 1,
    replicationFactor: 1,
  });
  const ready = provider.init({
    capabilities: { jobs: provider.jobs, topics: provider.topics },
    logger: silent,
  });

  afterAll(async () => {
    await provider.close();
  });

  test('publishing to a topic nobody created succeeds', async () => {
    await ready;
    await expect(provider.topics.publish('fresh', { n: 1 })).resolves.toBeDefined();
  }, 30_000);

  test('a group subscriber gets a message published right after subscribe resolves', async () => {
    await ready;
    const got: unknown[] = [];
    const sub = await provider.topics.subscribe(
      { topic: 'late', group: 'g', consumerId: 'c', from: 'latest' },
      async (m) => {
        got.push(m.payload);
        await m.ack();
      },
    );
    await provider.topics.publish('late', { n: 1 });
    const t0 = Date.now();
    while (got.length === 0 && Date.now() - t0 < 20_000) await Bun.sleep(100);
    await sub.stop();
    expect(got).toEqual([{ n: 1 }]);
  }, 60_000);
});
