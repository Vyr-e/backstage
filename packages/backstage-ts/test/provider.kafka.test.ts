import { describe, test, expect } from 'bun:test';
import { KafkaProvider } from '../src/provider/kafka';

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

async function kafkaAvailable(): Promise<boolean> {
  if (process.env.KAFKA_BROKERS == null && process.env.FORCE_KAFKA_TESTS !== '1') {
    // Default skip unless explicitly configured — avoids noisy kafkajs retries.
    return false;
  }
  try {
    const p = new KafkaProvider({ brokers, clientId: 'backstage-health' });
    const id = await Promise.race([
      p.publish('health', {}, { queue: '__health__' }),
      Bun.sleep(1500).then(() => {
        throw new Error('timeout');
      }),
    ]);
    await p.close();
    return !!id;
  } catch {
    return false;
  }
}

const available = await kafkaAvailable();

describe.skipIf(!available)('KafkaProvider integration', () => {
  test('publish → consume → ack', async () => {
    const provider = new KafkaProvider({
      brokers,
      clientId: `bs-test-${Date.now()}`,
    });
    const q = `kpub-${Date.now()}`;
    await provider.ensureQueues([q]);
    const id = await provider.publish('kafka.echo', { n: 1 }, { queue: q });
    expect(id).toBeTruthy();

    // Give consumer loop time to start and receive
    await Bun.sleep(500);
    const msgs = await provider.consume({
      queues: [q],
      consumerGroup: `kg-${Date.now()}`,
      consumerId: 'c1',
      maxMessages: 10,
      blockMs: 3000,
    });
    expect(msgs.some((m) => m.taskName === 'kafka.echo')).toBe(true);
    await provider.ack(msgs);
    await provider.close();
  });

  test('in-provider delayed schedule', async () => {
    const provider = new KafkaProvider({
      brokers,
      clientId: `bs-delay-${Date.now()}`,
    });
    const q = `kdelay-${Date.now()}`;
    await provider.ensureQueues([q]);
    await provider.publish('later', { d: 1 }, { queue: q, delay: 100 });
    expect(await provider.promoteDueScheduled(Date.now())).toBe(0);
    await Bun.sleep(120);
    expect(await provider.promoteDueScheduled()).toBeGreaterThanOrEqual(1);
    await provider.close();
  });
});

describe.skipIf(available)('KafkaProvider (skipped — broker unavailable)', () => {
  test('documents skip', () => {
    expect(available).toBe(false);
  });
});
