import { describe, test, expect } from 'bun:test';
import { RabbitMQProvider } from '../src/provider/rabbitmq';

const url = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

async function rabbitAvailable(): Promise<boolean> {
  if (process.env.RABBITMQ_URL == null && process.env.FORCE_RABBIT_TESTS !== '1') {
    return false;
  }
  try {
    const p = new RabbitMQProvider({ url });
    await p.ensureQueues(['__health__']);
    await p.close();
    return true;
  } catch {
    return false;
  }
}

const available = await rabbitAvailable();

describe.skipIf(!available)('RabbitMQProvider integration', () => {
  test('publish → consume → ack', async () => {
    const provider = new RabbitMQProvider({ url });
    const q = `rmq-${Date.now()}`;
    await provider.ensureQueues([q]);
    const id = await provider.publish('rmq.echo', { n: 1 }, { queue: q });
    expect(id).toBeTruthy();

    const msgs = await provider.consume({
      queues: [q],
      consumerGroup: 'g',
      consumerId: 'c',
      maxMessages: 5,
      blockMs: 500,
    });
    expect(msgs.some((m) => m.id === id)).toBe(true);
    await provider.ack(msgs.filter((m) => m.id === id));
    await provider.close();
  });

  test('delayed publish lands after TTL', async () => {
    const provider = new RabbitMQProvider({ url });
    const q = `rmq-delay-${Date.now()}`;
    await provider.ensureQueues([q]);
    await provider.publish('rmq.later', { d: 1 }, { queue: q, delay: 200 });
    const early = await provider.consume({
      queues: [q],
      consumerGroup: 'g',
      consumerId: 'c',
      maxMessages: 5,
      blockMs: 50,
    });
    expect(early.length).toBe(0);
    await Bun.sleep(300);
    const later = await provider.consume({
      queues: [q],
      consumerGroup: 'g',
      consumerId: 'c',
      maxMessages: 5,
      blockMs: 500,
    });
    expect(later.some((m) => m.taskName === 'rmq.later')).toBe(true);
    await provider.ack(later);
    await provider.close();
  });

  test('broadcast fanout', async () => {
    const provider = new RabbitMQProvider({ url });
    await provider.ensureBroadcast('w1', 'latest');
    await provider.ensureBroadcast('w2', 'latest');
    const id = await provider.broadcast('ping', { t: 1 });
    const a = await provider.consumeBroadcast({
      consumerIdentity: 'w1',
      maxMessages: 5,
      blockMs: 300,
    });
    const b = await provider.consumeBroadcast({
      consumerIdentity: 'w2',
      maxMessages: 5,
      blockMs: 300,
    });
    expect(a.some((m) => m.id === id)).toBe(true);
    expect(b.some((m) => m.id === id)).toBe(true);
    await provider.ackBroadcast('w1', a.map((m) => m.id));
    await provider.ackBroadcast('w2', b.map((m) => m.id));
    await provider.close();
  });
});

describe.skipIf(available)('RabbitMQProvider (skipped — broker unavailable)', () => {
  test('documents skip', () => {
    expect(available).toBe(false);
  });
});
