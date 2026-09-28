import { describe, test, expect } from 'bun:test';
import { RabbitMQProvider } from '../src/provider/rabbitmq';
import { runProviderContract } from '../src/testing';
import { Worker } from '../src/worker';

async function assertRabbitUp(): Promise<void> {
  const p = new RabbitMQProvider({
    url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
  });
  await p.init({
    capabilities: { jobs: p.jobs, topics: p.topics },
    logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
  });
  if (!p.delays) {
    await p.close();
    throw new Error('RabbitMQ delayed plugin did not provide delays');
  }
  await p.close();
}

describe('RabbitMQProvider contract', () => {
  test(
    'passes shared contract suite against real broker',
    async () => {
      await assertRabbitUp();
      const prefix = `rabbit-${Date.now()}`;
      await runProviderContract(
        async () =>
          new RabbitMQProvider({
            url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
            prefix,
          }),
        { timeoutMs: 45_000 },
      );
    },
    { timeout: 90_000 },
  );
});

describe('RabbitMQ production fixes', () => {
  test('delays capability resolved after init (Worker start check)', async () => {
    await assertRabbitUp();
    const prefix = `cap-${Date.now()}`;
    const provider = new RabbitMQProvider({
      url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
      prefix,
    });
    const worker = new Worker({
      provider,
      consumerGroup: 'cap-g',
      workerId: 'cap-w',
    });
    // bootProvider runs in ctor; await readiness via private? start briefly
    const startP = worker.start();
    await Bun.sleep(300);
    const report = worker.capabilities();
    expect(report.delays.available).toBe(true);
    expect(report.delays.name).toBeTruthy();
    await worker.stop();
    await startP;
  }, 30_000);

  test('deadLetter publish failure does not ack (job not lost)', async () => {
    await assertRabbitUp();
    const prefix = `dlfail-${Date.now()}`;
    const p = new RabbitMQProvider({
      url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
      prefix,
    });
    await p.init({
      capabilities: { jobs: p.jobs, topics: p.topics },
      logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
    });
    const q = 'q';
    await p.jobs.ensureQueues([q]);
    await p.jobs.publish({
      queue: q,
      taskName: 't',
      payload: { n: 1 },
      enqueuedAt: Date.now(),
      meta: {},
    });

    let delivery: any = null;
    const sub = await p.jobs.consume(
      { queues: [q], group: 'g', consumerId: 'c', prefetch: 1, idleTimeout: 1000 },
      async (d) => {
        if (!delivery) delivery = d;
      },
    );
    await Bun.sleep(1500);
    expect(delivery).not.toBeNull();

    // Kill broker then attempt deadLetter
    await Bun.$`sudo docker stop bs-rabbit`.quiet();
    await Bun.sleep(1500);
    let failed = false;
    try {
      await delivery.deadLetter({ error: 'x' });
    } catch {
      failed = true;
    }
    expect(failed).toBe(true);

    await Bun.$`sudo docker start bs-rabbit`.quiet();
    await Bun.sleep(6000);
    await sub.stop();
    await p.close();

    const p2 = new RabbitMQProvider({
      url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
      prefix,
    });
    await p2.init({
      capabilities: { jobs: p2.jobs, topics: p2.topics },
      logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
    });
    let recovered = false;
    const sub2 = await p2.jobs.consume(
      { queues: [q], group: 'g2', consumerId: 'c2', prefetch: 1, idleTimeout: 1000 },
      async (d) => {
        recovered = true;
        await d.ack();
      },
    );
    const deadline = Date.now() + 25_000;
    while (!recovered && Date.now() < deadline) await Bun.sleep(200);
    await sub2.stop();
    await p2.close();
    expect(recovered).toBe(true);
  }, 90_000);

  test('reconnect after broker kill mid-consume', async () => {
    await assertRabbitUp();
    const prefix = `recon-${Date.now()}`;
    const p = new RabbitMQProvider({
      url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
      prefix,
    });
    await p.init({
      capabilities: { jobs: p.jobs, topics: p.topics },
      logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
    });
    const q = 'work';
    await p.jobs.ensureQueues([q]);
    const processed: string[] = [];
    const sub = await p.jobs.consume(
      { queues: [q], group: 'g', consumerId: 'c', prefetch: 2, idleTimeout: 1000 },
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
    while (!processed.includes('before') && Date.now() - t0 < 10_000) await Bun.sleep(100);
    expect(processed).toContain('before');

    await Bun.$`sudo docker stop bs-rabbit`.quiet();
    await Bun.sleep(2000);
    await Bun.$`sudo docker start bs-rabbit`.quiet();
    await Bun.sleep(6000);

    let pubOk = false;
    for (let i = 0; i < 5; i++) {
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
    while (!processed.includes('after') && Date.now() - t1 < 30_000) await Bun.sleep(200);
    await sub.stop();
    await p.close();
    expect(processed).toContain('after');
  }, 90_000);
});
