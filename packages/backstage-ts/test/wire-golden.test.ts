/**
 * Golden wire tests — Redis key/field shapes via Worker / RedisStreamsProvider.
 * Must match pre-refactor master (except custom-queue DLQ key fix).
 */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Worker } from '../src/worker';
import { RedisStreamsProvider } from '../src/provider/redis';
import { Priority } from '../src/types';
import { deadLetterKey, streamKey, errorKey } from '../src/wire';

const PREFIX = `golden-${Date.now()}`;

function redis() {
  return new Bun.RedisClient('redis://localhost:6379');
}

function fieldMap(fields: string[]): Record<string, string> {
  const map: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) {
    const k = fields[i];
    const v = fields[i + 1];
    if (k !== undefined && v !== undefined) map[k] = v;
  }
  return map;
}

describe('Wire golden — enqueue / delay / dedupe via Worker', () => {
  let client: InstanceType<typeof Bun.RedisClient>;
  let worker: Worker;
  let provider: RedisStreamsProvider;

  beforeEach(async () => {
    client = redis();
    provider = new RedisStreamsProvider({
      redis: client,
      prefix: PREFIX,
      blockTimeout: 200,
      reclaimIntervalMs: 60_000,
    });
    worker = new Worker({
      provider,
      consumerGroup: `${PREFIX}-workers`,
      workerId: `${PREFIX}-w`,
    });
  });

  afterEach(async () => {
    await worker.stop().catch(() => {});
    const keys = (await client.send('KEYS', [`${PREFIX}*`])) as string[];
    if (keys?.length) await client.send('DEL', keys);
    try {
      client.close();
    } catch {
      /* ignore */
    }
  });

  test('immediate enqueue writes taskName, payload JSON string, enqueuedAt', async () => {
    const id = await worker.enqueue('order.process', { orderId: 'o1' });
    expect(id).toMatch(/^\d+-\d+$/);

    const entries = (await client.send('XRANGE', [
      `${PREFIX}:default`,
      '-',
      '+',
      'COUNT',
      '1',
    ])) as [string, string[]][];
    expect(entries.length).toBe(1);
    const entry = entries[0]!;
    expect(entry[0]).toBe(id!);
    const map = fieldMap(entry[1]);
    expect(map.taskName).toBe('order.process');
    expect(map.payload).toBe(JSON.stringify({ orderId: 'o1' }));
    expect(Number(map.enqueuedAt)).toBeGreaterThan(0);
    expect(map.attempts).toBeUndefined();
  });

  test('enqueue with attempts/backoff/timeout stores those fields', async () => {
    await worker.enqueue(
      't',
      { a: 1 },
      {
        attempts: 3,
        backoff: { type: 'fixed', delay: 500 },
        timeout: 2000,
        priority: Priority.URGENT,
      },
    );

    const entries = (await client.send('XRANGE', [
      `${PREFIX}:urgent`,
      '-',
      '+',
      'COUNT',
      '1',
    ])) as [string, string[]][];
    const map = fieldMap(entries[0]![1]);
    expect(map.attempts).toBe('3');
    expect(JSON.parse(map.backoff!)).toEqual({ type: 'fixed', delay: 500 });
    expect(map.timeout).toBe('2000');
  });

  test('delayed enqueue writes ZSET member with streamKey and score=executeAt', async () => {
    const before = Date.now();
    const id = await worker.enqueue('later', { x: 1 }, { delay: 60_000 });
    expect(id).toMatch(/^scheduled:\d+$/);
    const executeAt = Number(id!.split(':')[1]);
    expect(executeAt).toBeGreaterThanOrEqual(before + 59_000);

    const raw = await client.send('ZRANGE', [
      `${PREFIX}:scheduled`,
      '0',
      '-1',
      'WITHSCORES',
    ]);
    // Bun may return flat [member, score, ...] or nested [[member, score]]
    let memberStr: string;
    let score: number;
    if (Array.isArray(raw) && Array.isArray(raw[0])) {
      const row = raw[0] as [string, number];
      memberStr = row[0];
      score = Number(row[1]);
    } else if (Array.isArray(raw) && raw.length >= 2) {
      memberStr = String(raw[0]);
      score = Number(raw[1]);
    } else {
      throw new Error(`unexpected ZRANGE shape: ${JSON.stringify(raw)}`);
    }
    const member = JSON.parse(memberStr);
    expect(score).toBe(executeAt);
    expect(member.taskName).toBe('later');
    expect(member.payload).toBe(JSON.stringify({ x: 1 }));
    expect(member.streamKey).toBe(`${PREFIX}:default`);
    expect(member.priority).toBe('default');
    expect(typeof member.enqueuedAt).toBe('number');
  });

  test('dedupe uses SET NX EX on {prefix}:dedupe:{key}', async () => {
    const id1 = await worker.enqueue('t', {}, { dedupe: { key: 'k1', ttl: 5000 } });
    const id2 = await worker.enqueue('t', {}, { dedupe: { key: 'k1', ttl: 5000 } });
    expect(id1).toBeTruthy();
    expect(id2).toBeNull();

    const val = await client.send('GET', [`${PREFIX}:dedupe:k1`]);
    expect(val).toBe('1');
    const ttl = (await client.send('TTL', [`${PREFIX}:dedupe:k1`])) as number;
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(5);
  });

  test('custom queue stream key is {prefix}:{queue}', async () => {
    await worker.enqueue('n', { ok: true }, { queue: 'notifications' });
    const len = (await client.send('XLEN', [
      `${PREFIX}:notifications`,
    ])) as number;
    expect(len).toBe(1);
  });
});

describe('Wire golden — retry / dead-letter via RedisStreamsProvider', () => {
  let client: InstanceType<typeof Bun.RedisClient>;
  let provider: RedisStreamsProvider;

  beforeEach(async () => {
    client = redis();
    provider = new RedisStreamsProvider({
      redis: client,
      prefix: PREFIX + '-rd',
      blockTimeout: 100,
      reclaimIntervalMs: 50,
    });
    await provider.init({
      capabilities: {
        jobs: provider.jobs,
        delays: provider.delays,
        dedupe: provider.dedupe,
        topics: provider.topics,
      },
      logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
    });
  });

  afterEach(async () => {
    await provider.close();
    const keys = (await client.send('KEYS', [`${PREFIX}-rd*`])) as string[];
    if (keys?.length) await client.send('DEL', keys);
    try {
      client.close();
    } catch {
      /* ignore */
    }
  });

  test('retry stores error at {prefix}:error:{id} and leaves message pending', async () => {
    const prefix = PREFIX + '-rd';
    const queue = 'default';
    const id = await provider.jobs.publish({
      queue,
      taskName: 'retry.me',
      payload: { n: 1 },
      enqueuedAt: Date.now(),
      meta: {},
    });

    let retried = false;
    const sub = await provider.jobs.consume(
      {
        queues: [queue],
        group: `${prefix}-g`,
        consumerId: `${prefix}-c`,
        prefetch: 1,
        idleTimeout: 60_000,
      },
      async (d) => {
        if (d.taskName === 'retry.me' && !retried) {
          retried = true;
          await d.retry({ delayMs: 1000, error: 'boom' });
        }
      },
    );

    const deadline = Date.now() + 3000;
    while (!retried && Date.now() < deadline) await Bun.sleep(20);
    expect(retried).toBe(true);

    const stored = await client.send('GET', [errorKey(prefix, id)]);
    expect(stored).toBe('boom');

    // Still pending (not ACKed)
    const pending = await client.send('XPENDING', [
      streamKey(prefix, queue),
      `${prefix}-g`,
      '-',
      '+',
      '10',
    ]);
    expect(Array.isArray(pending) && pending.length > 0).toBe(true);

    await sub.stop();
  });

  test('dead-letter writes to {prefix}:{queue}:dead-letter then ACKs original', async () => {
    const prefix = PREFIX + '-rd';
    const queue = 'notifications';
    const id = await provider.jobs.publish({
      queue,
      taskName: 'dlq.me',
      payload: { x: true },
      enqueuedAt: Date.now(),
      meta: {},
    });

    let done = false;
    const sub = await provider.jobs.consume(
      {
        queues: [queue],
        group: `${prefix}-dlg`,
        consumerId: `${prefix}-dlc`,
        prefetch: 1,
        idleTimeout: 60_000,
      },
      async (d) => {
        if (d.taskName === 'dlq.me') {
          await d.deadLetter({ error: 'final' });
          done = true;
        }
      },
    );

    const deadline = Date.now() + 3000;
    while (!done && Date.now() < deadline) await Bun.sleep(20);
    expect(done).toBe(true);

    const dlKey = deadLetterKey(prefix, queue);
    const entries = (await client.send('XRANGE', [
      dlKey,
      '-',
      '+',
      'COUNT',
      '1',
    ])) as [string, string[]][];
    expect(entries.length).toBe(1);
    const map = fieldMap(entries[0]![1]);
    expect(map.taskName).toBe('dlq.me');
    expect(map.originalId).toBe(id);
    expect(map.error).toBe('final');
    expect(map.deliveryCount).toBeTruthy();
    expect(Number(map.deadLetteredAt)).toBeGreaterThan(0);

    // Original stream PEL should be clear for this id
    const pending = (await client.send('XPENDING', [
      streamKey(prefix, queue),
      `${prefix}-dlg`,
      '-',
      '+',
      '10',
    ])) as unknown[];
    const still = Array.isArray(pending)
      ? pending.filter((e) => Array.isArray(e) && e[0] === id)
      : [];
    expect(still.length).toBe(0);

    await sub.stop();
  });
});
