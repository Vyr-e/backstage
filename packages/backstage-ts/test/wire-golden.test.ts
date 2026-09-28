/**
 * Golden wire tests — capture Redis key/field shapes from current Stream/Worker.
 * After the provider refactor these must still match (except custom-queue DLQ key fix).
 */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Stream } from '../src/stream';
import { Priority } from '../src/types';

const PREFIX = `golden-${Date.now()}`;

function redis() {
  return new Bun.RedisClient('redis://localhost:6379');
}

describe('Wire golden — enqueue / delay / dedupe', () => {
  let client: InstanceType<typeof Bun.RedisClient>;
  let stream: Stream;

  beforeEach(async () => {
    client = redis();
    stream = new Stream(client, 'golden-workers', { prefix: PREFIX });
    await stream.initialize();
  });

  afterEach(async () => {
    const keys = (await client.send('KEYS', [`${PREFIX}*`])) as string[];
    if (keys?.length) await client.send('DEL', keys);
    client.close();
  });

  test('immediate enqueue writes taskName, payload JSON string, enqueuedAt', async () => {
    const id = await stream.enqueue('order.process', { orderId: 'o1' });
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
    const msgId = entry[0];
    const fields = entry[1];
    expect(msgId).toBe(id!);

    const map: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      const k = fields[i];
      const v = fields[i + 1];
      if (k !== undefined && v !== undefined) map[k] = v;
    }

    expect(map.taskName).toBe('order.process');
    expect(map.payload).toBe(JSON.stringify({ orderId: 'o1' }));
    expect(Number(map.enqueuedAt)).toBeGreaterThan(0);
    expect(map.attempts).toBeUndefined();
  });

  test('enqueue with attempts/backoff/timeout stores those fields', async () => {
    await stream.enqueue('t', { a: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 500 },
      timeout: 2000,
      priority: Priority.URGENT,
    });

    const entries = (await client.send('XRANGE', [
      `${PREFIX}:urgent`,
      '-',
      '+',
      'COUNT',
      '1',
    ])) as [string, string[]][];
    const fields = entries[0]![1];
    const map: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      const k = fields[i];
      const v = fields[i + 1];
      if (k !== undefined && v !== undefined) map[k] = v;
    }

    expect(map.attempts).toBe('3');
    expect(JSON.parse(map.backoff!)).toEqual({ type: 'fixed', delay: 500 });
    expect(map.timeout).toBe('2000');
  });

  test('delayed enqueue writes ZSET member with streamKey and score=executeAt', async () => {
    const before = Date.now();
    const id = await stream.enqueue('later', { x: 1 }, { delay: 60_000 });
    expect(id).toMatch(/^scheduled:\d+$/);
    const executeAt = Number(id!.split(':')[1]);
    expect(executeAt).toBeGreaterThanOrEqual(before + 59_000);

    const members = (await client.send('ZRANGE', [
      `${PREFIX}:scheduled`,
      '0',
      '-1',
      'WITHSCORES',
    ])) as [string, number][];
    expect(members.length).toBe(1);
    const row = members[0]!;
    const member = JSON.parse(row[0]!);
    const score = Number(row[1]!);
    expect(score).toBe(executeAt);
    expect(member.taskName).toBe('later');
    expect(member.payload).toBe(JSON.stringify({ x: 1 }));
    expect(member.streamKey).toBe(`${PREFIX}:default`);
    expect(member.priority).toBe('default');
    expect(typeof member.enqueuedAt).toBe('number');
  });

  test('dedupe uses SET NX EX on {prefix}:dedupe:{key}', async () => {
    const id1 = await stream.enqueue('t', {}, { dedupe: { key: 'k1', ttl: 5000 } });
    const id2 = await stream.enqueue('t', {}, { dedupe: { key: 'k1', ttl: 5000 } });
    expect(id1).toBeTruthy();
    expect(id2).toBeNull();

    const val = await client.send('GET', [`${PREFIX}:dedupe:k1`]);
    expect(val).toBe('1');
    const ttl = (await client.send('TTL', [`${PREFIX}:dedupe:k1`])) as number;
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(5);
  });

  test('custom queue stream key is {prefix}:{queue}', async () => {
    await stream.enqueue('n', { ok: true }, { queue: 'notifications' });
    const len = (await client.send('XLEN', [`${PREFIX}:notifications`])) as number;
    expect(len).toBe(1);
  });
});
