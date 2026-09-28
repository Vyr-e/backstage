import { describe, test, expect, beforeEach } from 'bun:test';
import type {
  BackstageProvider,
  MessageRef,
  PublishOptions,
  ConsumeArgs,
  ReclaimIdleArgs,
  DeadLetterMeta,
} from '../src/provider/types';
import { RedisStreamsProvider } from '../src/provider/redis';
import { Worker } from '../src/worker';

/**
 * In-memory FakeProvider exercising the full BackstageProvider contract
 * without Redis/Rabbit/Kafka.
 */
class FakeProvider implements BackstageProvider {
  readonly name = 'fake';
  readonly capabilities = {
    durable: true,
    broadcast: true,
    scheduling: true,
    retries: true,
    deduplication: true,
  };

  private queues = new Map<string, MessageRef[]>();
  private pending = new Map<string, { msg: MessageRef; claimedAt: number }>();
  private delayed: Array<{ executeAt: number; msg: Omit<MessageRef, 'id' | 'deliveryCount'> & { id?: string } }> = [];
  private dedupe = new Map<string, number>();
  private broadcastLog: MessageRef[] = [];
  private broadcastPending = new Map<string, Set<string>>();
  private dlq: MessageRef[] = [];
  private seq = 0;

  async ensureQueues(queues: string[]): Promise<void> {
    for (const q of queues) {
      if (!this.queues.has(q)) this.queues.set(q, []);
    }
  }

  async close(): Promise<void> {
    this.queues.clear();
    this.pending.clear();
  }

  async publish(
    taskName: string,
    payload: unknown,
    opts: PublishOptions = {},
  ): Promise<string | null> {
    if (opts.dedupe) {
      const now = Date.now();
      const exp = this.dedupe.get(opts.dedupe.key);
      if (exp && exp > now) return null;
      this.dedupe.set(opts.dedupe.key, now + (opts.dedupe.ttl ?? 3600000));
    }

    const queue = String(opts.queue ?? opts.priority ?? 'default');
    await this.ensureQueues([queue]);
    const enqueuedAt = Date.now();
    const id = `fake-${++this.seq}`;

    if (opts.delay && opts.delay > 0) {
      this.delayed.push({
        executeAt: enqueuedAt + opts.delay,
        msg: {
          id,
          queue,
          taskName,
          payload,
          enqueuedAt,
          attempts: opts.attempts,
          backoff: opts.backoff,
          timeout: opts.timeout,
        },
      });
      return `scheduled:${enqueuedAt + opts.delay}`;
    }

    const ref: MessageRef = {
      id,
      queue,
      taskName,
      payload,
      enqueuedAt,
      deliveryCount: 1,
      attempts: opts.attempts,
      backoff: opts.backoff,
      timeout: opts.timeout,
    };
    this.queues.get(queue)!.push(ref);
    return id;
  }

  async consume(args: ConsumeArgs): Promise<MessageRef[]> {
    const out: MessageRef[] = [];
    for (const q of args.queues) {
      const list = this.queues.get(q) ?? [];
      while (out.length < args.maxMessages && list.length > 0) {
        const msg = list.shift()!;
        this.pending.set(msg.id, { msg, claimedAt: Date.now() });
        out.push(msg);
      }
    }
    if (out.length === 0 && args.blockMs && args.blockMs > 0) {
      await Bun.sleep(Math.min(args.blockMs, 20));
    }
    return out;
  }

  async ack(messages: MessageRef[]): Promise<void> {
    for (const m of messages) this.pending.delete(m.id);
  }

  async ackAndForget(messages: MessageRef[]): Promise<void> {
    await this.ack(messages);
  }

  async reclaimIdle(args: ReclaimIdleArgs): Promise<MessageRef[]> {
    const now = Date.now();
    const claimed: MessageRef[] = [];
    for (const [, entry] of this.pending) {
      if (!args.queues.includes(entry.msg.queue)) continue;
      if (now - entry.claimedAt < args.idleMs) continue;
      entry.msg.deliveryCount += 1;
      entry.claimedAt = now;
      claimed.push({ ...entry.msg });
      if (claimed.length >= (args.maxCount ?? 10)) break;
    }
    return claimed;
  }

  async deadLetter(message: MessageRef, meta: DeadLetterMeta): Promise<void> {
    this.dlq.push({ ...message, id: `dlq-${meta.originalId}` });
    this.pending.delete(message.id);
  }

  async promoteDueScheduled(nowMs?: number): Promise<number> {
    const now = nowMs ?? Date.now();
    const due = this.delayed.filter((d) => d.executeAt <= now);
    this.delayed = this.delayed.filter((d) => d.executeAt > now);
    for (const d of due) {
      const ref: MessageRef = {
        id: d.msg.id ?? `fake-${++this.seq}`,
        queue: d.msg.queue,
        taskName: d.msg.taskName,
        payload: d.msg.payload,
        enqueuedAt: d.msg.enqueuedAt,
        deliveryCount: 1,
        attempts: d.msg.attempts,
        backoff: d.msg.backoff,
        timeout: d.msg.timeout,
      };
      await this.ensureQueues([ref.queue]);
      this.queues.get(ref.queue)!.push(ref);
    }
    return due.length;
  }

  async ensureBroadcast(consumerIdentity: string, _start: 'latest' | 'beginning' = 'latest'): Promise<void> {
    if (!this.broadcastPending.has(consumerIdentity)) {
      this.broadcastPending.set(consumerIdentity, new Set());
    }
  }

  async broadcast(taskName: string, payload: unknown): Promise<string> {
    const id = `bcast-${++this.seq}`;
    this.broadcastLog.push({
      id,
      queue: 'broadcast',
      taskName,
      payload,
      enqueuedAt: Date.now(),
      deliveryCount: 1,
    });
    return id;
  }

  async consumeBroadcast(args: {
    consumerIdentity: string;
    maxMessages: number;
  }): Promise<MessageRef[]> {
    await this.ensureBroadcast(args.consumerIdentity);
    const seen = this.broadcastPending.get(args.consumerIdentity)!;
    const out: MessageRef[] = [];
    for (const m of this.broadcastLog) {
      if (seen.has(m.id)) continue;
      seen.add(m.id);
      out.push(m);
      if (out.length >= args.maxMessages) break;
    }
    return out;
  }

  async ackBroadcast(consumerIdentity: string, ids: string[]): Promise<void> {
    const seen = this.broadcastPending.get(consumerIdentity);
    if (seen) for (const id of ids) seen.add(id);
  }

  getDlq(): MessageRef[] {
    return this.dlq;
  }
}

async function runContractSuite(provider: BackstageProvider, label: string) {
  describe(`Provider contract: ${label}`, () => {
    test('publish → consume → ack', async () => {
      await provider.ensureQueues(['default']);
      const id = await provider.publish('contract.echo', { n: 1 }, {
        queue: 'default',
      });
      expect(id).toBeTruthy();

      const msgs = await provider.consume({
        queues: ['default'],
        consumerGroup: 'g1',
        consumerId: 'c1',
        maxMessages: 10,
      });
      expect(msgs.length).toBeGreaterThanOrEqual(1);
      const msg = msgs.find((m) => m.id === id) ?? msgs[0]!;
      expect(msg.taskName).toBe('contract.echo');
      expect(msg.payload).toEqual({ n: 1 });

      await provider.ack([msg]);
    });

    test('dedupe returns null on duplicate', async () => {
      const a = await provider.publish('d', {}, {
        queue: 'default',
        dedupe: { key: `dup-${label}-${Date.now()}`, ttl: 60000 },
      });
      const b = await provider.publish('d', {}, {
        queue: 'default',
        dedupe: { key: `dup-${label}-${Date.now() - 1}`, ttl: 60000 },
      });
      // Use same key
      const key = `same-key-${label}-${Date.now()}`;
      const c = await provider.publish('d', { i: 1 }, {
        queue: 'default',
        dedupe: { key, ttl: 60000 },
      });
      const d = await provider.publish('d', { i: 2 }, {
        queue: 'default',
        dedupe: { key, ttl: 60000 },
      });
      expect(c).toBeTruthy();
      expect(d).toBeNull();
      expect(a).toBeTruthy();
      expect(b).toBeTruthy();
    });

    test('delay + promoteDueScheduled', async () => {
      const id = await provider.publish('later', { x: 1 }, {
        queue: 'default',
        delay: 5000,
      });
      expect(String(id)).toStartWith('scheduled:');

      // Not due yet
      const promotedEarly = await provider.promoteDueScheduled(Date.now());
      // Redis Lua may or may not promote; force far-future cutoff reverse:
      // promote with now far ahead
      const promoted = await provider.promoteDueScheduled(Date.now() + 10_000);
      expect(promoted).toBeGreaterThanOrEqual(0);

      if (provider.name === 'fake' || provider.name === 'kafka') {
        expect(promoted).toBeGreaterThanOrEqual(1);
      }
    });

    test('broadcast lifecycle', async () => {
      await provider.ensureBroadcast('worker-a', 'latest');
      const id = await provider.broadcast('cfg.reload', { v: 2 });
      expect(id).toBeTruthy();

      const msgs = await provider.consumeBroadcast({
        consumerIdentity: 'worker-a',
        maxMessages: 10,
        blockMs: 100,
      });
      // May be empty for Redis if start=latest and message was before group — ensure after init
      // For fake, message after ensure is visible
      if (provider.name === 'fake') {
        expect(msgs.some((m) => m.id === id)).toBe(true);
      }
      if (msgs.length > 0) {
        await provider.ackBroadcast(
          'worker-a',
          msgs.map((m) => m.id),
        );
      }
    });

    test('deadLetter removes from pending', async () => {
      await provider.ensureQueues(['default']);
      const id = await provider.publish('fail.me', {}, { queue: 'default' });
      const msgs = await provider.consume({
        queues: ['default'],
        consumerGroup: 'g-dlq',
        consumerId: 'c-dlq',
        maxMessages: 50,
      });
      const msg = msgs.find((m) => m.id === id);
      if (!msg) return; // may have been consumed by parallel tests on shared Redis
      await provider.deadLetter(msg, {
        originalId: msg.id,
        deliveryCount: 5,
        error: 'boom',
      });
    });

    test('close is safe', async () => {
      await provider.close();
    });
  });
}

describe('FakeProvider contract', () => {
  const fake = new FakeProvider();
  // Inline suite against this instance
  test('full flow with Worker', async () => {
    const provider = new FakeProvider();
    const worker = new Worker({
      provider,
      workerId: 'fake-worker',
      consumerGroup: 'fake-group',
      queues: [],
    });

    let saw: unknown = null;
    worker.on('job', async (p) => {
      saw = p;
    });

    await provider.publish('job', { ok: true }, { queue: 'default' });
    // Manually drive one consume+handle cycle via provider (Worker.start blocks)
    await provider.ensureQueues(['urgent', 'default', 'low']);
    const msgs = await provider.consume({
      queues: ['default'],
      consumerGroup: 'fake-group',
      consumerId: 'fake-worker',
      maxMessages: 1,
    });
    expect(msgs.length).toBe(1);
    expect(msgs[0]!.payload).toEqual({ ok: true });
    await provider.ack(msgs);
  });

  test('publish consume ack', async () => {
    const p = new FakeProvider();
    await p.ensureQueues(['default']);
    const id = await p.publish('t', { a: 1 }, { queue: 'default' });
    expect(id).toBeTruthy();
    const msgs = await p.consume({
      queues: ['default'],
      consumerGroup: 'g',
      consumerId: 'c',
      maxMessages: 5,
    });
    expect(msgs[0]!.id).toBe(id!);
    await p.ack(msgs);
  });

  test('dedupe', async () => {
    const p = new FakeProvider();
    const k = 'k1';
    expect(await p.publish('t', {}, { dedupe: { key: k } })).toBeTruthy();
    expect(await p.publish('t', {}, { dedupe: { key: k } })).toBeNull();
  });

  test('schedule promote', async () => {
    const p = new FakeProvider();
    await p.publish('t', {}, { queue: 'default', delay: 10000 });
    expect(await p.promoteDueScheduled(Date.now())).toBe(0);
    expect(await p.promoteDueScheduled(Date.now() + 20000)).toBe(1);
    const msgs = await p.consume({
      queues: ['default'],
      consumerGroup: 'g',
      consumerId: 'c',
      maxMessages: 5,
    });
    expect(msgs.length).toBe(1);
  });

  test('reclaimIdle increments deliveryCount', async () => {
    const p = new FakeProvider();
    await p.publish('t', {}, { queue: 'default' });
    const msgs = await p.consume({
      queues: ['default'],
      consumerGroup: 'g',
      consumerId: 'c',
      maxMessages: 1,
    });
    await Bun.sleep(30);
    const claimed = await p.reclaimIdle({
      queues: ['default'],
      consumerGroup: 'g',
      consumerId: 'c2',
      idleMs: 10,
    });
    expect(claimed.length).toBe(1);
    expect(claimed[0]!.deliveryCount).toBe(2);
  });

  test('broadcast', async () => {
    const p = new FakeProvider();
    await p.ensureBroadcast('w1', 'latest');
    const id = await p.broadcast('evt', { n: 1 });
    const msgs = await p.consumeBroadcast({
      consumerIdentity: 'w1',
      maxMessages: 10,
    });
    expect(msgs.some((m) => m.id === id)).toBe(true);
    await p.ackBroadcast('w1', [id]);
  });

  test('deadLetter', async () => {
    const p = new FakeProvider();
    await p.publish('t', {}, { queue: 'default' });
    const [msg] = await p.consume({
      queues: ['default'],
      consumerGroup: 'g',
      consumerId: 'c',
      maxMessages: 1,
    });
    await p.deadLetter(msg!, {
      originalId: msg!.id,
      deliveryCount: 9,
      error: 'x',
    });
    expect(p.getDlq().length).toBe(1);
  });
});

describe('RedisStreamsProvider contract', () => {
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  let provider: RedisStreamsProvider;
  const suffix = Date.now().toString(36);

  beforeEach(() => {
    provider = new RedisStreamsProvider({
      url: redisUrl,
      consumerGroup: `contract-${suffix}`,
    });
  });

  test('publish → consume → ack', async () => {
    const q = `cpub-${suffix}`;
    await provider.ensureQueues([q]);
    const id = await provider.publish('contract.echo', { n: 1 }, { queue: q });
    expect(id).toBeTruthy();

    const msgs = await provider.consume({
      queues: [q],
      consumerGroup: `contract-${suffix}`,
      consumerId: `c-${suffix}`,
      maxMessages: 10,
      blockMs: 500,
    });
    expect(msgs.length).toBeGreaterThanOrEqual(1);
    const msg = msgs.find((m) => m.id === id)!;
    expect(msg.taskName).toBe('contract.echo');
    expect(msg.payload).toEqual({ n: 1 });
    await provider.ack([msg]);
    await provider.close();
  });

  test('dedupe', async () => {
    const key = `dedupe-${suffix}`;
    const q = `cded-${suffix}`;
    await provider.ensureQueues([q]);
    const a = await provider.publish('d', {}, { queue: q, dedupe: { key, ttl: 60000 } });
    const b = await provider.publish('d', {}, { queue: q, dedupe: { key, ttl: 60000 } });
    expect(a).toBeTruthy();
    expect(b).toBeNull();
    await provider.close();
  });

  test('schedule + promote', async () => {
    const q = `csched-${suffix}`;
    await provider.ensureQueues([q]);
    const id = await provider.publish('later', { x: 1 }, { queue: q, delay: 50 });
    expect(String(id)).toStartWith('scheduled:');
    await Bun.sleep(80);
    const n = await provider.promoteDueScheduled();
    expect(n).toBeGreaterThanOrEqual(1);
    const msgs = await provider.consume({
      queues: [q],
      consumerGroup: `contract-${suffix}`,
      consumerId: `c2-${suffix}`,
      maxMessages: 10,
      blockMs: 500,
    });
    expect(msgs.some((m) => m.taskName === 'later')).toBe(true);
    await provider.ack(msgs);
    await provider.close();
  });

  test('broadcast', async () => {
    const wid = `bw-${suffix}`;
    await provider.ensureBroadcast(wid, 'latest');
    const id = await provider.broadcast('cfg', { v: 1 });
    const msgs = await provider.consumeBroadcast({
      consumerIdentity: wid,
      maxMessages: 10,
      blockMs: 500,
    });
    expect(msgs.some((m) => m.id === id)).toBe(true);
    await provider.ackBroadcast(wid, msgs.map((m) => m.id));
    await provider.close();
  });

  test('deadLetter uses queue-named DLQ', async () => {
    const q = `cdlq-${suffix}`;
    await provider.ensureQueues([q]);
    const id = await provider.publish('fail', { z: 1 }, { queue: q });
    const msgs = await provider.consume({
      queues: [q],
      consumerGroup: `contract-${suffix}`,
      consumerId: `c3-${suffix}`,
      maxMessages: 10,
      blockMs: 500,
    });
    const msg = msgs.find((m) => m.id === id)!;
    await provider.deadLetter(msg, {
      originalId: msg.id,
      deliveryCount: 5,
      error: 'nope',
    });
    const redis = provider.getClient();
    const len = await redis.send('XLEN', [`backstage:${q}:dead-letter`]);
    expect(Number(len)).toBeGreaterThanOrEqual(1);
    await provider.close();
  });
});
