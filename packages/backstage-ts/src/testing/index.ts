import { expect } from 'bun:test';
import type { BackstageProvider, JobDelivery } from '../provider';

export interface ContractOptions {
  skipTopics?: boolean;
  skipDelays?: boolean;
  /** Max wait for a delivery (ms). */
  timeoutMs?: number;
}

/**
 * Run the shared provider contract suite against a live provider.
 * Call from provider-specific test files.
 */
export async function runProviderContract(
  createProvider: () => Promise<BackstageProvider> | BackstageProvider,
  opts: ContractOptions = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const provider = await createProvider();
  const caps = {
    jobs: provider.jobs,
    topics: provider.topics,
    delays: provider.delays,
    dedupe: provider.dedupe,
  };
  if (provider.init) {
    await provider.init({
      capabilities: caps,
      logger: {
        info() {},
        warn() {},
        error() {},
        debug() {},
      } as any,
    });
  }

  expect(provider.jobs).toBeTruthy();

  const prefix = `contract-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const queue = `q-${prefix}`;
  await provider.jobs.ensureQueues([queue]);

  // --- publish + consume + ack (at-least-once happy path) ---
  const id = await provider.jobs.publish({
    queue,
    taskName: 'contract.ping',
    payload: { ok: true },
    enqueuedAt: Date.now(),
    meta: {},
  });
  expect(typeof id).toBe('string');

  let got: JobDelivery | null = null;
  const sub = await provider.jobs.consume(
    {
      queues: [queue],
      group: `cg-${prefix}`,
      consumerId: `c-${prefix}`,
      prefetch: 2,
      idleTimeout: 500,
    },
    async (d) => {
      if (d.taskName === 'contract.ping' && !got) {
        got = d;
        await d.ack();
      }
    },
  );
  await waitUntil(() => got !== null, timeoutMs);
  expect(got).not.toBeNull();
  expect(got!.payload).toEqual({ ok: true });
  await sub.stop();

  // --- crash redelivery: stop mid-handler without ack → redelivered ---
  const qCrash = `${queue}-crash`;
  await provider.jobs.ensureQueues([qCrash]);
  await provider.jobs.publish({
    queue: qCrash,
    taskName: 'contract.crash',
    payload: { once: true },
    enqueuedAt: Date.now(),
    meta: {},
  });
  let crashCount = 0;
  const subCrash1 = await provider.jobs.consume(
    {
      queues: [qCrash],
      group: `cg-crash-${prefix}`,
      consumerId: `c-crash-a-${prefix}`,
      prefetch: 1,
      idleTimeout: 200,
    },
    async () => {
      crashCount++;
      // Simulate crash: never ack/retry/deadLetter
    },
  );
  await waitUntil(() => crashCount >= 1, timeoutMs);
  await subCrash1.stop();
  // Second consumer in same group should reclaim after idle
  let redelivered = false;
  const subCrash2 = await provider.jobs.consume(
    {
      queues: [qCrash],
      group: `cg-crash-${prefix}`,
      consumerId: `c-crash-b-${prefix}`,
      prefetch: 1,
      idleTimeout: 150,
    },
    async (d) => {
      if (d.taskName === 'contract.crash' && d.deliveryCount >= 2) {
        redelivered = true;
        await d.ack();
      } else if (d.taskName === 'contract.crash') {
        // still first delivery somehow — leave for reclaim
        await d.retry({ delayMs: 50 });
      }
    },
  );
  await waitUntil(() => redelivered, timeoutMs);
  await subCrash2.stop();
  expect(redelivered).toBe(true);

  // --- prefetch never exceeded ---
  const q2 = `${queue}-pf`;
  await provider.jobs.ensureQueues([q2]);
  for (let i = 0; i < 5; i++) {
    await provider.jobs.publish({
      queue: q2,
      taskName: 'contract.prefetch',
      payload: { i },
      enqueuedAt: Date.now(),
      meta: {},
    });
  }
  let peak = 0;
  let inFlight = 0;
  let acked = 0;
  const subPf = await provider.jobs.consume(
    {
      queues: [q2],
      group: `cg-pf-${prefix}`,
      consumerId: `c-pf-${prefix}`,
      prefetch: 2,
      idleTimeout: 2000,
    },
    async (d) => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await Bun.sleep(80);
      inFlight--;
      acked++;
      await d.ack();
    },
  );
  await waitUntil(() => acked >= 5, timeoutMs);
  await subPf.stop();
  expect(peak).toBeLessThanOrEqual(2);

  // --- retry: not before delayMs; deliveryCount increments ---
  const q3 = `${queue}-retry`;
  await provider.jobs.ensureQueues([q3]);
  await provider.jobs.publish({
    queue: q3,
    taskName: 'contract.retry',
    payload: {},
    enqueuedAt: Date.now(),
    meta: { backoff: { type: 'fixed', delay: 400 } },
  });
  const counts: number[] = [];
  const times: number[] = [];
  const subRetry = await provider.jobs.consume(
    {
      queues: [q3],
      group: `cg-retry-${prefix}`,
      consumerId: `c-retry-${prefix}`,
      prefetch: 1,
      idleTimeout: 100,
    },
    async (d) => {
      counts.push(d.deliveryCount);
      times.push(Date.now());
      if (counts.length < 2) {
        await d.retry({ delayMs: 400, error: 'boom' });
      } else {
        await d.ack();
      }
    },
  );
  await waitUntil(() => counts.length >= 2, timeoutMs);
  await subRetry.stop();
  expect(counts[0]).toBe(1);
  expect(counts[1]!).toBeGreaterThanOrEqual(2);
  // Soft check: second delivery should not arrive immediately
  expect(times[1]! - times[0]!).toBeGreaterThanOrEqual(250);

  // --- dead-letter after max deliveries; read back DLQ entry with error ---
  const q4 = `${queue}-dlq`;
  await provider.jobs.ensureQueues([q4]);
  await provider.jobs.publish({
    queue: q4,
    taskName: 'contract.dlq',
    payload: { x: 1 },
    enqueuedAt: Date.now(),
    meta: { attempts: 1 },
  });
  let dlqDone = false;
  const subDlq = await provider.jobs.consume(
    {
      queues: [q4],
      group: `cg-dlq-${prefix}`,
      consumerId: `c-dlq-${prefix}`,
      prefetch: 1,
      idleTimeout: 100,
    },
    async (d) => {
      if (d.deliveryCount > 1) {
        await d.deadLetter({ error: 'final-fail' });
        dlqDone = true;
      } else {
        await d.retry({ delayMs: 50, error: 'temp' });
      }
    },
  );
  await waitUntil(() => dlqDone, timeoutMs);
  await subDlq.stop();

  // Read back DLQ via Redis when provider exposes it
  if ('redis' in provider && (provider as any).redis) {
    const redis = (provider as any).redis as {
      send(cmd: string, args: string[]): Promise<unknown>;
    };
    const prefixGuess =
      (provider as any).prefix ??
      String(q4).split('-').slice(0, 2).join('-');
    // Prefer provider.prefix when RedisStreamsProvider
    const pfx = (provider as any).prefix as string | undefined;
    if (pfx) {
      const dlKey = `${pfx}:${q4}:dead-letter`;
      const entries = (await redis.send('XRANGE', [
        dlKey,
        '-',
        '+',
        'COUNT',
        '1',
      ])) as [string, string[]][];
      expect(entries.length).toBeGreaterThanOrEqual(1);
      const fields = entries[0]![1];
      const map: Record<string, string> = {};
      for (let i = 0; i < fields.length; i += 2) {
        map[fields[i]!] = fields[i + 1]!;
      }
      expect(map.error).toBe('final-fail');
      expect(map.taskName).toBe('contract.dlq');
      expect(map.originalId).toBeTruthy();
    }
    void prefixGuess;
  }

  // --- dedupe exclusive across two provider instances ---
  if (provider.dedupe) {
    const other = await createProvider();
    if (other.init) {
      await other.init({
        capabilities: {
          jobs: other.jobs,
          topics: other.topics,
          delays: other.delays,
          dedupe: other.dedupe,
        },
        logger: {
          info() {},
          warn() {},
          error() {},
          debug() {},
        } as any,
      });
    }
    const key = `dedupe-${prefix}`;
    const a = await provider.dedupe.claim(key, 5000);
    const b = await other.dedupe!.claim(key, 5000);
    expect(a).toBe(true);
    expect(b).toBe(false);
    await other.close();
  }

  // --- delays schedule ---
  if (!opts.skipDelays && provider.delays) {
    const q5 = `${queue}-delay`;
    await provider.jobs.ensureQueues([q5]);
    const runAt = Date.now() + 300;
    await provider.delays.schedule(
      {
        queue: q5,
        taskName: 'contract.delayed',
        payload: { late: true },
        enqueuedAt: Date.now(),
        meta: {},
      },
      runAt,
    );
    // Kick promote if Redis provider (worker normally owns the loop)
    if (typeof (provider as any).promoteCrossProvider === 'function') {
      const promo = setInterval(() => {
        (provider as any).promoteCrossProvider().catch(() => {});
      }, 50);
      let delayedGot = false;
      const subD = await provider.jobs.consume(
        {
          queues: [q5],
          group: `cg-delay-${prefix}`,
          consumerId: `c-delay-${prefix}`,
          prefetch: 1,
          idleTimeout: 500,
        },
        async (d) => {
          if (d.taskName === 'contract.delayed') {
            delayedGot = true;
            await d.ack();
          }
        },
      );
      await Bun.sleep(80);
      expect(delayedGot).toBe(false);
      await waitUntil(() => delayedGot, timeoutMs);
      clearInterval(promo);
      await subD.stop();
      expect(delayedGot).toBe(true);
    }
  }

  // --- topics fan-out + group + durability while members down ---
  if (!opts.skipTopics && provider.topics) {
    const topic = `t.${prefix}`;
    let a = 0;
    let b = 0;
    const subA = await provider.topics.subscribe(
      {
        topic,
        consumerId: `fan-a-${prefix}`,
        from: 'latest',
      },
      async (m) => {
        a++;
        await m.ack();
      },
    );
    const subB = await provider.topics.subscribe(
      {
        topic,
        consumerId: `fan-b-${prefix}`,
        from: 'latest',
      },
      async (m) => {
        b++;
        await m.ack();
      },
    );
    await Bun.sleep(100);
    await provider.topics.publish(topic, { n: 1 });
    await waitUntil(() => a >= 1 && b >= 1, timeoutMs);
    await subA.stop();
    await subB.stop();
    expect(a).toBeGreaterThanOrEqual(1);
    expect(b).toBeGreaterThanOrEqual(1);

    // group: exactly one of two group members
    let g1 = 0;
    let g2 = 0;
    const gSub1 = await provider.topics.subscribe(
      {
        topic: `${topic}.g`,
        group: `billing-${prefix}`,
        consumerId: `g1-${prefix}`,
        from: 'latest',
      },
      async (m) => {
        g1++;
        await m.ack();
      },
    );
    const gSub2 = await provider.topics.subscribe(
      {
        topic: `${topic}.g`,
        group: `billing-${prefix}`,
        consumerId: `g2-${prefix}`,
        from: 'latest',
      },
      async (m) => {
        g2++;
        await m.ack();
      },
    );
    await Bun.sleep(100);
    await provider.topics.publish(`${topic}.g`, { n: 2 });
    await waitUntil(() => g1 + g2 >= 1, timeoutMs);
    await Bun.sleep(200);
    await gSub1.stop();
    await gSub2.stop();
    expect(g1 + g2).toBe(1);

    // group durability: publish while all members down, then resume
    const durableTopic = `${topic}.durable`;
    const durableGroup = `dur-${prefix}`;
    // Create group at earliest so messages published while down are kept
    const warm = await provider.topics.subscribe(
      {
        topic: durableTopic,
        group: durableGroup,
        consumerId: `warm-${prefix}`,
        from: 'earliest',
      },
      async (m) => {
        await m.ack();
      },
    );
    await Bun.sleep(80);
    await warm.stop();
    await provider.topics.publish(durableTopic, { surviving: true });
    let gotDurable = false;
    const resume = await provider.topics.subscribe(
      {
        topic: durableTopic,
        group: durableGroup,
        consumerId: `resume-${prefix}`,
        from: 'earliest',
      },
      async (m) => {
        if ((m.payload as any)?.surviving) {
          gotDurable = true;
        }
        await m.ack();
      },
    );
    await waitUntil(() => gotDurable, timeoutMs);
    await resume.stop();
    expect(gotDurable).toBe(true);
  }

  await provider.close();
}

async function waitUntil(pred: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!pred() && Date.now() < deadline) {
    await Bun.sleep(40);
  }
}
