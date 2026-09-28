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
  const timeoutMs = opts.timeoutMs ?? 8000;
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

  // --- retry increments deliveryCount; not before delayMs (soft check via reclaim) ---
  const q3 = `${queue}-retry`;
  await provider.jobs.ensureQueues([q3]);
  await provider.jobs.publish({
    queue: q3,
    taskName: 'contract.retry',
    payload: {},
    enqueuedAt: Date.now(),
    meta: { backoff: { type: 'fixed', delay: 200 } },
  });
  const counts: number[] = [];
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
      if (counts.length < 2) {
        await d.retry({ delayMs: 150, error: 'boom' });
      } else {
        await d.ack();
      }
    },
  );
  await waitUntil(() => counts.length >= 2, timeoutMs);
  await subRetry.stop();
  expect(counts[0]).toBe(1);
  expect(counts[1]).toBeGreaterThanOrEqual(2);

  // --- dead-letter after max deliveries with error ---
  const q4 = `${queue}-dlq`;
  await provider.jobs.ensureQueues([q4]);
  await provider.jobs.publish({
    queue: q4,
    taskName: 'contract.dlq',
    payload: { x: 1 },
    enqueuedAt: Date.now(),
    meta: { attempts: 1 },
  });
  let dlqError: string | undefined;
  let dlqDone = false;
  // Simulate orchestrator: dead-letter when deliveryCount > attempts
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
        dlqError = 'final-fail';
        dlqDone = true;
      } else {
        await d.retry({ delayMs: 50, error: 'temp' });
      }
    },
  );
  await waitUntil(() => dlqDone, timeoutMs);
  await subDlq.stop();
  expect(dlqError).toBe('final-fail');

  // --- dedupe exclusive across claims ---
  if (provider.dedupe) {
    const a = await provider.dedupe.claim(`dedupe-${prefix}`, 5000);
    const b = await provider.dedupe.claim(`dedupe-${prefix}`, 5000);
    expect(a).toBe(true);
    expect(b).toBe(false);
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
    // Should not arrive immediately
    await Bun.sleep(80);
    expect(delayedGot).toBe(false);
    await waitUntil(() => delayedGot, timeoutMs);
    await subD.stop();
    expect(delayedGot).toBe(true);
  }

  // --- topics fan-out ---
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
  }

  await provider.close();
}

async function waitUntil(pred: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!pred() && Date.now() < deadline) {
    await Bun.sleep(40);
  }
}
