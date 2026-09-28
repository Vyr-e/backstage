import { describe, test, expect } from 'bun:test';
import { Worker } from '../src/worker';
import { CapabilityMissingError } from '../src/provider';
import type { BackstageProvider, JobsCapability } from '../src/provider';

function jobsOnlyProvider(): BackstageProvider {
  const jobs: JobsCapability = {
    name: 'jobs-only',
    async ensureQueues() {},
    async publish() {
      return '1-0';
    },
    async consume() {
      return { async stop() {} };
    },
  };
  return {
    name: 'jobs-only',
    jobs,
    async close() {},
  };
}

describe('Capability resolution', () => {
  test('capabilities() reports missing topics/delays/dedupe for jobs-only provider', () => {
    const worker = new Worker({ provider: jobsOnlyProvider() });
    const report = worker.capabilities();
    expect(report.provider).toBe('jobs-only');
    expect(report.jobs.available).toBe(true);
    expect(report.topics.available).toBe(false);
    expect(report.delays.available).toBe(false);
    expect(report.dedupe.available).toBe(false);
  });

  test('enqueue with dedupe throws CapabilityMissingError', async () => {
    const worker = new Worker({ provider: jobsOnlyProvider() });
    await expect(
      worker.enqueue('t', {}, { dedupe: { key: 'k' } }),
    ).rejects.toBeInstanceOf(CapabilityMissingError);
  });

  test('schedule throws CapabilityMissingError without delays', async () => {
    const worker = new Worker({ provider: jobsOnlyProvider() });
    await expect(worker.schedule('t', {}, 1000)).rejects.toBeInstanceOf(
      CapabilityMissingError,
    );
  });

  test('publish throws CapabilityMissingError without topics', async () => {
    const worker = new Worker({ provider: jobsOnlyProvider() });
    await expect(worker.publish('t', {})).rejects.toBeInstanceOf(
      CapabilityMissingError,
    );
  });

  test('default Redis provider reports all capabilities', () => {
    const worker = new Worker({ host: 'localhost', port: 6379 });
    const report = worker.capabilities();
    expect(report.provider).toBe('redis-streams');
    expect(report.jobs.available).toBe(true);
    expect(report.topics.available).toBe(true);
    expect(report.delays.available).toBe(true);
    expect(report.dedupe.available).toBe(true);
  });
});

describe('Worker start/stop semantics', () => {
  test('second start() throws while running', async () => {
    const provider = new (await import('../src/provider/redis')).RedisStreamsProvider({
      host: 'localhost',
      port: 6379,
      prefix: `start-${Date.now()}`,
      blockTimeout: 100,
      reclaimIntervalMs: 60_000,
    });
    const worker = new Worker({
      provider,
      consumerGroup: 'start-test',
      workerId: 'start-w',
    });
    const started = worker.start();
    await Bun.sleep(80);
    await expect(worker.start()).rejects.toThrow('already running');
    await worker.stop();
    await started;
  });
});
