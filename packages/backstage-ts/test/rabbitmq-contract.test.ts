import { describe, test } from 'bun:test';
import { RabbitMQProvider } from '../src/provider/rabbitmq';
import { runProviderContract } from '../src/testing';
import { RedisStreamsProvider } from '../src/provider/redis';

async function rabbitReachable(): Promise<boolean> {
  try {
    const p = new RabbitMQProvider({ url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672' });
    await p.init({
      capabilities: { jobs: p.jobs, topics: p.topics },
      logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
    });
    await p.close();
    return true;
  } catch {
    return false;
  }
}

describe('RabbitMQProvider contract', () => {
  test(
    'passes shared contract suite (skips if RabbitMQ unreachable)',
    async () => {
      if (!(await rabbitReachable())) {
        console.log('SKIP: RabbitMQ not reachable on localhost:5672');
        return;
      }
      const prefix = `rabbit-${Date.now()}`;
      const redisDelays = new RedisStreamsProvider({
        host: 'localhost',
        port: 6379,
        prefix: `${prefix}-delays`,
      });
      await runProviderContract(
        async () => {
          const p = new RabbitMQProvider({
            url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
            prefix,
          });
          // delays plugged via init detection or override — contract uses provider.delays
          await redisDelays.init({
            capabilities: {
              jobs: redisDelays.jobs,
              delays: redisDelays.delays,
              dedupe: redisDelays.dedupe,
              topics: redisDelays.topics,
            },
            logger: { info() {}, warn() {}, error() {}, debug() {} } as any,
          });
          return p;
        },
        { timeoutMs: 30_000 },
      );
      await redisDelays.close();
    },
    { timeout: 60_000 },
  );
});
