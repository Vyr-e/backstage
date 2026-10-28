import { expect, test } from 'bun:test';
import { createLogger, LogLevel } from '../src/logger';
import { RabbitMQProvider } from '../src/provider/rabbitmq';
import { runProviderContract } from '../src/testing';

const url = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5673';

test('RabbitMQ 4.4 native delays pass the provider contract without Redis or a plugin', async () => {
  await runProviderContract(
    async () => new RabbitMQProvider({ url, nativeDelays: true, prefix: `native-ts-${Date.now()}` }),
    { timeoutMs: 45_000 },
  );
}, 90_000);

test.skipIf(!process.env.RABBITMQ_LEGACY_URL)('native delays reject an older broker instead of silently delivering immediately', async () => {
  const provider = new RabbitMQProvider({
    url: process.env.RABBITMQ_LEGACY_URL,
    nativeDelays: true,
  });
  try {
    await expect(provider.init({
      capabilities: { jobs: provider.jobs, topics: provider.topics },
      logger: createLogger({ level: LogLevel.ERROR }),
    })).rejects.toThrow('4.4+');
  } finally {
    await provider.close();
  }
});
