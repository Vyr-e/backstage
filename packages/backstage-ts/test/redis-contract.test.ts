import { describe, test } from 'bun:test';
import { RedisStreamsProvider } from '../src/provider/redis';
import { runProviderContract } from '../src/testing';

describe('RedisStreamsProvider contract', () => {
  test(
    'passes shared provider contract suite',
    async () => {
      const prefix = `rpc-${Date.now()}`;
      await runProviderContract(
        () =>
          new RedisStreamsProvider({
            host: 'localhost',
            port: 6379,
            prefix,
            reclaimIntervalMs: 150,
            blockTimeout: 150,
          }),
      );
    },
    { timeout: 90_000 },
  );
});
