import { describe, test } from 'bun:test';
import { RedisStreamsProvider } from '../src/provider/redis';
import { runProviderContract } from '../src/testing';

describe('RedisStreamsProvider contract', () => {
  test(
    'passes shared provider contract suite',
    async () => {
      await runProviderContract(
        () =>
          new RedisStreamsProvider({
            host: 'localhost',
            port: 6379,
            prefix: `rpc-${Date.now()}`,
            reclaimIntervalMs: 200,
            blockTimeout: 200,
          }),
      );
    },
    { timeout: 60_000 },
  );
});
