import { describe, expect, test } from 'bun:test';
import { Broadcast } from '../src/broadcast';
import type { RedisClient } from '../src/types';

function createMockRedis(result: unknown) {
  const calls: Array<{ command: string; args: string[] }> = [];

  return {
    client: {
      async send(command: string, args: string[]) {
        calls.push({ command, args });
        if (command === 'XREADGROUP') return result;
        if (command === 'XACK') return 1;
        if (command === 'XGROUP') return 'OK';
        return null;
      },
    } as unknown as RedisClient,
    calls,
  };
}

describe('Broadcast', () => {
  test('starts new consumer groups after existing broadcasts by default', async () => {
    const { client, calls } = createMockRedis(null);
    const broadcast = new Broadcast({
      redis: client,
      workerId: 'server-new',
      loggerConfig: { silent: true },
    });

    await broadcast.initialize();

    expect(calls[0]).toEqual({
      command: 'XGROUP',
      args: [
        'CREATE',
        'backstage:broadcast',
        'broadcast-server-new',
        '$',
        'MKSTREAM',
      ],
    });
  });

  test('can replay existing broadcasts when explicitly requested', async () => {
    const { client, calls } = createMockRedis(null);
    const broadcast = new Broadcast({
      redis: client,
      workerId: 'server-replay',
      startPosition: 'beginning',
      loggerConfig: { silent: true },
    });

    await broadcast.initialize();

    expect(calls[0]).toEqual({
      command: 'XGROUP',
      args: [
        'CREATE',
        'backstage:broadcast',
        'broadcast-server-replay',
        '0',
        'MKSTREAM',
      ],
    });
  });

  test('reads Bun object-shaped XREADGROUP results', async () => {
    const { client, calls } = createMockRedis({
      'backstage:broadcast': [
        [
          '1710000000000-0',
          [
            'taskName',
            'fare:estimate_result',
            'payload',
            JSON.stringify({ estimateId: 'est_123', economyKobo: 1200 }),
            'enqueuedAt',
            '1710000000000',
          ],
        ],
      ],
    });

    const broadcast = new Broadcast({
      redis: client,
      workerId: 'server-123',
      loggerConfig: { silent: true },
    });

    const messages = await broadcast.read(5);

    expect(messages).toHaveLength(1);
    expect(messages[0]?.id).toBe('1710000000000-0');
    expect(messages[0]?.taskName).toBe('fare:estimate_result');
    expect(messages[0]?.payload).toEqual({
      estimateId: 'est_123',
      economyKobo: 1200,
    });
    expect(messages[0]?.enqueuedAt).toBe(1710000000000);

    expect(calls[0]).toEqual({
      command: 'XREADGROUP',
      args: [
        'GROUP',
        'broadcast-server-123',
        'server-123',
        'COUNT',
        '10',
        'BLOCK',
        '5',
        'STREAMS',
        'backstage:broadcast',
        '>',
      ],
    });
  });

  test('reads standard array-shaped XREADGROUP results', async () => {
    const { client } = createMockRedis([
      [
        'backstage:broadcast',
        [
          [
            '1710000000001-0',
            [
              'taskName',
              'interop.broadcast',
              'payload',
              JSON.stringify({ invalidate: true }),
              'enqueuedAt',
              '1710000000001',
            ],
          ],
        ],
      ],
    ]);

    const broadcast = new Broadcast({
      redis: client,
      workerId: 'server-456',
      loggerConfig: { silent: true },
    });

    const messages = await broadcast.read();

    expect(messages).toHaveLength(1);
    expect(messages[0]?.taskName).toBe('interop.broadcast');
    expect(messages[0]?.payload).toEqual({ invalidate: true });
  });
});
