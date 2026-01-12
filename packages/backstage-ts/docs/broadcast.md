# Broadcast

Send messages to all workers (not just one).

## Use Case

- Cache invalidation
- Configuration reload
- Emergency stop signals

## How It Works

Normal streams use one consumer group → each message goes to one worker.

Broadcast creates a unique consumer group per worker → all workers receive the message.

## Usage

### With Worker (Recommended)

```typescript
import { Worker, Broadcast } from '@backstage/core';

const worker = new Worker({ workerId: 'worker-1' });

// Broadcast extracts redis and workerId from worker
const broadcast = new Broadcast({ worker });

await broadcast.initialize();

broadcast.onMessage(async (msg) => {
  switch (msg.taskName) {
    case 'config.reload':
      await reloadConfig();
      break;
    case 'cache.clear':
      await clearLocalCache();
      break;
  }
});

broadcast.start();
worker.start();
```

### Standalone

```typescript
import { Broadcast, type RedisClient } from '@backstage/core';

const redis: RedisClient = new Bun.RedisClient('redis://localhost:6379');

const broadcast = new Broadcast({
  redis,
  workerId: 'worker-1',
});

await broadcast.initialize();

// Subscribe to broadcasts
broadcast.onMessage(async (message) => {
  if (message.taskName === 'cache.invalidate') {
    await clearCache();
  }
});

// Start listening
broadcast.start();

// Send broadcast (from any worker)
await broadcast.send('cache.invalidate', { key: 'users' });
```

## Stream Key

All broadcasts use: `backstage:broadcast`

Each worker creates its own consumer group: `backstage:broadcast:worker-{id}`

## Cleanup

Old broadcast messages are not automatically trimmed. Add periodic cleanup:

```typescript
// Trim to last 1000 messages
await worker.redis.send('XTRIM', [
  'backstage:broadcast',
  'MAXLEN',
  '~',
  '1000',
]);
```
