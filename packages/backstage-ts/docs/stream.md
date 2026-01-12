# Stream

The Stream class manages Redis Streams for task enqueueing across priority queues.

## Usage

```typescript
import { Stream, Priority, type RedisClient } from '@backstage/core';

const redis: RedisClient = new Bun.RedisClient('redis://localhost:6379');
const stream = new Stream(redis, 'my-consumer-group');

// Initialize (creates consumer groups)
await stream.initialize();

// Enqueue immediate task
const id = await stream.enqueue('email.send', { to: 'user@test.com' });

// Enqueue with priority
await stream.enqueue('urgent.alert', data, { priority: Priority.URGENT });

// Schedule for later
await stream.enqueue('reminder', data, { delay: 60000 }); // 60s delay
```

## Priority Levels

| Priority           | Stream Key          | Use Case                  |
| ------------------ | ------------------- | ------------------------- |
| `Priority.URGENT`  | `backstage:urgent`  | Payments, driver matching |
| `Priority.DEFAULT` | `backstage:default` | Normal tasks              |
| `Priority.LOW`     | `backstage:low`     | Analytics, cleanup        |

## Scheduled Tasks

Delayed tasks go into a Redis Sorted Set (`backstage:scheduled`) with the execution timestamp as the score:

```typescript
// Runs in 5 minutes
await stream.enqueue('cleanup', {}, { delay: 300000 });
```

The worker calls `stream.processScheduledTasks()` every second to move due tasks to their streams.

## Redis Commands Used

| Method                    | Redis Command                            |
| ------------------------- | ---------------------------------------- |
| `initialize()`            | `XGROUP CREATE ... MKSTREAM`             |
| `enqueue()`               | `XADD` (immediate) or `ZADD` (delayed)   |
| `processScheduledTasks()` | `ZRANGEBYSCORE` + `XADD` + `ZREM`        |
| `getStreamKeys()`         | Returns hardcoded keys in priority order |

## Direct Access

If you need lower-level access:

```typescript
// Get stream keys in priority order
const keys = stream.getStreamKeys();
// ['backstage:urgent', 'backstage:default', 'backstage:low']

// Process scheduled tasks manually
const count = await stream.processScheduledTasks();
console.log(`Moved ${count} tasks to streams`);
```
