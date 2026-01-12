# Queue Utilities

Inspect and manage queues.

## Inspect Queues

```typescript
import {
  inspect,
  type RedisClient,
  Queue,
  DefaultQueues,
} from '@backstage/core';

const redis: RedisClient = new Bun.RedisClient('redis://localhost:6379');

const info = await inspect(redis, [
  DefaultQueues.URGENT,
  DefaultQueues.DEFAULT,
  DefaultQueues.LOW,
]);

console.log(info);
// {
//   queues: {
//     urgent: { pending: 5, deadLetter: 0 },
//     default: { pending: 150, deadLetter: 3 },
//     low: { pending: 1000, deadLetter: 0 },
//   },
//   scheduled: 45,
//   broadcast: 12,
//   totalPending: 1155,
//   totalDeadLetter: 3,
// }
```

## Get Pending Count

```typescript
import { numPendingTasks, Queue } from '@backstage/core';

const queue = new Queue('default');
const count = await numPendingTasks(redis, queue);
console.log(`${count} pending tasks`);
```

## Purge Queue

```typescript
import { purgeQueue, Queue } from '@backstage/core';

const queue = new Queue('default');
await purgeQueue(redis, queue);
// Deletes: backstage:default stream
```

## Purge Dead-Letter

```typescript
import { purgeDeadLetter, Queue } from '@backstage/core';

const queue = new Queue('default');
await purgeDeadLetter(redis, queue);
// Deletes: backstage:default:dead-letter stream
```

## Read Dead-Letter Messages

```typescript
// Get all dead-letter messages
const messages = await redis.send('XRANGE', [
  'backstage:default:dead-letter',
  '-',
  '+',
]);

for (const [id, fields] of messages) {
  console.log('Failed task:', id, fields);
}
```

## Retry Dead-Letter

```typescript
// Move dead-letter message back to queue
const messages = await redis.send('XRANGE', [
  'backstage:default:dead-letter',
  '-',
  '+',
  'COUNT',
  '1',
]);

if (messages.length > 0) {
  const [id, fields] = messages[0];
  const data = parseFields(fields);

  // Re-enqueue
  await stream.enqueue(data.taskName, JSON.parse(data.payload));

  // Remove from dead-letter
  await redis.send('XDEL', ['backstage:default:dead-letter', id]);
}
```

## Queue Keys Reference

| Queue    | Stream              | Dead-Letter                     | Scheduled             |
| -------- | ------------------- | ------------------------------- | --------------------- |
| urgent   | `backstage:urgent`  | `backstage:urgent:dead-letter`  | -                     |
| default  | `backstage:default` | `backstage:default:dead-letter` | -                     |
| low      | `backstage:low`     | `backstage:low:dead-letter`     | -                     |
| (global) | -                   | -                               | `backstage:scheduled` |
