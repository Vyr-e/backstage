# Backstage SDK

Background worker system using Redis Streams with at-least-once delivery.

## Features

- **Multi-Priority Queues** - urgent, default, low + custom queues
- **Job Deduplication** - prevent duplicate submissions with key + TTL
- **Workflow Chaining** - return `{ next, delay, payload }` from handlers
- **Cron Scheduling** - run tasks on cron schedules
- **PEL Reclaimer** - automatic recovery of stuck messages
- **Broadcast** - send messages to all workers
- **Graceful Shutdown** - wait for active tasks before exit
- **Dead-Letter Queue** - failed tasks after max retries

## Quick Start

```typescript
import { Worker } from '@backstage/core';

const worker = new Worker({
  host: 'localhost',
  port: 6379,
});

// Register handlers
worker.on('payment.process', async (data) => {
  console.log('Processing:', data);
  return { next: 'email.receipt', delay: 5000, payload: data };
});

worker.on('email.receipt', async (data) => {
  console.log('Sending receipt:', data);
});

await worker.start();
```

## Enqueueing Tasks

```typescript
// Immediate
await worker.enqueue('payment.process', { orderId: '123' });

// With priority
await worker.enqueue('task', data, { priority: Priority.URGENT });

// Delayed
await worker.schedule('reminder.send', data, 60000);

// Custom queue
await worker.enqueue('task', data, { queue: 'notifications' });
```

## Job Deduplication

Prevent duplicate job submissions (e.g., double-click protection):

```typescript
// First call succeeds, returns message ID
const id1 = await worker.enqueue('order.create', order, {
  dedupe: { key: `order-${order.id}`, ttl: 60000 },
});

// Second call within TTL returns null (skipped)
const id2 = await worker.enqueue('order.create', order, {
  dedupe: { key: `order-${order.id}`, ttl: 60000 },
});
// id2 === null
```

## Enhanced Job Options

```typescript
await worker.enqueue('payment.process', order, {
  attempts: 3, // Max retries for this job
  backoff: {
    // Retry strategy
    type: 'exponential', // 'fixed' or 'exponential'
    delay: 1000, // Base delay in ms
    maxDelay: 30000, // Cap for exponential
  },
  timeout: 10000, // Processing timeout in ms
});
```

## Custom Queues

```typescript
import { Queue, Stream } from '@backstage/core';

const notifQueue = new Queue('notifications', { priority: 1 });
const analyticsQueue = new Queue('analytics', { priority: 10 });

const stream = new Stream(redis, 'my-group', {
  queues: [notifQueue, analyticsQueue],
});
```

## Cron Scheduler

```typescript
import { Scheduler, CronTask } from '@backstage/core';

const scheduler = new Scheduler({
  schedules: [
    new CronTask('0 0 * * *', 'cleanup.daily'),
    new CronTask('*/5 * * * *', 'health.check'),
  ],
});

await scheduler.start();
```

## Broadcast

Send messages to all workers:

```typescript
import { Broadcast } from '@backstage/core';

const broadcast = new Broadcast({ worker });
await broadcast.initialize();
await broadcast.send('cache.invalidate', { key: 'users' });
```

## Environment Variables

| Variable                   | Default           | Description    |
| -------------------------- | ----------------- | -------------- |
| `REDIS_HOST`               | localhost         | Redis host     |
| `REDIS_PORT`               | 6379              | Redis port     |
| `REDIS_PASSWORD`           | -                 | Redis password |
| `REDIS_DB`                 | 0                 | Redis database |
| `BACKSTAGE_CONSUMER_GROUP` | backstage-workers | Consumer group |
| `BACKSTAGE_WORKER_ID`      | hostname-pid      | Worker ID      |

## API Reference

### Worker

```typescript
new Worker(config?: WorkerConfig, loggerConfig?: LoggerConfig)
worker.on<T>(taskName, handler, options?)
worker.enqueue(taskName, payload, options?)  // Returns string | null
worker.schedule(taskName, payload, delayMs, options?)
worker.start()
worker.stop()
worker.redis      // Access Redis client
worker.workerId   // Get worker ID
```

### EnqueueOptions

```typescript
{
  priority?: Priority;           // URGENT, DEFAULT, LOW
  queue?: string;                // Custom queue name
  delay?: number;                // Delay in ms
  dedupe?: { key, ttl? };        // Deduplication
  attempts?: number;             // Max retries
  backoff?: { type, delay, maxDelay? };
  timeout?: number;              // Processing timeout
}
```

### Queue

```typescript
new Queue(name, { priority, softTimeout, hardTimeout, maxRetries });
```

### CronTask

```typescript
new CronTask(schedule, taskName, queue?, args?)
// schedule: "minute hour dayOfMonth month dayOfWeek"
```

### Broadcast

```typescript
new Broadcast({ worker }); // Recommended
new Broadcast({ redis, workerId }); // Standalone
broadcast.initialize();
broadcast.send(taskName, payload);
```
