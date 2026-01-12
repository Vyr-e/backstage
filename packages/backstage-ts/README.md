# Backstage SDK

Background worker system using Redis Streams with at-least-once delivery.

## Features

- **Multi-Priority Queues** - urgent, default, low priority streams
- **Workflow Chaining** - return `{ next, delay, payload }` from handlers
- **Cron Scheduling** - run tasks on cron schedules
- **PEL Reclaimer** - automatic recovery of stuck messages
- **Broadcast** - send messages to all workers
- **Graceful Shutdown** - wait for active tasks before exit
- **Dead-Letter Queue** - failed tasks after max retries

## Quick Start

```typescript
import { Worker, Queue, CronTask, Scheduler } from '@backstage/core';

// Create a worker
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

// Start
await worker.start();
```

## Enqueueing Tasks

```typescript
// Immediate
await worker.enqueue('payment.process', { orderId: '123' });

// Delayed
await worker.schedule('reminder.send', data, 60000);
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

## Queue Utilities

```typescript
import { inspect, purgeQueue, numPendingTasks } from '@backstage/core';

const info = await inspect(redis, queues);
console.log(info.totalPending);

await purgeQueue(redis, queue);
```

## Docker

```bash
docker build -t backstage-worker .
docker run -e REDIS_HOST=redis backstage-worker
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
new Worker(config?: WorkerConfig)
worker.on(taskName, handler, options?)
worker.enqueue(taskName, payload, options?)
worker.schedule(taskName, payload, delayMs)
worker.start()
worker.stop()
```

### Queue

```typescript
new Queue(name, { priority, softTimeout, hardTimeout, maxRetries });
```

### Task

```typescript
const task = new Task(handler, { name, queue, maxRetries });
task.enqueue(data);
task.enqueueAfterDelay(delayMs, data);
task.broadcast(data);
```

### CronTask

```typescript
new CronTask(schedule, taskName, queue?, args?)
// schedule: "minute hour dayOfMonth month dayOfWeek"
```

### Scheduler

```typescript
new Scheduler({ schedules, queues, logLevel });
scheduler.start();
scheduler.stop();
```
