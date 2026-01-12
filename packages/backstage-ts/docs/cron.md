# Cron Scheduler

Run tasks on cron schedules using `CronTask` and `Scheduler`.

## Basic Usage

```typescript
import { Scheduler, CronTask } from '@backstage/core';

const scheduler = new Scheduler({
  host: 'localhost',
  port: 6379,
  schedules: [
    new CronTask('0 0 * * *', 'cleanup.daily'), // Midnight
    new CronTask('*/5 * * * *', 'health.check'), // Every 5 min
    new CronTask('0 9 * * 1-5', 'report.weekday'), // 9am weekdays
  ],
});

await scheduler.start();
```

## Cron Syntax

```
┌───────────── minute (0-59)
│ ┌─────────── hour (0-23)
│ │ ┌───────── day of month (1-31)
│ │ │ ┌─────── month (1-12)
│ │ │ │ ┌───── day of week (0-6, 0=Sunday)
│ │ │ │ │
* * * * *
```

## Examples

| Schedule         | Meaning           |
| ---------------- | ----------------- |
| `* * * * *`      | Every minute      |
| `*/5 * * * *`    | Every 5 minutes   |
| `0 * * * *`      | Every hour        |
| `0 0 * * *`      | Midnight daily    |
| `0 9-17 * * 1-5` | 9am-5pm, weekdays |
| `0 0 1 * *`      | First of month    |
| `30 4 * * 0`     | 4:30am Sundays    |

## CronTask with Queue

```typescript
import { CronTask, Queue } from '@backstage/core';

const urgentQueue = new Queue('urgent', { priority: 1 });

const task = new CronTask(
  '*/1 * * * *', // Every minute
  'urgent.check', // Task name
  urgentQueue, // Target queue
  { type: 'health' } // Args passed to handler
);
```

## Manual Next Run

```typescript
const task = new CronTask('0 9 * * *', 'daily.report');

// Get next execution time
const nextRun = task.getNextRun(new Date());
console.log(`Next run: ${nextRun}`);

// Mark as run (updates internal state)
task.markRun(new Date());
```

## Scheduler with Worker

Scheduler and Worker are separate processes:

```typescript
// scheduler.ts
const scheduler = new Scheduler({ schedules: [...] });
scheduler.start();

// worker.ts
const worker = new Worker();
worker.on('cleanup.daily', async () => { ... });
worker.on('health.check', async () => { ... });
worker.start();
```

The scheduler enqueues tasks to Redis; workers consume them.
