# Worker

The Worker consumes tasks from Redis Streams using `XREADGROUP` with at-least-once delivery.

## Basic Usage

```typescript
import { Worker, Priority } from '@backstage/core';

const worker = new Worker({
  host: 'localhost',
  port: 6379,
  consumerGroup: 'my-app-workers',
});

// Register handlers
worker.on('order.process', async (data) => {
  await processOrder(data);
});

// Start consuming
await worker.start();
```

## Configuration

```typescript
const worker = new Worker({
  host: 'localhost',
  port: 6379,
  password: 'secret',
  db: 0,
  consumerGroup: 'backstage-workers',
  workerId: 'worker-1', // Auto-generated if omitted
  blockTimeout: 5000, // XREADGROUP BLOCK ms
  idleTimeout: 30000, // Before reclaimer claims
  maxDeliveries: 5, // Then dead-letter
  gracePeriod: 30000, // Shutdown grace period
  reclaimerInterval: 10000, // Reclaimer poll interval
});
```

## Enqueue & Schedule

```typescript
// Enqueue immediate
await worker.enqueue('task.name', { foo: 'bar' });

// With priority
await worker.enqueue('task.name', data, { priority: Priority.URGENT });

// Schedule for later (delay in ms)
await worker.schedule('task.name', data, 60000);
```

## Workflow Chaining

Return an object with `next` to chain tasks:

```typescript
worker.on('step1', async (data) => {
  const result = await doStep1(data);
  return {
    next: 'step2',
    delay: 5000, // optional delay before step2
    payload: { ...data, step1Result: result },
  };
});

worker.on('step2', async (data) => {
  await doStep2(data);
  // No return = terminal step
});
```

## Graceful Shutdown

```typescript
// Handles SIGTERM, SIGINT, SIGQUIT
await worker.start();

// Or manually stop
await worker.stop();
```

The worker waits up to `gracePeriod` for active tasks to complete.

## Custom Logger

```typescript
const worker = new Worker(config, {
  silent: true, // Disable logging
  handler: (entry) => {
    // Send to external service
    datadogLogs.log(entry);
  },
});
```

## Handler Type Safety

```typescript
interface OrderPayload {
  orderId: string;
  amount: number;
}

worker.on<OrderPayload>('order.process', async (data) => {
  console.log(data.orderId); // TypeScript knows the type
});
```
