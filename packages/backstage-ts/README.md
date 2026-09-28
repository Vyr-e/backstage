# Backstage SDK

Transport-agnostic background worker system with at-least-once delivery.
Redis Streams is the default provider; RabbitMQ and Kafka are also supported.

## Installation

```bash
bun add @vyr-e/backstage
# Optional providers:
# bun add amqplib     # RabbitMQ
# bun add kafkajs     # Kafka
```

## Features

- **Multi-Priority Queues** - urgent, default, low + custom queues
- **Pluggable providers** - Redis Streams, RabbitMQ, Kafka
- **Job Deduplication** - prevent duplicate submissions with key + TTL
- **Workflow Chaining** - return `{ next, delay, payload }` from handlers
- **Cron Scheduling** - run tasks on cron schedules
- **Idle reclaim** - recover stuck messages with backoff
- **Batched ACKs** - high-throughput message acknowledgment
- **Broadcast** - send messages to all workers
- **Graceful Shutdown** - wait for active tasks before exit
- **Dead-Letter Queue** - failed tasks after max retries

## Quick Start (Redis — default)

```typescript
import { Worker } from '@vyr-e/backstage';

const worker = new Worker({
  host: 'localhost',
  port: 6379,
});

worker.on('payment.process', async (data) => {
  console.log('Processing:', data);
  return { next: 'email.receipt', delay: 5000, payload: data };
});

worker.on('email.receipt', async (data) => {
  console.log('Sending receipt:', data);
});

await worker.start();
```

Existing Redis users need no changes: `host` / `port` / `password` / `db`
still construct a `RedisStreamsProvider` under the hood.

## Providers

Redis Streams is the **default**. Use `host` / `port` (or `new Worker()`) —
no `provider` required. Pass `provider` only to opt into RabbitMQ or Kafka.

```typescript
import {
  Worker,
  RabbitMQProvider,
  KafkaProvider,
} from '@vyr-e/backstage';

// Default Redis (no provider)
const worker = new Worker({ host: 'localhost', port: 6379 });

// RabbitMQ (opt-in)
const rabbitWorker = new Worker({
  provider: new RabbitMQProvider({ url: 'amqp://guest:guest@localhost:5672' }),
});

// Kafka (opt-in)
const kafkaWorker = new Worker({
  provider: new KafkaProvider({ brokers: ['localhost:9092'] }),
});
```

### Capability notes

| Capability     | Redis Streams | RabbitMQ                         | Kafka                          |
|----------------|---------------|----------------------------------|--------------------------------|
| Durable work   | Streams + PEL | Durable queues                   | Topics + consumer groups       |
| Scheduling     | ZSET + Lua    | Per-message TTL + DLX delayed Q  | In-provider delayed table      |
| Broadcast      | Per-worker CG | Fanout exchange                  | Unique consumer group per worker |
| Retries        | Leave pending + reclaim | Unacked + reclaimIdle | Uncommitted + reclaimIdle |
| Dedup          | SET NX EX     | In-process TTL map               | In-process TTL map             |
| Dead-letter    | `backstage:{queue}:dead-letter` | DLX / DLQ | `{prefix}.{queue}.dead-letter` |

Redis remains the reference implementation and preserves the Go interop wire format.

### RabbitMQ setup

1. Run RabbitMQ (`docker run -p 5672:5672 rabbitmq:3-management`).
2. Pass `RabbitMQProvider` via `Worker({ provider })`.
3. Delayed jobs use a per-queue `.delayed` queue with message TTL; expired messages DLX into the work queue. `promoteDueScheduled()` is a no-op.

### Kafka setup

1. Run Kafka (KRaft or ZooKeeper) with brokers reachable from the app.
2. Pass `KafkaProvider` via `Worker({ provider })`.
3. Topics are `{prefix}.{queue}` (default prefix `backstage`). Auto-create must be enabled, or create topics ahead of time.
4. Scheduling uses an in-provider delayed table + `promoteDueScheduled()` (Worker already polls this every 1s).

## Enqueueing Tasks

```typescript
await worker.enqueue('payment.process', { orderId: '123' });
await worker.enqueue('task', data, { priority: Priority.URGENT });
await worker.schedule('reminder.send', data, 60000);
await worker.enqueue('task', data, { queue: 'notifications' });
```

## Job Deduplication

```typescript
const id1 = await worker.enqueue('order.create', order, {
  dedupe: { key: `order-${order.id}`, ttl: 60000 },
});
const id2 = await worker.enqueue('order.create', order, {
  dedupe: { key: `order-${order.id}`, ttl: 60000 },
});
// id2 === null
```

## Enhanced Job Options

```typescript
await worker.enqueue('payment.process', order, {
  attempts: 3,
  backoff: {
    type: 'exponential',
    delay: 1000,
    maxDelay: 30000,
  },
  timeout: 10000,
});
```

## Custom Queues

```typescript
import { Queue, Worker } from '@vyr-e/backstage';

const notifQueue = new Queue('notifications', { priority: 1 });

const worker = new Worker({
  queues: [notifQueue],
});
```

## Cron Scheduler

```typescript
import { Scheduler, CronTask } from '@vyr-e/backstage';

// Defaults to Redis Streams (same as Worker)
const scheduler = new Scheduler({
  host: 'localhost',
  schedules: [
    new CronTask('0 0 * * *', 'cleanup.daily'),
    new CronTask('*/5 * * * *', 'health.check'),
  ],
});

await scheduler.start();
```

## Broadcast

```typescript
import { Broadcast } from '@vyr-e/backstage';

const broadcast = new Broadcast({ worker });
await broadcast.initialize();
await broadcast.send('cache.invalidate', { key: 'users' });
```

New listeners receive broadcasts sent after they initialize. To intentionally
replay retained messages (Redis), set `startPosition: 'beginning'`.

## Migration (existing Redis users)

1. **No required code changes** — `new Worker({ host, port })` and `new Worker()` still construct Redis Streams under the hood.
2. Pass `provider` only when switching to RabbitMQ or Kafka (opt-in).
3. `worker.redis` still works when using the default Redis provider.
4. Dead-letter keys for custom queues are now `backstage:{queue}:dead-letter` (previously priority-based for Worker DLQ). Align Go consumers if they inspected DLQ by priority only.
5. `Stream` / `Reclaimer` / `Broadcast` remain exported as thin façades over the provider.

## Breaking changes

- Worker process loop no longer calls Redis commands directly (behavior unchanged for Redis).
- `worker.redis` throws if the injected provider is not Redis.
- Scheduler cron payload is published via `provider.publish` (JSON body); Redis wire fields remain `taskName` / `payload` / `enqueuedAt`.

## Environment Variables

| Variable                   | Default           | Description    |
| -------------------------- | ----------------- | -------------- |
| `REDIS_HOST`               | localhost         | Redis host     |
| `REDIS_PORT`               | 6379              | Redis port     |
| `REDIS_PASSWORD`           | -                 | Redis password |
| `REDIS_DB`                 | 0                 | Redis database |
| `BACKSTAGE_CONSUMER_GROUP` | backstage-workers | Consumer group |
| `BACKSTAGE_WORKER_ID`      | hostname-pid      | Worker ID      |
| `RABBITMQ_URL`             | -                 | RabbitMQ URL (tests) |
| `KAFKA_BROKERS`            | localhost:9092    | Kafka brokers (tests) |

## API Reference

### Worker

```typescript
new Worker(config?: WorkerOptions, loggerConfig?: LoggerConfig)
worker.on<T>(taskName, handler, options?)
worker.enqueue(taskName, payload, options?)
worker.schedule(taskName, payload, delayMs, options?)
worker.start()
worker.stop()
worker.provider  // BackstageProvider
worker.redis     // Redis client (Redis provider only)
worker.workerId
```

### BackstageProvider (contract)

```typescript
ensureQueues(queues)
publish(taskName, payload, opts?)
consume({ queues, consumerGroup, consumerId, maxMessages, blockMs? })
ack(messages) / ackAndForget?(messages)
reclaimIdle({ queues, consumerGroup, consumerId, idleMs, maxCount? })
deadLetter(message, meta)
promoteDueScheduled(nowMs?)
ensureBroadcast(consumerIdentity, start)
broadcast(taskName, payload)
consumeBroadcast({ consumerIdentity, maxMessages, blockMs? })
ackBroadcast(consumerIdentity, ids)
cleanupBroadcastGhosts?(idleMs)
close()
```
