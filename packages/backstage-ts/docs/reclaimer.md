# Reclaimer

Recovers stuck messages from crashed workers using Redis Stream's Pending Entries List (PEL).

## How It Works

1. Worker A reads message → added to PEL
2. Worker A crashes before `XACK` → message stuck in PEL
3. Reclaimer (on Worker B) finds idle messages with `XPENDING`
4. Reclaimer claims them with `XCLAIM`
5. Worker B reprocesses

## Automatic (via Worker)

The Worker runs the reclaimer automatically:

```typescript
const worker = new Worker({
  idleTimeout: 30000, // Claim after 30s idle
  maxDeliveries: 5, // Dead-letter after 5 attempts
  reclaimerInterval: 10000, // Check every 10s
});
```

## Manual Usage

```typescript
import { Reclaimer, type RedisClient } from '@backstage/core';

const reclaimer = new Reclaimer(
  redis,
  'backstage-workers', // Consumer group
  'worker-2', // This worker's ID
  30000, // Idle timeout (ms)
  5 // Max deliveries
);

await reclaimer.initialize(['backstage:urgent', 'backstage:default']);

// Reclaim idle messages
const claimed = await reclaimer.reclaimIdleMessages('backstage:default');

for (const message of claimed) {
  if (message.deliveryCount > 5) {
    // Move to dead-letter
  } else {
    // Reprocess
  }
}
```

## Message Properties

Claimed messages include delivery count:

```typescript
interface StreamMessage {
  id: string; // Redis message ID
  taskName: string;
  payload: unknown;
  deliveryCount: number; // How many times delivered
  enqueuedAt: number;
}
```

## Dead-Letter Queue

Messages exceeding `maxDeliveries` go to dead-letter:

```
backstage:urgent:dead-letter
backstage:default:dead-letter
backstage:low:dead-letter
```

Dead-letter entries include:

- `originalId` - Original message ID
- `deliveryCount` - Number of attempts
- `deadLetteredAt` - Timestamp

## Tuning

| Parameter           | Low Value                  | High Value                  |
| ------------------- | -------------------------- | --------------------------- |
| `idleTimeout`       | Fast recovery, more claims | Slow recovery, fewer claims |
| `reclaimerInterval` | More Redis traffic         | Slower detection            |
| `maxDeliveries`     | Quick dead-letter          | More retries                |
