# Write your own Backstage provider

Backstage splits **orchestration** (retries, backoff, chaining, timeouts) from **transport**.
A provider only moves bytes: publish, push-consume with ack/retry/deadLetter, and optional
topics/delays/dedupe.

## Capabilities

| Capability | Required? | Role |
|---|---|---|
| `jobs` | yes | Queues, publish, push `consume(opts, onDelivery)` |
| `topics` | no | Named pub/sub (fan-out or group) |
| `delays` | no | Durable `schedule(job, runAt)` |
| `dedupe` | no | Atomic `claim(key, ttlMs)` |

Resolution order: explicit `capabilities.X` → `provider.X` → missing.
Missing capabilities throw `CapabilityMissingError` / `*CapabilityError` with a hint.

**Never claim a capability you fake in memory.** If Kafka has no delays, omit `delays`
and let the app plug Redis (or Postgres) delays.

## Minimal Postgres sketch (jobs only)

```ts
class PostgresJobs implements JobsCapability {
  name = 'postgres-jobs';
  async ensureQueues(queues: string[]) { /* CREATE TABLE IF NOT EXISTS … */ }
  async publish(job: OutgoingJob) { /* INSERT … RETURNING id */ }
  async consume(opts, onDelivery) {
    // Poll or LISTEN/NOTIFY; call onDelivery with ack/retry/deadLetter
    // that UPDATE row state durably before resolving.
    return { async stop() { /* cancel loop */ } };
  }
}

class PostgresProvider implements BackstageProvider {
  name = 'postgres';
  jobs = new PostgresJobs(/* pool */);
  async close() { await this.pool.end(); }
}

new Worker({ provider: new PostgresProvider({ url }) });
```

## Contract suite

```ts
import { runProviderContract } from '@vyr-e/backstage/testing';
await runProviderContract(() => new PostgresProvider({ url }));
```

```go
backstagetest.RunProviderContract(t, func() backstage.Provider {
  return postgres.New(cfg)
}, backstagetest.Options{})
```

The suite checks at-least-once redelivery, prefetch, retry delay, dead-letter with
error, delays, dedupe across instances, and topic fan-out/group/durability.
