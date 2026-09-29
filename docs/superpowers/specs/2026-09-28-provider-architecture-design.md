# Backstage Provider Architecture — Design

**Date:** 2026-09-28
**Status:** Draft for review
**Scope:** `packages/backstage-ts` and `packages/backstage-go`

## 1. Goal

Backstage is a durable worker orchestration system: you declare workers,
they process jobs, and handlers can chain work to the next worker. Today
every part of that is hard-wired to Redis Streams.

The goal is to make the transport swappable without touching app code.
An app built on Backstage (e.g. the ride-hailing system) should be able to
move from Redis Streams to RabbitMQ or Kafka by changing one line:

```ts
new Worker({ provider: new RabbitMQProvider({ url }) })
```

Handlers, `enqueue`, `schedule`, chaining, cron, and queues stay exactly
the same.

### Success criteria

1. Omitting `provider` behaves byte-for-byte like today (same Redis keys,
   fields, retry behavior). Existing apps and Go↔TS interop are untouched.
2. Swapping to RabbitMQ or Kafka requires no handler changes.
3. A third party can write a provider (e.g. Postgres) by implementing a
   documented interface and passing a shared contract test suite.
4. No capability is silently faked. Missing capabilities are reported up
   front and throw a clear error when used.
5. Every built-in provider is durable and at-least-once — nothing lives
   only in process memory.

### Non-goals

- Exactly-once delivery.
- Cross-provider migration of in-flight data (draining Redis into Kafka).
- Cancelling a running handler on hard timeout (behavior stays as today).
- Changing the handler signature.

## 2. Architecture

```
App API          worker.on / enqueue / schedule / chaining / Scheduler + CronTask
                 NEW: publish / subscribe / capabilities()
Orchestrator     handler registry · concurrency · timeouts · retry policy + backoff
(core)           chaining · dead-letter decision · shutdown · capability checks
Provider         named bundle of capabilities:
                 jobs (required) · topics · delays · dedupe (optional)
Implementations  RedisStreamsProvider (default) · RabbitMQProvider · KafkaProvider · custom
```

**Rule:** the orchestrator owns *what should happen* (retry? dead-letter?
chain?). The provider owns *how bytes move* (connections, acks,
redelivery, storage). Core never talks to a transport directly.

## 3. Public API (additive)

### TypeScript

```ts
const worker = new Worker({
  // all existing options unchanged
  provider?: BackstageProvider,            // default: RedisStreamsProvider from host/port/password/db
  capabilities?: {                         // plug in or override individual capabilities
    topics?: TopicsCapability;
    delays?: DelaysCapability;
    dedupe?: DedupeCapability;
  },
});

await worker.publish('ride.cancelled', { rideId });                   // → message id
worker.subscribe('ride.cancelled', async (payload, msg) => { ... });  // every instance
worker.subscribe('ride.cancelled', handler, { group: 'billing' });    // one instance per group
worker.subscribe(topic, handler, { from: 'earliest' });                // default 'latest'
worker.capabilities();                                                 // CapabilityReport
```

`subscribe` may be called before or after `start()`. `publish`,
`enqueue`, and `schedule` work without `start()` (producer-only
processes); providers connect lazily.

`Scheduler` accepts the same `provider` / `capabilities` options.

### Go

```go
client := backstage.New(backstage.Config{
    // existing fields unchanged
    Provider:     kafka.New(kafka.Config{Brokers: brokers}), // nil = Redis Streams
    Capabilities: backstage.Capabilities{Delays: redisDelays},
})

client.Publish(ctx, "ride.cancelled", payload)
client.Subscribe("ride.cancelled", handler)                           // every instance
client.Subscribe("ride.cancelled", handler, backstage.WithGroup("billing"))
client.Subscribe(topic, handler, backstage.FromEarliest())
client.Capabilities() // CapabilityReport
```

## 4. Capability contracts

Each capability is a small interface with a `name` so the report can say
where it came from.

```ts
interface BackstageProvider {
  name: string;
  jobs: JobsCapability;
  topics?: TopicsCapability;
  delays?: DelaysCapability;
  dedupe?: DedupeCapability;
  init?(ctx: ProviderContext): Promise<void>;
  close(): Promise<void>;
}

interface ProviderContext {
  capabilities: ResolvedCapabilities; // after plug-ins/overrides are applied
  logger: Logger;
}

interface JobsCapability {
  name: string;
  /** Other capabilities this one needs (e.g. Kafka jobs need 'delays' for retries). */
  requires?: CapabilityName[];
  ensureQueues(queues: string[]): Promise<void>;
  publish(job: OutgoingJob): Promise<string>;              // immediate delivery only
  consume(opts: ConsumeOptions, onDelivery: (d: JobDelivery) => Promise<void>): Promise<Subscription>;
}

interface OutgoingJob {
  queue: string; taskName: string; payload: unknown; enqueuedAt: number;
  meta: { attempts?: number; backoff?: BackoffConfig; timeout?: number };
  deliveryCount?: number; // set by retry re-publishes on non-Redis providers
}

interface ConsumeOptions {
  queues: string[];     // ordered highest → lowest priority
  group: string;        // consumer group (config.consumerGroup)
  consumerId: string;   // worker id
  prefetch: number;     // max unsettled deliveries outstanding at once
  idleTimeout: number;  // ms before an unsettled delivery counts as abandoned
}

interface JobDelivery {
  id: string; queue: string; taskName: string; payload: unknown;
  enqueuedAt: number; deliveryCount: number; meta: OutgoingJob['meta'];
  ack(): Promise<void>;
  retry(opts: { delayMs: number; error?: string }): Promise<void>;
  deadLetter(opts: { error?: string }): Promise<void>;
}

interface TopicsCapability {
  name: string;
  publish(topic: string, payload: unknown): Promise<string>;
  subscribe(opts: TopicSubscribeOptions, onMessage: (m: TopicDelivery) => Promise<void>): Promise<Subscription>;
}
interface TopicSubscribeOptions { topic: string; group?: string; consumerId: string; from: 'latest' | 'earliest' }
interface TopicDelivery { id: string; topic: string; payload: unknown; publishedAt: number; deliveryCount: number; ack(): Promise<void> }

interface DelaysCapability {
  name: string;
  /** Durably store the job; it must reach ctx.capabilities.jobs.publish at or after runAt. */
  schedule(job: OutgoingJob, runAt: number): Promise<string>;
}

interface DedupeCapability {
  name: string;
  /** Atomic across processes. true = first claim, false = duplicate. */
  claim(key: string, ttlMs: number): Promise<boolean>;
}

interface Subscription { stop(): Promise<void> }
```

Go mirrors these as interfaces (`Provider`, `Jobs`, `Delivery`, `Topics`,
`Delays`, `Dedupe`, `Subscription`) with `context.Context` as the first
argument and `error` returns.

## 5. Responsibilities

| Backstage core | Provider |
|---|---|
| Route by `taskName`, unknown task → `ack` + warn (as today) | Connections, queue/topic/group creation |
| Concurrency: sets `prefetch`, runs up to `concurrency` handlers | Never has more than `prefetch` unsettled deliveries outstanding |
| Hard timeout: message `timeout` field, else task `hardTimeout` | Tracks `deliveryCount` |
| On failure: `deliveryCount > maxDeliveries` → `deadLetter`, else `retry` | Implements `retry` so redelivery is **not before** `delayMs` |
| Retry delay: `computeBackoff(meta.backoff, deliveryCount)` if set, else `idleTimeout` | Redelivers unsettled deliveries after a consumer dies |
| Chaining via `jobs.publish` / `delays.schedule` | Durable storage for everything it accepts |
| Dedupe check via `dedupe.claim` before publish | — |
| Capability resolution, report, and errors | Declares honest capabilities |
| Graceful shutdown: stop subscriptions, wait `gracePeriod`, `close()` | Settles nothing on its own during shutdown |

`computeBackoff` is one shared pure function (exported) so the Redis
provider's reclaim loop and core agree exactly. Semantics match today's
`calculateBackoff` in both SDKs.

## 6. Capability resolution and errors

Resolution order per capability: explicit `capabilities.X` → `provider.X`
→ missing.

**Report** (`worker.capabilities()`, also logged at `start()`):

```
provider: kafka
  jobs    ✓ kafka
  topics  ✓ kafka
  delays  ✓ redis-delays (plugged)
  dedupe  ✗ missing — implement DedupeCapability and pass capabilities.dedupe
```

**Errors** — `CapabilityMissingError` (TS) / `*CapabilityError` wrapping
`ErrCapabilityMissing` (Go), carrying `provider`, `capability`, and a hint
naming the interface to implement and the option to pass it through.

| When | Check |
|---|---|
| `start()` | every `jobs.requires` entry resolved; `topics` present if any `subscribe` registered |
| `enqueue`/`schedule` with delay, delayed chaining | `delays` |
| `enqueue` with `dedupe` | `dedupe` |
| `publish` / `subscribe` after start | `topics` |

A delayed chain from a handler that hits a missing capability fails that
handler (normal retry path) and logs the capability error.

## 7. Durability contract (all providers)

1. **Jobs are at-least-once.** A delivery that is not `ack`ed,
   `retry`ed, or `deadLetter`ed — handler crash, process kill, lost
   connection — is redelivered after at most `idleTimeout` (or the
   transport's native equivalent).
2. **Nothing accepted is held only in memory.** `publish`, `schedule`,
   `retry`, `deadLetter`, and `claim` return only after the data is durable
   in the backing store (publisher confirms, fsync'd broker, etc.).
3. **Settlement is exact.** `ack` removes only that delivery. Kafka must
   never commit an offset past an unsettled delivery.
4. **Delays promote at-least-once:** claim → publish → remove. A crash
   may duplicate a delayed job, never drop it.
5. **Topics:** group subscriptions are durable (messages published while
   all group members are down are delivered later). Fan-out
   subscriptions receive messages published while the instance is
   subscribed; handler failures retry up to `maxDeliveries` then drop with
   an error log. Topics have no dead-letter.

## 8. Providers

### 8.1 RedisStreamsProvider (default; jobs, topics, delays, dedupe)

The current Redis logic moved behind the interface **without changing
the wire format**. Both SDKs must produce identical keys and fields.

| Concern | Redis (unchanged unless noted) |
|---|---|
| Job stream | `{prefix}:{queue}` — `urgent`/`default`/`low` or custom queue name |
| Fields | `taskName`, `payload` (JSON string), `enqueuedAt`, optional `attempts`, `backoff` (JSON), `timeout` |
| Consumer group | `config.consumerGroup`, created at `0` with `MKSTREAM` |
| Consume | `XREADGROUP` over streams in priority order; `BLOCK` only when idle; batched `XACK` (+ `XDEL` if `deleteOnAck`) |
| `retry` | Leave pending. Store `error` at `{prefix}:error:{id}` (1h TTL, as Go does today). Internal reclaim loop (`XPENDING IDLE` + `XCLAIM`) redelivers once idle ≥ `max(idleTimeout, computeBackoff(...))` |
| `deliveryCount` | PEL delivery count |
| `deadLetter` | `XADD {prefix}:{queue}:dead-letter` with `originalId`, `deliveryCount`, `deadLetteredAt`, `error`; then `XACK` on the original stream |
| Delays | ZSET `{prefix}:scheduled`, same JSON member shape and Lua promoter (`XADD` inside Lua when paired with Redis jobs). When plugged into another jobs provider: Lua moves due members to `{prefix}:scheduled:claimed`, then publishes through `ctx.capabilities.jobs`, then removes |
| Dedupe | `SET {prefix}:dedupe:{key} 1 NX EX ceil(ttl/1000)` |
| Topics (new) | Stream `{prefix}:topic:{topic}`, `XADD MAXLEN ~ topicMaxLen` (default 10 000). Fan-out group `sub:{consumerId}`; named group `grp:{group}`. Idle fan-out groups destroyed after `topicGroupIdle` (default 1h), same logic as today's broadcast cleanup |
| Escape hatches | `provider.redis` (client), `provider.scripts` (ScriptRegistry) |

**Bug fixes included (called out in changelog):**
- Custom-queue dead letters go to `{prefix}:{queue}:dead-letter` (both
  SDKs currently use the priority key `{prefix}:default:dead-letter`,
  which disagrees with `Queue.deadLetterKey` and the inspect/purge
  helpers). Priority queues are unaffected.
- Go: dead-lettering a custom-queue message `XACK`s the wrong stream, so
  the message is never removed from its PEL.
- TS: honor the per-message `timeout` field (Go already does).
- TS: record the last handler error for the dead-letter entry (Go already does).

### 8.2 RabbitMQProvider (jobs, topics, delays*; no dedupe)

| Concern | Design |
|---|---|
| Queues | Durable queue `{prefix}.{queue}`; DLQ `{prefix}.{queue}.dead-letter` |
| Publish | Confirm channel; `persistent: true`; resolve after broker confirm |
| Consume | `basic.consume` per queue with `basic.qos(prefetch)`; priority ordering is best-effort |
| `ack` | `basic.ack` |
| `retry` | Publish copy with `deliveryCount+1` via resolved `delays` (`delayMs = 0` → publish straight to the work queue), await confirm, then `ack` original |
| `deadLetter` | Publish to DLQ (confirmed), then `ack` |
| Crash redelivery | Native: unacked messages return when the channel closes. `deliveryCount` comes from the header, +1 if `redelivered` |
| Delays* | `jobs.requires = ['delays']` (retries need it). `delays` is provided via `rabbitmq_delayed_message_exchange` when the plugin is detected in `init`; if absent, startup fails unless a `delays` capability is plugged in |
| Topics | Topic exchange `{prefix}.topics`, routing key = topic. Fan-out: exclusive auto-delete queue per instance. Group: durable queue `{prefix}.topic.{topic}.{group}` |
| Dedupe | Not provided — plug in Redis or custom |

### 8.3 KafkaProvider (jobs, topics; no delays, no dedupe)

| Concern | Design |
|---|---|
| Topics per queue | `{prefix}.{queue}`; DLQ `{prefix}.{queue}.dead-letter`. Topic creation via admin client in `ensureQueues` |
| Publish | `acks: all`, idempotent producer |
| Consume | One consumer in `group`, subscribed to all queues (re-subscribe when queues are added). Deliveries dispatched concurrently up to `prefetch` |
| `ack` / commits | Per-partition offset tracker; commit only the highest **contiguous** settled offset + 1 |
| `retry` | Requires `delays` (`jobs.requires = ['delays']`): schedule copy with `deliveryCount+1`, then mark settled |
| `deadLetter` | Produce to DLQ (acks all), then mark settled |
| Crash redelivery | Native: uncommitted offsets are re-read after rebalance |
| Topics | Topic `{prefix}.topic.{topic}`. Fan-out: consumer group `{prefix}.sub.{consumerId}`; group: `{prefix}.grp.{group}` |
| Delays / dedupe | Not provided — plug in Redis or custom |
| Topic creation | Default **3 partitions**, **replicationFactor 3** (configurable; tests use 1/1) |
| Limits / non-goals | No transactional producers; does not rely on log compaction |

## 9. Backward compatibility

Released as additive minor versions: TS `1.2.0`, Go `v1.1.0`.

- All existing exports keep working. `Stream`, `Reclaimer`, `Broadcast`,
  `ScriptRegistry`, `worker.redis`, `worker.scripts`, and the
  inspect/purge utilities become thin wrappers over the Redis provider.
  Using `worker.redis` / `worker.scripts` with a non-Redis provider throws a
  clear error; their types do not change.
- Go: `client.Redis()`, `Inspect`, `Purge*`, `BroadcastListener`, and
  `client.Broadcast` keep working (Redis only).
- Legacy broadcast keeps its own stream `{prefix}:broadcast` and
  `broadcast-{workerId}` groups and is **not** bridged to topics. It is
  marked deprecated in docs in favor of `publish` / `subscribe`.
- The only behavior change is the custom-queue dead-letter key fix (§8.1).

## 10. Packaging

- **TS:** `@vyr-e/backstage` (core + Redis) plus subpath exports
  `/rabbitmq`, `/kafka`, and `/testing` (contract suite). `amqplib` and
  `kafkajs` are optional `peerDependencies`; importing a subpath without
  its peer throws an install hint.
- **Go:** core module keeps only `go-redis`. RabbitMQ and Kafka ship as
  separate modules (`packages/backstage-go/providers/rabbitmq`,
  `.../providers/kafka`) with their own `go.mod`. Contract suite lives in
  `backstagetest`.

## 11. Testing

1. **Contract suite** (TS `/testing`, Go `backstagetest`) that any
   provider runs against itself:
   - at-least-once: stop a consumer mid-handler → redelivered
   - `retry` is not delivered before `delayMs`; `deliveryCount` increments
   - `prefetch` is never exceeded
   - dead-letter after `maxDeliveries`, including `error`
   - delays survive a provider restart
   - dedupe is exclusive across two provider instances
   - topics: fan-out reaches every subscriber; group reaches exactly one
     per group; group messages survive all members being down
   - capability report and `CapabilityMissingError` behavior
2. **Wire golden tests** for Redis: recorded keys and fields for enqueue,
   delayed enqueue, retry, dead-letter, and dedupe must match the
   pre-refactor output.
3. **Go↔TS interop tests** (existing) keep passing, plus one for topics.
4. `docker-compose.test.yml` with Redis, RabbitMQ (delayed-message
   plugin), and Kafka (KRaft). Provider tests skip when a service is not
   reachable.

## 12. Implementation phases (sequential)

1. **TS core + Redis:** contracts, orchestrator refactor, Redis provider
   (wire-identical), topics, capability report and errors, compat
   wrappers, bug fixes, contract suite. Release TS 1.2.0.
2. **Go core + Redis:** parity with phase 1, interop tests. Release Go v1.1.0.
3. **RabbitMQ:** TS then Go, both passing the contract suite.
4. **Kafka:** TS then Go, both passing the contract suite.
5. **Docs:** provider guide ("write your own provider"), migration guide
   for switching an app from Redis to RabbitMQ or Kafka.

Grok's `refactor/provider-architecture` branch is not merged. Its
`wire.ts` / `wire.go` constants may be reused in phase 1.

## 13. Risks

- **Redis refactor regressions** in a production app. Mitigation: wire
  golden tests and running the existing suite unchanged against the new
  core.
- **Bun compatibility** of `amqplib` / `kafkajs`. Verify early in
  phases 3–4; if either fails, pick an alternative client before building
  on it.
- **Kafka concurrent processing within a partition** breaks per-key
  ordering. Acceptable: Backstage jobs do not promise ordering today.
