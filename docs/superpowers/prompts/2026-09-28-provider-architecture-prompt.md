# Prompt: Implement the Backstage Provider Architecture

You are working in the Backstage monorepo (`packages/backstage-ts`, a Bun
TypeScript SDK, and `packages/backstage-go`, a Go SDK). Read this whole
prompt, then read the design spec before writing any code:

`docs/superpowers/specs/2026-09-28-provider-architecture-design.md`

The spec is the source of truth for interfaces, key names, and behavior.
This prompt explains the intent, the rules, and how to work. If the two
ever disagree, stop and ask.

---

## 1. What Backstage is and why this change exists

Backstage is a **durable worker orchestration system**. Its whole job is
to spare developers from hand-building orchestration layers around their
queues. You declare workers (`worker.on('task', handler)`), push jobs
(`enqueue`, `schedule`), and handlers can pass work on to the next worker
by returning `{ next, delay, payload }`. Backstage handles everything
hard: concurrency, backpressure, timeouts, retries with backoff,
dead-lettering, chaining, cron, and graceful shutdown.

Today every piece of that is hard-wired to Redis Streams. The owner is
building a ride-hailing platform entirely on Backstage. If they ever need
RabbitMQ or Kafka, they would have to refactor the whole app. That is the
problem to solve.

**The idea, in the owner's words:** keep Redis Streams as the default,
move all the Redis logic into a Redis Streams provider, and expose an
optional `provider` option that lets you swap in the real deal (RabbitMQ,
Kafka). If you want your own transport, for example a queue built on
Postgres, you implement the `BackstageProvider` interface, declare its
capabilities, and Backstage uses it.

**The dividing line:** Backstage handles all the complex orchestration
(concurrency, timeouts, retries, backoff, chaining, dead-letter
decisions). Providers handle only transport (moving bytes, acking,
redelivery, storage). If an orchestration concern leaks into a provider,
providers drift apart and behavior differs by transport, and that
defeats the whole point.

**Capabilities are pluggable.** A provider declares what it can do
(`jobs` is required; `topics`, `delays`, and `dedupe` are optional). If
it lacks one, the user can plug in their own implementation, for example
Kafka for jobs plus a Redis delay store. Backstage reports what is
available (`worker.capabilities()`, also logged at startup). When
someone uses a capability nobody provides, it throws a clear error that
names the provider, the missing capability, and the interface to
implement to extend it.

**Broadcast becomes publish/subscribe on named topics.**
`publish(topic, payload)` sends. `subscribe(topic, handler)` delivers to
every running instance. `subscribe(topic, handler, { group })` delivers
to exactly one instance per group.

## 2. Non-negotiable rules

1. **Redis behavior is untouched.** With no `provider`, everything must
   be byte-for-byte identical to today: stream keys, field names, the
   scheduled ZSET member shape, the Lua scripts, consumer groups, and
   retry-by-reclaim. A production app and mixed Go/TS worker fleets
   depend on this wire format. The only allowed change is the
   custom-queue dead-letter bug fix described in spec §8.1.
2. **Fully backward compatible.** Every current export in both SDKs
   keeps working with the same types. This ships as TS `1.2.0` and Go
   `v1.1.0`. Do not remove, rename, or change the signature of anything
   public.
3. **Durable, at-least-once, honestly.** Never hold jobs, delays, or
   dedupe state only in process memory. Never claim a capability you
   fake. If a transport can't do something durably, don't declare it;
   let the user plug it in.
4. **The orchestrator owns policy; the provider owns transport.** See
   the responsibilities table in spec §5. Retry and dead-letter
   *decisions* and backoff *calculation* live in core. There is one shared
   `computeBackoff` function.
5. **The app-facing API doesn't change** apart from the additions in spec
   §3 (`provider`, `capabilities`, `publish`, `subscribe`,
   `capabilities()`).

## 3. Mistakes from a previous attempt: do not repeat them

A branch `origin/refactor/provider-architecture` exists from an earlier
attempt. **Do not merge or build on it.** You may copy its wire-constant
files (`src/wire.ts`, `wire.go`) if they match master exactly. Its
problems:

- It put broadcast in the same flat provider interface as jobs, as five
  methods that leaked Redis concepts (`consumerIdentity`, ghost groups).
  Topics must be their own capability with a push-style `subscribe`.
- It used a **pull** API (`consume() → batch`, `reclaimIdle()`), which
  forced RabbitMQ and Kafka into in-memory buffers. Use the **push**
  delivery API from spec §4 (`consume(opts, onDelivery)` with
  `delivery.ack/retry/deadLetter`).
- Kafka committed the *highest* offset on ack, which silently skipped
  unfinished messages. Commit only the highest *contiguous* settled
  offset.
- Kafka delays and RabbitMQ/Kafka dedupe lived in in-memory maps while
  still claiming the capability.
- RabbitMQ used `basic.get` polling with sleeps and no publisher
  confirms.
- It declared `capabilities` but core never checked them.
- It kept two code paths in the Worker (the provider plus the old
  `Stream` object).
- It made `worker.scripts` nullable, which is a type break.
- The README imported providers from the package root, which didn't
  export them.
- It added `amqplib`/`kafkajs` and the Go Kafka/RabbitMQ modules as hard
  dependencies for every user.

## 4. How to work

- **Branching.** Create a new local branch from `master`, for example
  `feat/providers-phase-1`. Never commit to `master`. Never push, open
  PRs, or delete remote branches unless the owner asks.
- **Leave the owner's changes alone.** The working tree may contain
  uncommitted changes to `logger.ts`, `logger.go`, `consumer.go`, and
  `.DS_Store`. Do not commit, revert, or overwrite them. If they block
  you, stop and ask.
- **Test first.** For every behavior, write the failing test, watch it
  fail, then implement. Before you refactor any Redis path, capture its
  current behavior in golden wire tests (spec §11.2), so the refactor is
  proven identical.
- **One phase at a time** (spec §12). At the end of each phase, stop and
  report. Do not start the next phase without the owner's go-ahead.
- **Keep files small and focused.** One capability or concern per file.
  Match the existing code style, comment density, and naming in each SDK.
- **Tooling.** TS uses Bun only (`bun test`, `bun run`, `Bun.redis`; see
  `packages/backstage-ts/CLAUDE.md`). Go uses `go test ./...` and
  `go vet ./...`. Redis-dependent tests need Redis on `localhost:6379`.
  Add `docker-compose.test.yml` (Redis, RabbitMQ with the
  delayed-message plugin, Kafka in KRaft mode) and make provider tests
  skip cleanly when a service isn't reachable.
- **Report honestly.** If a test fails or you skip something, say so
  with the output. Never describe work as done that you haven't run.

## 5. Phases and acceptance criteria

### Phase 1: TS core + Redis provider (release candidate 1.2.0)

Build:
- The capability interfaces and `BackstageProvider` (spec §4), the
  `ProviderContext`, the capability resolution and report, and
  `CapabilityMissingError` (spec §6).
- The orchestrator: refactor `Worker` so all transport goes through
  resolved capabilities. Concurrency, prefetch, hard timeout (message
  `timeout` field, then task `hardTimeout`), the retry-vs-dead-letter
  decision, backoff via the shared `computeBackoff`, chaining, dedupe
  check, and graceful shutdown all live in core.
- `RedisStreamsProvider`: move today's Redis logic behind `jobs`,
  `delays`, `dedupe`, and new `topics` exactly as specified in spec §8.1.
  This includes the internal reclaim loop for `retry` and the
  `{prefix}:error:{id}` last-error key.
- Topics on Redis (`{prefix}:topic:{topic}`, fan-out and group
  subscriptions, `MAXLEN ~`, idle fan-out group cleanup).
- `worker.publish`, `worker.subscribe`, `worker.capabilities()`, and
  `Scheduler` accepting `provider`/`capabilities`.
- Backward-compat wrappers: `Stream`, `Reclaimer`, `Broadcast`,
  `ScriptRegistry`, `worker.redis`, `worker.scripts`, and utils, all over
  the Redis provider with unchanged types. Legacy broadcast keeps
  `{prefix}:broadcast` and is not bridged to topics.
- Bug fixes (spec §8.1): the custom-queue dead-letter key, honoring the
  message `timeout`, and recording the last error on dead-letter.
- The contract test suite exported at `@vyr-e/backstage/testing`
  (spec §11.1), run against the Redis provider.

Done when:
- [ ] Golden wire tests prove the enqueue, delayed enqueue, retry,
      dead-letter, and dedupe output matches pre-refactor master (apart
      from the documented dead-letter key fix).
- [ ] The whole existing TS test suite passes unmodified, except tests
      that asserted the buggy dead-letter key.
- [ ] Existing Go↔TS interop tests pass.
- [ ] The Redis provider passes the full contract suite.
- [ ] `tsc --noEmit` is clean, and the package root exports no
      RabbitMQ/Kafka code.
- [ ] The capability report prints at startup, and every row in spec §6
      has a test for its error.

### Phase 2: Go core + Redis provider (release candidate v1.1.0)

Reach parity with phase 1 in Go, using idiomatic interfaces with
`context.Context` and `error` returns (spec §3, §4). The core module keeps
only the `go-redis` dependency. Add the Go contract suite in a
`backstagetest` package. Also fix the Go-only bug where dead-lettering a
custom-queue message acks the wrong stream.

Done when: Go golden wire tests match master, the existing Go tests
pass, the interop tests pass in both directions (including a new topics
interop test), `go vet` is clean, and the Redis provider passes
`backstagetest`.

### Phase 3: RabbitMQ provider (TS, then Go)

Follow spec §8.2. It needs publisher confirms, `basic.consume` with
`qos(prefetch)`, retry via the resolved `delays` capability,
`jobs.requires = ['delays']`, the delayed-message plugin detected in
`init`, and a topic exchange for topics. It provides no dedupe. Ship it
as the TS subpath `/rabbitmq` with `amqplib` as an optional peer
dependency, and as the Go module `providers/rabbitmq`. Verify `amqplib`
works under Bun **before** building on it.

Done when: both implementations pass the contract suite against a real
RabbitMQ, and startup without the plugin and without a plugged `delays`
fails with the capability error.

### Phase 4: Kafka provider (TS, then Go)

Follow spec §8.3. It needs `acks: all` with an idempotent producer, a
per-partition contiguous-offset commit tracker, retry and dead-letter
that mark the delivery settled only after a durable produce,
`jobs.requires = ['delays']`, and consumer groups for topics. It
provides no delays and no dedupe. Ship it as the TS subpath `/kafka`
(optional peer `kafkajs`) and the Go module `providers/kafka`. Verify
`kafkajs` works under Bun first.

Done when: both implementations pass the contract suite against real
Kafka with a Redis `delays` plugged in. Include a test that settles
offsets out of order and proves nothing is skipped after a crash.

### Phase 5: Documentation

- Update both READMEs.
- Add a "Write your own provider" guide, using Postgres as the running
  example, that walks through the interfaces, capabilities, `requires`,
  and running the contract suite.
- Add a migration guide: "switch an existing app from Redis to RabbitMQ
  or Kafka" (one config change plus the capabilities you need to plug in).
- Mark legacy broadcast as deprecated in favor of topics.

## 6. What to report at the end of each phase

- What you built, file by file, in brief.
- Test commands you ran and their real results (pass/fail counts, and
  the output of any failure).
- Anything you deviated from in the spec and why, plus open questions.
- Then stop and wait for the owner.
