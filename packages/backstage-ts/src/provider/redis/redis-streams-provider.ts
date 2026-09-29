import { computeBackoff } from '../../compute-backoff';
import { ScriptRegistry } from '../../script-registry';
import type { BackoffConfig, RedisClient } from '../../types';
import { Priority, STREAM_PREFIX } from '../../types';
import {
  deadLetterKey,
  dedupeKey,
  errorKey,
  scheduledClaimedKey,
  scheduledKey,
  streamKey,
  topicFanoutGroup,
  topicNamedGroup,
  topicStreamKey,
} from '../../wire';
import type {
  BackstageProvider,
  ConsumeOptions,
  DedupeCapability,
  DelayPromoter,
  DelaysCapability,
  JobDelivery,
  JobsCapability,
  OutgoingJob,
  ProviderContext,
  Subscription,
  TopicDelivery,
  TopicSubscribeOptions,
  TopicsCapability,
} from '../types';
import { CLAIM_SCHEDULED_LUA, PROCESS_SCHEDULED_LUA } from './lua';
import { normalizeXReadGroup, parseFields } from './parse';

export interface RedisStreamsProviderConfig {
  host?: string;
  port?: number;
  password?: string;
  db?: number;
  /** Existing client — skips URL construction. */
  redis?: RedisClient;
  prefix?: string;
  topicMaxLen?: number;
  topicGroupIdleMs?: number;
  /** Idle ms before topic reclaim (XPENDING IDLE / XCLAIM). Default 60000. */
  idleTimeoutMs?: number;
  deleteOnAck?: boolean;
  /** Block timeout for XREADGROUP when idle (ms). */
  blockTimeout?: number;
  reclaimIntervalMs?: number;
  /** Max topic delivery attempts before drop (no DLQ). Default 5. */
  maxDeliveries?: number;
}

function buildUrl(cfg: RedisStreamsProviderConfig): string {
  const host = cfg.host ?? 'localhost';
  const port = cfg.port ?? 6379;
  const db = cfg.db ?? 0;
  const auth = cfg.password ? `:${encodeURIComponent(cfg.password)}@` : '';
  return `redis://${auth}${host}:${port}/${db}`;
}

const ACK_BATCH_SIZE = 100;
const ACK_FLUSH_INTERVAL_MS = 50;

/**
 * A connection of its own for one XREADGROUP ... BLOCK loop. Redis answers a
 * connection's commands in order, so a blocked read on the shared connection
 * would stall every publish, ack and other read behind it.
 */
class BlockingReader {
  private constructor(
    readonly client: RedisClient,
    private readonly clientId: string,
    private readonly control: RedisClient,
  ) {}

  static async open(shared: RedisClient): Promise<BlockingReader> {
    const client = await shared.duplicate();
    const clientId = String(await client.send('CLIENT', ['ID']));
    return new BlockingReader(client, clientId, shared);
  }

  /**
   * Wait for `loop` to exit, ending any pending BLOCK early. CLIENT UNBLOCK
   * makes the read return as if it timed out, so no entry is handed to a
   * consumer that is going away. Where UNBLOCK is not permitted the loop
   * still exits when its BLOCK times out.
   */
  async stop(loop: Promise<void>): Promise<void> {
    let done = false;
    void loop.finally(() => {
      done = true;
    });
    while (!done) {
      await this.control
        .send('CLIENT', ['UNBLOCK', this.clientId])
        .catch(() => {});
      await Promise.race([loop, Bun.sleep(50)]);
    }
    this.client.close();
  }
}

/** Batched XACK (+ optional XDEL) flusher shared by a consume subscription. */
class AckBatcher {
  private pending = new Map<string, string[]>();
  private timer: ReturnType<typeof setInterval> | null = null;
  private closed = false;

  constructor(
    private redis: RedisClient,
    private group: string,
    private deleteOnAck: boolean,
  ) {
    this.timer = setInterval(() => {
      this.flush().catch(() => {});
    }, ACK_FLUSH_INTERVAL_MS);
  }

  queue(streamKey: string, id: string): void {
    if (this.closed) {
      // Best-effort immediate ack after close
      void this.redis.send('XACK', [streamKey, this.group, id]);
      return;
    }
    let list = this.pending.get(streamKey);
    if (!list) {
      list = [];
      this.pending.set(streamKey, list);
    }
    list.push(id);
    if (list.length >= ACK_BATCH_SIZE) {
      this.pending.set(streamKey, []);
      this.flushStream(streamKey, list).catch(() => {});
    }
  }

  async flush(): Promise<void> {
    const entries = [...this.pending.entries()];
    this.pending.clear();
    await Promise.all(
      entries.map(([key, ids]) =>
        ids.length ? this.flushStream(key, ids) : Promise.resolve(),
      ),
    );
  }

  private async flushStream(sKey: string, ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    await this.redis.send('XACK', [sKey, this.group, ...ids]);
    if (this.deleteOnAck) {
      await this.redis.send('XDEL', [sKey, ...ids]);
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    await this.flush();
  }
}

export class RedisStreamsProvider implements BackstageProvider {
  readonly name = 'redis-streams';
  readonly jobs: JobsCapability;
  readonly topics: TopicsCapability;
  readonly delays: DelaysCapability & DelayPromoter;
  readonly dedupe: DedupeCapability;

  readonly redis: RedisClient;
  readonly scripts: ScriptRegistry;
  readonly prefix: string;
  private readonly topicMaxLen: number;
  private readonly topicGroupIdleMs: number;
  private readonly idleTimeoutMs: number;
  private readonly deleteOnAck: boolean;
  private readonly blockTimeout: number;
  private readonly reclaimIntervalMs: number;
  private readonly maxDeliveries: number;
  private ctx: ProviderContext | null = null;
  // Set by delays.bindJobs; overrides ctx jobs for promotion.
  private promoteJobs: JobsCapability | null = null;
  private ownsClient: boolean;

  constructor(config: RedisStreamsProviderConfig = {}) {
    this.ownsClient = !config.redis;
    this.redis = config.redis ?? new Bun.RedisClient(buildUrl(config));
    this.scripts = new ScriptRegistry(this.redis);
    this.prefix = config.prefix ?? STREAM_PREFIX;
    this.topicMaxLen = config.topicMaxLen ?? 10_000;
    this.topicGroupIdleMs = config.topicGroupIdleMs ?? 3_600_000;
    this.idleTimeoutMs = config.idleTimeoutMs ?? 60_000;
    this.deleteOnAck = config.deleteOnAck ?? false;
    this.blockTimeout = config.blockTimeout ?? 5000;
    this.reclaimIntervalMs = config.reclaimIntervalMs ?? 30_000;
    this.maxDeliveries = config.maxDeliveries ?? 5;

    this.jobs = this.createJobs();
    this.delays = this.createDelays();
    this.dedupe = this.createDedupe();
    this.topics = this.createTopics();
  }

  async init(ctx: ProviderContext): Promise<void> {
    this.ctx = ctx;
  }

  async close(): Promise<void> {
    if (this.ownsClient) {
      try {
        this.redis.close();
      } catch {
        // already closed
      }
    }
  }

  private async ensureGroup(
    key: string,
    group: string,
    start = '0',
  ): Promise<void> {
    try {
      await this.redis.send('XGROUP', [
        'CREATE',
        key,
        group,
        start,
        'MKSTREAM',
      ]);
    } catch (err: unknown) {
      if (err instanceof Error && !err.message.includes('BUSYGROUP')) throw err;
    }
  }

  private createJobs(): JobsCapability {
    const self = this;
    return {
      name: 'redis-streams',
      async ensureQueues(_queues: string[]): Promise<void> {
        // Streams + groups are created via XGROUP CREATE … MKSTREAM in consume.
        // Do NOT XADD MAXLEN ~ 0 here — that truncates queued jobs.
      },
      async publish(job: OutgoingJob): Promise<string> {
        const key = streamKey(self.prefix, job.queue);
        const args: string[] = [
          key,
          '*',
          'taskName',
          job.taskName,
          'payload',
          JSON.stringify(job.payload),
          'enqueuedAt',
          String(job.enqueuedAt),
        ];
        if (job.meta.attempts !== undefined) {
          args.push('attempts', String(job.meta.attempts));
        }
        if (job.meta.backoff) {
          args.push('backoff', JSON.stringify(job.meta.backoff));
        }
        if (job.meta.timeout !== undefined) {
          args.push('timeout', String(job.meta.timeout));
        }
        return (await self.redis.send('XADD', args)) as string;
      },
      async consume(
        opts: ConsumeOptions,
        onDelivery: (d: JobDelivery) => Promise<void>,
      ): Promise<Subscription> {
        for (const q of opts.queues) {
          await self.ensureGroup(streamKey(self.prefix, q), opts.group, '0');
        }

        let running = true;
        let gotMessages = false;
        const active = new Set<Promise<void>>();
        const acks = new AckBatcher(self.redis, opts.group, self.deleteOnAck);
        const reader = await BlockingReader.open(self.redis);

        const reclaimTimer = setInterval(() => {
          if (!running) return;
          self.reclaimLoop(opts, onDelivery, active, acks).catch(() => {});
        }, self.reclaimIntervalMs);

        const loop = (async () => {
          while (running) {
            try {
              if (active.size >= opts.prefetch) {
                await Promise.race([
                  Promise.race(active),
                  Bun.sleep(50),
                ]);
                continue;
              }
              const available = opts.prefetch - active.size;
              const count = Math.max(1, available);
              const keys = opts.queues.map((q) => streamKey(self.prefix, q));
              const ids = keys.map(() => '>');
              const args: string[] = [
                'GROUP',
                opts.group,
                opts.consumerId,
                'COUNT',
                String(count),
              ];
              if (!gotMessages) {
                args.push('BLOCK', String(self.blockTimeout));
              }
              args.push('STREAMS', ...keys, ...ids);

              const result = await reader.client.send('XREADGROUP', args);
              const entries = normalizeXReadGroup(result);
              if (entries.length === 0) {
                gotMessages = false;
                continue;
              }
              gotMessages = true;

              for (const [sKey, messages] of entries) {
                if (!Array.isArray(messages)) continue;
                const queue = sKey.slice(self.prefix.length + 1);
                for (const entry of messages) {
                  if (!Array.isArray(entry) || entry.length < 2) continue;
                  const [msgId, fields] = entry as [string, unknown[]];
                  const delivery = self.toJobDelivery(
                    queue,
                    msgId,
                    fields,
                    1,
                    opts,
                    acks,
                  );
                  if (!delivery) continue;
                  const p = onDelivery(delivery).catch(() => {});
                  active.add(p);
                  p.finally(() => active.delete(p));
                }
              }
            } catch {
              if (running) {
                await Bun.sleep(1000);
              }
            }
          }
        })();

        return {
          async stop() {
            running = false;
            clearInterval(reclaimTimer);
            // Do not wait on in-flight handlers — Worker applies gracePeriod.
            await reader.stop(loop);
            await acks.close();
          },
        };
      },
    };
  }

  private async reclaimLoop(
    opts: ConsumeOptions,
    onDelivery: (d: JobDelivery) => Promise<void>,
    active: Set<Promise<void>>,
    acks: AckBatcher,
  ): Promise<void> {
    for (const q of opts.queues) {
      const sKey = streamKey(this.prefix, q);
      let pending: unknown;
      try {
        pending = await this.redis.send('XPENDING', [
          sKey,
          opts.group,
          'IDLE',
          String(opts.idleTimeout),
          '-',
          '+',
          '10',
        ]);
      } catch (err) {
        this.ctx?.logger.error('Error checking pending messages', {
          error: String(err),
          streamKey: sKey,
        });
        continue;
      }
      if (!pending || !Array.isArray(pending)) continue;

      for (const entry of pending) {
        if (!Array.isArray(entry) || entry.length < 4) continue;
        const [messageId, , idleTime, deliveryCount] = entry as [
          string,
          string,
          number,
          number,
        ];

        const details = await this.redis.send('XRANGE', [
          sKey,
          messageId,
          messageId,
          'COUNT',
          '1',
        ]);
        if (!details || !Array.isArray(details) || details.length === 0) continue;
        const [, fields] = details[0] as [string, unknown[]];
        const data = parseFields(fields);
        let backoff: BackoffConfig | undefined;
        if (data.backoff) {
          try {
            backoff = JSON.parse(data.backoff);
          } catch {
            /* ignore */
          }
        }
        if (backoff) {
          const required = computeBackoff(backoff, deliveryCount);
          if (idleTime < required) continue;
        }

        try {
          const result = await this.redis.send('XCLAIM', [
            sKey,
            opts.group,
            opts.consumerId,
            String(opts.idleTimeout),
            messageId,
          ]);
          if (!result || !Array.isArray(result) || result.length === 0) continue;
          const first = result[0];
          if (!Array.isArray(first) || first.length < 2) continue;
          const [claimedId, claimedFields] = first as [string, unknown[]];
          const delivery = this.toJobDelivery(
            q,
            claimedId,
            claimedFields,
            Number(deliveryCount) + 1,
            opts,
            acks,
          );
          if (!delivery) continue;
          const p = onDelivery(delivery).catch(() => {});
          active.add(p);
          p.finally(() => active.delete(p));
        } catch {
          // claim race
        }
      }
    }
  }

  private toJobDelivery(
    queue: string,
    id: string,
    fields: unknown[],
    deliveryCount: number,
    opts: ConsumeOptions,
    acks: AckBatcher,
  ): JobDelivery | null {
    try {
      const data = parseFields(fields);
      if (data._init) return null;
      const meta: OutgoingJob['meta'] = {};
      if (data.attempts) meta.attempts = parseInt(data.attempts, 10);
      if (data.backoff) {
        try {
          meta.backoff = JSON.parse(data.backoff);
        } catch {
          /* ignore */
        }
      }
      if (data.timeout) meta.timeout = parseInt(data.timeout, 10);

      const sKey = streamKey(this.prefix, queue);
      const self = this;

      return {
        id,
        queue,
        taskName: data.taskName || '',
        payload: JSON.parse(data.payload || 'null'),
        enqueuedAt: parseInt(data.enqueuedAt || '0', 10) || Date.now(),
        deliveryCount,
        meta,
        async ack() {
          acks.queue(sKey, id);
        },
        async retry({ delayMs, error }) {
          if (error) {
            await self.redis.send('SET', [
              errorKey(self.prefix, id),
              error,
              'EX',
              '3600',
            ]);
          }
          void delayMs;
        },
        async deadLetter({ error }) {
          const dlq = deadLetterKey(self.prefix, queue);
          let err = error;
          if (!err) {
            const stored = await self.redis.send('GET', [
              errorKey(self.prefix, id),
            ]);
            if (typeof stored === 'string') err = stored;
          }
          const args = [
            dlq,
            '*',
            'taskName',
            data.taskName || '',
            'payload',
            data.payload || 'null',
            'enqueuedAt',
            data.enqueuedAt || String(Date.now()),
            'originalId',
            id,
            'deliveryCount',
            String(deliveryCount),
            'deadLetteredAt',
            String(Date.now()),
          ];
          if (err) args.push('error', err);
          await self.redis.send('XADD', args);
          // Dead-letter ACK is immediate (settlement must remove from PEL now)
          await self.redis.send('XACK', [sKey, opts.group, id]);
          await self.redis
            .send('DEL', [errorKey(self.prefix, id)])
            .catch(() => {});
        },
      };
    } catch {
      return null;
    }
  }

  private createDelays(): DelaysCapability & DelayPromoter {
    const self = this;
    return {
      name: 'redis-streams',
      async schedule(job: OutgoingJob, runAt: number): Promise<string> {
        const key = scheduledKey(self.prefix);
        const target = streamKey(self.prefix, job.queue);
        const member = JSON.stringify({
          taskName: job.taskName,
          payload: JSON.stringify(job.payload),
          enqueuedAt: job.enqueuedAt,
          streamKey: target,
          priority: job.queue,
          deliveryCount: job.deliveryCount,
          attempts: job.meta.attempts,
          backoff: job.meta.backoff
            ? JSON.stringify(job.meta.backoff)
            : undefined,
          timeout: job.meta.timeout,
        });
        await self.redis.send('ZADD', [key, String(runAt), member]);
        // Promote loop lives only in the Worker — producers must not run one.
        return `scheduled:${runAt}`;
      },
      bindJobs(jobs: JobsCapability): void {
        self.promoteJobs = jobs;
      },
      promote(): Promise<number> {
        return self.promoteCrossProvider();
      },
    };
  }

  /**
   * Promote due delayed jobs. Called by the Worker promote timer only.
   * Also retries stuck members in scheduled:claimed (cross-provider path).
   */
  async promoteCrossProvider(): Promise<number> {
    const jobs = this.promoteJobs ?? this.ctx?.capabilities.jobs;
    if (!jobs) return 0;
    if (jobs.name === 'redis-streams') {
      const result = await this.redis.send('EVAL', [
        PROCESS_SCHEDULED_LUA,
        '1',
        scheduledKey(this.prefix),
        String(Date.now()),
        this.prefix,
        Priority.DEFAULT,
      ]);
      return (result as number) ?? 0;
    }

    // Only retry claimed members older than 30s (score = claim time)
    const claimAgeCutoff = Date.now() - 30_000;
    const stuck = (await this.redis.send('ZRANGEBYSCORE', [
      scheduledClaimedKey(this.prefix),
      '-inf',
      String(claimAgeCutoff),
    ])) as string[];
    let n = 0;
    for (const raw of stuck ?? []) {
      if (await this.publishClaimedMember(jobs, raw)) n++;
    }

    const claimed = (await this.redis.send('EVAL', [
      CLAIM_SCHEDULED_LUA,
      '2',
      scheduledKey(this.prefix),
      scheduledClaimedKey(this.prefix),
      String(Date.now()),
    ])) as string[];

    for (const raw of claimed ?? []) {
      if (await this.publishClaimedMember(jobs, raw)) n++;
    }
    return n;
  }

  private async publishClaimedMember(
    jobs: JobsCapability,
    raw: string,
  ): Promise<boolean> {
    try {
      const task = JSON.parse(raw);
      const queue =
        typeof task.streamKey === 'string'
          ? task.streamKey.slice(this.prefix.length + 1)
          : task.priority || Priority.DEFAULT;
      await jobs.publish({
        queue,
        taskName: task.taskName,
        payload: JSON.parse(task.payload || 'null'),
        enqueuedAt: task.enqueuedAt ?? Date.now(),
        deliveryCount: task.deliveryCount ?? 1,
        meta: {
          attempts: task.attempts,
          backoff: task.backoff ? JSON.parse(task.backoff) : undefined,
          timeout: task.timeout,
        },
      });
      await this.redis.send('ZREM', [scheduledClaimedKey(this.prefix), raw]);
      return true;
    } catch {
      // leave in claimed for retry on next promote tick
      return false;
    }
  }

  private createDedupe(): DedupeCapability {
    const self = this;
    return {
      name: 'redis-streams',
      async claim(key: string, ttlMs: number): Promise<boolean> {
        const ttlSeconds = Math.ceil(ttlMs / 1000);
        const set = await self.redis.send('SET', [
          dedupeKey(self.prefix, key),
          '1',
          'NX',
          'EX',
          String(ttlSeconds),
        ]);
        return !!set;
      },
    };
  }

  private createTopics(): TopicsCapability {
    const self = this;
    return {
      name: 'redis-streams',
      async publish(topic: string, payload: unknown): Promise<string> {
        const key = topicStreamKey(self.prefix, topic);
        return (await self.redis.send('XADD', [
          key,
          'MAXLEN',
          '~',
          String(self.topicMaxLen),
          '*',
          'payload',
          JSON.stringify(payload),
          'publishedAt',
          String(Date.now()),
        ])) as string;
      },
      async subscribe(
        opts: TopicSubscribeOptions,
        onMessage: (m: TopicDelivery) => Promise<void>,
      ): Promise<Subscription> {
        const key = topicStreamKey(self.prefix, opts.topic);
        const group = opts.group
          ? topicNamedGroup(opts.group)
          : topicFanoutGroup(opts.consumerId);
        const start = opts.from === 'earliest' ? '0' : '$';
        await self.ensureGroup(key, group, start);
        const reader = await BlockingReader.open(self.redis);

        let running = true;
        const loop = (async () => {
          while (running) {
            try {
              const result = await reader.client.send('XREADGROUP', [
                'GROUP',
                group,
                opts.consumerId,
                'COUNT',
                '10',
                'BLOCK',
                String(self.blockTimeout),
                'STREAMS',
                key,
                '>',
              ]);
              const entries = normalizeXReadGroup(result);
              for (const [, messages] of entries) {
                if (!Array.isArray(messages)) continue;
                for (const entry of messages) {
                  if (!Array.isArray(entry) || entry.length < 2) continue;
                  const [msgId, fields] = entry as [string, unknown[]];
                  await self.handleTopicMessage(
                    key,
                    group,
                    opts.topic,
                    msgId,
                    fields,
                    1,
                    onMessage,
                  );
                }
              }
            } catch {
              if (running) await Bun.sleep(500);
            }
          }
        })();

        const reclaimTimer = setInterval(() => {
          if (!running) return;
          self
            .reclaimTopicMessages(key, group, opts, onMessage)
            .catch(() => {});
        }, self.reclaimIntervalMs);

        const cleanupTimer = setInterval(() => {
          if (!opts.group) {
            self.cleanupIdleTopicGroups(key).catch(() => {});
          }
        }, self.topicGroupIdleMs);

        return {
          async stop() {
            running = false;
            clearInterval(reclaimTimer);
            clearInterval(cleanupTimer);
            await reader.stop(loop);
          },
        };
      },
    };
  }

  private async handleTopicMessage(
    key: string,
    group: string,
    topic: string,
    msgId: string,
    fields: unknown[],
    deliveryCount: number,
    onMessage: (m: TopicDelivery) => Promise<void>,
  ): Promise<void> {
    const data = parseFields(fields);
    const redis = this.redis;
    const delivery: TopicDelivery = {
      id: msgId,
      topic,
      payload: JSON.parse(data.payload || 'null'),
      publishedAt: parseInt(data.publishedAt || '0', 10) || Date.now(),
      deliveryCount,
      async ack() {
        await redis.send('XACK', [key, group, msgId]);
      },
    };

    try {
      await onMessage(delivery);
      await delivery.ack();
    } catch (err) {
      if (deliveryCount >= this.maxDeliveries) {
        // Spec §7: topics have no DLQ — drop with error log after maxDeliveries
        this.ctx?.logger.error(
          `Topic handler failed after ${deliveryCount} deliveries; dropping`,
          {
            topic,
            id: msgId,
            error: err instanceof Error ? err.message : String(err),
          },
        );
        await delivery.ack();
      }
      // else leave pending for reclaim retry
    }
  }

  private async reclaimTopicMessages(
    key: string,
    group: string,
    opts: TopicSubscribeOptions,
    onMessage: (m: TopicDelivery) => Promise<void>,
  ): Promise<void> {
    const idle = String(this.idleTimeoutMs);
    let pending: unknown;
    try {
      pending = await this.redis.send('XPENDING', [
        key,
        group,
        'IDLE',
        idle,
        '-',
        '+',
        '10',
      ]);
    } catch (err) {
      this.ctx?.logger.error('Error checking pending messages', {
        error: String(err),
        streamKey: key,
      });
      return;
    }
    if (!pending || !Array.isArray(pending)) return;
    for (const entry of pending) {
      if (!Array.isArray(entry) || entry.length < 4) continue;
      const [messageId, , , deliveryCount] = entry as [
        string,
        string,
        number,
        number,
      ];
      try {
        const result = await this.redis.send('XCLAIM', [
          key,
          group,
          opts.consumerId,
          idle,
          messageId,
        ]);
        if (!result || !Array.isArray(result) || result.length === 0) continue;
        const first = result[0];
        if (!Array.isArray(first) || first.length < 2) continue;
        const [claimedId, claimedFields] = first as [string, unknown[]];
        await this.handleTopicMessage(
          key,
          group,
          opts.topic,
          claimedId,
          claimedFields,
          Number(deliveryCount) + 1,
          onMessage,
        );
      } catch {
        // claim race
      }
    }
  }

  private async cleanupIdleTopicGroups(stream: string): Promise<number> {
    const groups = await this.redis.send('XINFO', ['GROUPS', stream]);
    if (!Array.isArray(groups)) return 0;
    let deleted = 0;
    for (const g of groups) {
      let name = '';
      if (Array.isArray(g)) {
        const map = parseFields(g);
        name = map.name || '';
      } else if (g && typeof g === 'object') {
        name = String((g as { name?: string }).name || '');
      }
      if (name.startsWith('sub:')) {
        try {
          const consumers = await this.redis.send('XINFO', [
            'CONSUMERS',
            stream,
            name,
          ]);
          let allIdle = true;
          if (Array.isArray(consumers)) {
            for (const c of consumers) {
              const idleMs =
                c && typeof c === 'object' && !Array.isArray(c)
                  ? Number((c as { idle?: number }).idle ?? 0)
                  : 0;
              if (idleMs < this.topicGroupIdleMs) allIdle = false;
            }
          }
          if (allIdle) {
            await this.redis.send('XGROUP', ['DESTROY', stream, name]);
            deleted++;
          }
        } catch {
          /* ignore */
        }
      }
    }
    return deleted;
  }
}
