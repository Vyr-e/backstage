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
  deleteOnAck?: boolean;
  /** Block timeout for XREADGROUP when idle (ms). */
  blockTimeout?: number;
  reclaimIntervalMs?: number;
}

function buildUrl(cfg: RedisStreamsProviderConfig): string {
  const host = cfg.host ?? 'localhost';
  const port = cfg.port ?? 6379;
  const db = cfg.db ?? 0;
  const auth = cfg.password ? `:${encodeURIComponent(cfg.password)}@` : '';
  return `redis://${auth}${host}:${port}/${db}`;
}

export class RedisStreamsProvider implements BackstageProvider {
  readonly name = 'redis-streams';
  readonly jobs: JobsCapability;
  readonly topics: TopicsCapability;
  readonly delays: DelaysCapability;
  readonly dedupe: DedupeCapability;

  readonly redis: RedisClient;
  readonly scripts: ScriptRegistry;
  readonly prefix: string;
  private readonly topicMaxLen: number;
  private readonly topicGroupIdleMs: number;
  private readonly deleteOnAck: boolean;
  private readonly blockTimeout: number;
  private readonly reclaimIntervalMs: number;
  private ctx: ProviderContext | null = null;
  private ownsClient: boolean;
  private promoteTimer: ReturnType<typeof setInterval> | null = null;

  constructor(config: RedisStreamsProviderConfig = {}) {
    this.ownsClient = !config.redis;
    this.redis = config.redis ?? new Bun.RedisClient(buildUrl(config));
    this.scripts = new ScriptRegistry(this.redis);
    this.prefix = config.prefix ?? STREAM_PREFIX;
    this.topicMaxLen = config.topicMaxLen ?? 10_000;
    this.topicGroupIdleMs = config.topicGroupIdleMs ?? 3_600_000;
    this.deleteOnAck = config.deleteOnAck ?? false;
    this.blockTimeout = config.blockTimeout ?? 5000;
    this.reclaimIntervalMs = config.reclaimIntervalMs ?? 30_000;

    this.jobs = this.createJobs();
    this.delays = this.createDelays();
    this.dedupe = this.createDedupe();
    this.topics = this.createTopics();
  }

  async init(ctx: ProviderContext): Promise<void> {
    this.ctx = ctx;
  }

  async close(): Promise<void> {
    if (this.promoteTimer) {
      clearInterval(this.promoteTimer);
      this.promoteTimer = null;
    }
    if (this.ownsClient) {
      try {
        this.redis.close();
      } catch {
        // already closed by a stopped consumer
      }
    }
  }

  private ensurePromoteLoop(): void {
    if (this.promoteTimer) return;
    this.promoteTimer = setInterval(() => {
      this.promoteCrossProvider().catch(() => {});
    }, 200);
  }

  private async ensureGroup(key: string, group: string, start = '0'): Promise<void> {
    try {
      await this.redis.send('XGROUP', ['CREATE', key, group, start, 'MKSTREAM']);
    } catch (err: unknown) {
      if (err instanceof Error && !err.message.includes('BUSYGROUP')) throw err;
    }
  }

  private createJobs(): JobsCapability {
    const self = this;
    return {
      name: 'redis-streams',
      async ensureQueues(queues: string[]): Promise<void> {
        // group is set per consume; ensureQueues only creates streams with MKSTREAM via a placeholder group create later
        for (const q of queues) {
          const key = streamKey(self.prefix, q);
          // Touch stream existence without binding a specific consumer group yet
          await self.redis.send('XADD', [key, 'MAXLEN', '~', '0', '*', '_init', '1']).catch(() => {});
          // Prefer MKSTREAM via XGROUP when we know the group — Worker will pass group in consume
        }
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

        const reclaimTimer = setInterval(() => {
          if (!running) return;
          self.reclaimLoop(opts, onDelivery, active).catch(() => {});
        }, self.reclaimIntervalMs);

        const loop = (async () => {
          while (running) {
            try {
              if (active.size >= opts.prefetch) {
                await Promise.race(active);
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
                // Cap slice so Subscription.stop() is responsive
                args.push('BLOCK', String(Math.min(self.blockTimeout, 250)));
              }
              args.push('STREAMS', ...keys, ...ids);

              const result = await self.redis.send('XREADGROUP', args);
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
                  );
                  if (!delivery) continue;
                  const p = onDelivery(delivery).catch(() => {});
                  active.add(p);
                  p.finally(() => active.delete(p));
                }
              }
            } catch (err) {
              if (running) {
                await Bun.sleep(1000);
              }
            }
          }
          await Promise.allSettled([...active]);
        })();

        return {
          async stop() {
            running = false;
            clearInterval(reclaimTimer);
            await loop;
          },
        };
      },
    };
  }

  private async reclaimLoop(
    opts: ConsumeOptions,
    onDelivery: (d: JobDelivery) => Promise<void>,
    active: Set<Promise<void>>,
  ): Promise<void> {
    for (const q of opts.queues) {
      const sKey = streamKey(this.prefix, q);
      const pending = await this.redis.send('XPENDING', [
        sKey,
        opts.group,
        'IDLE',
        String(opts.idleTimeout),
        '-',
        '+',
        '10',
      ]);
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
          } catch {}
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
          // XPENDING count is pre-claim; XCLAIM increments times_delivered
          const delivery = this.toJobDelivery(
            q,
            claimedId,
            claimedFields,
            Number(deliveryCount) + 1,
            opts,
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
  ): JobDelivery | null {
    try {
      const data = parseFields(fields);
      if (data._init) return null;
      const meta: OutgoingJob['meta'] = {};
      if (data.attempts) meta.attempts = parseInt(data.attempts, 10);
      if (data.backoff) {
        try {
          meta.backoff = JSON.parse(data.backoff);
        } catch {}
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
          await self.redis.send('XACK', [sKey, opts.group, id]);
          if (self.deleteOnAck) {
            await self.redis.send('XDEL', [sKey, id]);
          }
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
          // Leave pending — reclaim honors delayMs via idle+backoff gate.
          // When backoff is on the message, reclaim uses computeBackoff.
          // When only delayMs is passed (no message backoff), we still leave
          // pending; reclaim uses idleTimeout as the floor (opts.idleTimeout).
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
          await self.redis.send('XACK', [sKey, opts.group, id]);
          await self.redis.send('DEL', [errorKey(self.prefix, id)]).catch(() => {});
        },
      };
    } catch {
      return null;
    }
  }

  private createDelays(): DelaysCapability {
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
          attempts: job.meta.attempts,
          backoff: job.meta.backoff
            ? JSON.stringify(job.meta.backoff)
            : undefined,
          timeout: job.meta.timeout,
        });
        await self.redis.send('ZADD', [key, String(runAt), member]);
        self.ensurePromoteLoop();
        await self.promoteCrossProvider();
        return `scheduled:${runAt}`;
      },
    };
  }

  /** Promote due delayed jobs through resolved jobs capability (non-Redis jobs). */
  async promoteCrossProvider(): Promise<number> {
    if (!this.ctx) return 0;
    const jobs = this.ctx.capabilities.jobs;
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

    const claimed = (await this.redis.send('EVAL', [
      CLAIM_SCHEDULED_LUA,
      '2',
      scheduledKey(this.prefix),
      scheduledClaimedKey(this.prefix),
      String(Date.now()),
    ])) as string[];

    let n = 0;
    for (const raw of claimed ?? []) {
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
          meta: {
            attempts: task.attempts,
            backoff: task.backoff ? JSON.parse(task.backoff) : undefined,
            timeout: task.timeout,
          },
        });
        await this.redis.send('ZREM', [scheduledClaimedKey(this.prefix), raw]);
        n++;
      } catch {
        // leave in claimed for retry
      }
    }
    return n;
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

        let running = true;
        const loop = (async () => {
          while (running) {
            try {
              const result = await self.redis.send('XREADGROUP', [
                'GROUP',
                group,
                opts.consumerId,
                'COUNT',
                '10',
                'BLOCK',
                String(Math.min(self.blockTimeout, 250)),
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
                  const data = parseFields(fields);
                  const delivery: TopicDelivery = {
                    id: msgId,
                    topic: opts.topic,
                    payload: JSON.parse(data.payload || 'null'),
                    publishedAt: parseInt(data.publishedAt || '0', 10) || Date.now(),
                    deliveryCount: 1,
                    async ack() {
                      await self.redis.send('XACK', [key, group, msgId]);
                    },
                  };
                  try {
                    await onMessage(delivery);
                    await delivery.ack();
                  } catch {
                    // leave pending for reclaim / retry up to max — fan-out drops after max in Worker
                  }
                }
              }
            } catch {
              if (running) await Bun.sleep(500);
            }
          }
        })();

        const cleanupTimer = setInterval(() => {
          if (!opts.group) {
            self.cleanupIdleTopicGroups(key).catch(() => {});
          }
        }, self.topicGroupIdleMs);

        return {
          async stop() {
            running = false;
            clearInterval(cleanupTimer);
            await loop;
          },
        };
      },
    };
  }

  private async cleanupIdleTopicGroups(stream: string): Promise<number> {
    const groups = await this.redis.send('XINFO', ['GROUPS', stream]);
    if (!Array.isArray(groups)) return 0;
    let deleted = 0;
    // Bun may return objects or arrays — best-effort cleanup of fan-out groups
    for (const g of groups) {
      let name = '';
      let idle = 0;
      if (Array.isArray(g)) {
        const map = parseFields(g);
        name = map.name || '';
        idle = parseInt(map['last-delivered-id'] ? '0' : '0', 10);
      } else if (g && typeof g === 'object') {
        name = String((g as { name?: string }).name || '');
      }
      if (name.startsWith('sub:')) {
        // Use consumer idle via XINFO CONSUMERS when available
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
          void idle;
        }
      }
    }
    return deleted;
  }
}
