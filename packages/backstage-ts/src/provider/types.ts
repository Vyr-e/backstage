import type { BackoffConfig, EnqueueOptions, Priority } from '../types';

export interface ProviderCapabilities {
  durable?: boolean;
  broadcast?: boolean;
  scheduling?: boolean;
  retries?: boolean;
  deduplication?: boolean;
}

export interface PublishOptions {
  priority?: Priority;
  queue?: string;
  delay?: number;
  dedupe?: {
    key: string;
    ttl?: number;
  };
  attempts?: number;
  backoff?: BackoffConfig;
  timeout?: number;
}

export interface MessageRef {
  id: string;
  queue: string;
  taskName: string;
  payload: unknown;
  enqueuedAt: number;
  deliveryCount: number;
  attempts?: number;
  backoff?: BackoffConfig;
  timeout?: number;
}

export interface ConsumeArgs {
  queues: string[];
  consumerGroup: string;
  consumerId: string;
  maxMessages: number;
  blockMs?: number;
}

export interface ReclaimIdleArgs {
  queues: string[];
  consumerGroup: string;
  consumerId: string;
  idleMs: number;
  maxCount?: number;
}

export interface DeadLetterMeta {
  originalId: string;
  deliveryCount: number;
  error?: string;
}

/**
 * Transport-agnostic provider contract.
 * Core never reaches around this for enqueue/consume/ack/reclaim/DLQ/schedule/broadcast.
 */
export interface BackstageProvider {
  readonly name: string;
  readonly capabilities?: ProviderCapabilities;

  ensureQueues(queues: string[]): Promise<void>;
  close(): Promise<void>;

  publish(
    taskName: string,
    payload: unknown,
    opts?: PublishOptions,
  ): Promise<string | null>;

  consume(args: ConsumeArgs): Promise<MessageRef[]>;

  ack(messages: MessageRef[]): Promise<void>;

  /** ACK and remove from the underlying store when supported (e.g. Redis XDEL). */
  ackAndForget?(messages: MessageRef[]): Promise<void>;

  reclaimIdle(args: ReclaimIdleArgs): Promise<MessageRef[]>;

  deadLetter(message: MessageRef, meta: DeadLetterMeta): Promise<void>;

  promoteDueScheduled(nowMs?: number): Promise<number>;

  ensureBroadcast(
    consumerIdentity: string,
    start: 'latest' | 'beginning',
  ): Promise<void>;

  broadcast(taskName: string, payload: unknown): Promise<string>;

  consumeBroadcast(args: {
    consumerIdentity: string;
    maxMessages: number;
    blockMs?: number;
  }): Promise<MessageRef[]>;

  ackBroadcast(consumerIdentity: string, ids: string[]): Promise<void>;

  cleanupBroadcastGhosts?(idleMs: number): Promise<number>;
}

export type { EnqueueOptions };
