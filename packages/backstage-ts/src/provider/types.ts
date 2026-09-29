import type { BackoffConfig } from '../types';
import type { Logger } from '../logger';

export type CapabilityName = 'jobs' | 'topics' | 'delays' | 'dedupe';

export interface Subscription {
  stop(): Promise<void>;
}

export interface OutgoingJob {
  queue: string;
  taskName: string;
  payload: unknown;
  enqueuedAt: number;
  meta: {
    attempts?: number;
    backoff?: BackoffConfig;
    timeout?: number;
  };
  deliveryCount?: number;
}

export interface ConsumeOptions {
  queues: string[];
  group: string;
  consumerId: string;
  prefetch: number;
  idleTimeout: number;
}

export interface JobDelivery {
  id: string;
  queue: string;
  taskName: string;
  payload: unknown;
  enqueuedAt: number;
  deliveryCount: number;
  meta: OutgoingJob['meta'];
  ack(): Promise<void>;
  retry(opts: { delayMs: number; error?: string }): Promise<void>;
  deadLetter(opts: { error?: string }): Promise<void>;
}

export interface JobsCapability {
  name: string;
  requires?: CapabilityName[];
  ensureQueues(queues: string[]): Promise<void>;
  publish(job: OutgoingJob): Promise<string>;
  consume(
    opts: ConsumeOptions,
    onDelivery: (d: JobDelivery) => Promise<void>,
  ): Promise<Subscription>;
}

export interface TopicSubscribeOptions {
  topic: string;
  group?: string;
  consumerId: string;
  from: 'latest' | 'earliest';
}

export interface TopicDelivery {
  id: string;
  topic: string;
  payload: unknown;
  publishedAt: number;
  deliveryCount: number;
  ack(): Promise<void>;
}

export interface TopicsCapability {
  name: string;
  publish(topic: string, payload: unknown): Promise<string>;
  subscribe(
    opts: TopicSubscribeOptions,
    onMessage: (m: TopicDelivery) => Promise<void>,
  ): Promise<Subscription>;
}

export interface DelaysCapability {
  name: string;
  schedule(job: OutgoingJob, runAt: number): Promise<string>;
}

export interface DedupeCapability {
  name: string;
  claim(key: string, ttlMs: number): Promise<boolean>;
}

export interface ResolvedCapabilities {
  jobs: JobsCapability;
  topics?: TopicsCapability;
  delays?: DelaysCapability;
  dedupe?: DedupeCapability;
}

export interface ProviderContext {
  capabilities: ResolvedCapabilities;
  logger: Logger;
}

export interface BackstageProvider {
  name: string;
  jobs: JobsCapability;
  topics?: TopicsCapability;
  delays?: DelaysCapability;
  dedupe?: DedupeCapability;
  init?(ctx: ProviderContext): Promise<void>;
  close(): Promise<void>;
}

export interface CapabilityOverrides {
  topics?: TopicsCapability;
  delays?: DelaysCapability;
  dedupe?: DedupeCapability;
}

export interface CapabilityReportEntry {
  available: boolean;
  name?: string;
  source: 'provider' | 'plugged' | 'missing';
  hint?: string;
}

export interface CapabilityReport {
  provider: string;
  jobs: CapabilityReportEntry;
  topics: CapabilityReportEntry;
  delays: CapabilityReportEntry;
  dedupe: CapabilityReportEntry;
}
