/**
 * Backstage SDK
 *
 * Transport-agnostic worker orchestration. Redis Streams is the default provider.
 */

export * from './core';

export { Worker, type WorkerOptions } from './worker';
export { Scheduler, type SchedulerConfig } from './scheduler';
export { computeBackoff } from './compute-backoff';

export {
  RedisStreamsProvider,
  type RedisStreamsProviderConfig,
} from './provider/redis';

export type {
  BackstageProvider,
  CapabilityName,
  CapabilityOverrides,
  CapabilityReport,
  CapabilityReportEntry,
  ConsumeOptions,
  DedupeCapability,
  DelayPromoter,
  DelaysCapability,
  JobDelivery,
  JobsCapability,
  OutgoingJob,
  ProviderContext,
  ResolvedCapabilities,
  Subscription,
  TopicDelivery,
  TopicSubscribeOptions,
  TopicsCapability,
} from './provider';

export {
  CapabilityMissingError,
  buildCapabilityReport,
  formatCapabilityReport,
  isDelayPromoter,
  requireCapability,
  resolveCapabilities,
} from './provider';
