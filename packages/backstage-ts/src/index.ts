/**
 * Backstage SDK
 */

// Core
export { Worker } from './worker';
export { Stream } from './stream';
export { Reclaimer } from './reclaimer';
export { Broadcast } from './broadcast';
export { Scheduler } from './scheduler';

// Task & Queue
export { Task, type TaskFn, type TaskOptions } from './task';
export { Queue, DefaultQueues, type QueueOptions } from './queue';
export { CronTask } from './cron';

// Infrastructure
export {
  Logger,
  LogLevel,
  createLogger,
  type LoggerConfig,
  type LogHandler,
  type LogEntry,
} from './logger';
export { serialize, deserialize } from './serializer';
export {
  BackstageError,
  SoftTimeout,
  HardTimeout,
  PreventTaskExecution,
  TaskNotFound,
  QueueNotFound,
  InvalidCronSchedule,
  RedisConnectionError,
} from './exceptions';

// Utilities
export {
  inspect,
  numPendingTasks,
  numScheduledTasks,
  numDeadLetterTasks,
  getPendingTasks,
  getScheduledTasks,
  purgeQueue,
  purgeScheduled,
  purgeDeadLetter,
  getConsumerGroups,
  getConsumers,
  type QueueInfo,
  type QueuesInfo,
} from './utils';

// Types
export {
  Priority,
  STREAM_PREFIX,
  BROADCAST_STREAM,
  getStreamKey,
  getDeadLetterKey,
  getScheduledKey,
  getDefaultWorkerId,
  parseFields,
  DEFAULT_WORKER_CONFIG,
  type RedisClient,
  type WorkflowInstruction,
  type TaskHandler,
  type StreamMessage,
  type StreamMessageData,
  type TaskConfig,
  type WorkerConfig,
  type EnqueueOptions,
} from './types';
