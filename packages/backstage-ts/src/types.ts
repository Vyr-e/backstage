/**
 * Backstage SDK - Core Types
 *
 * Type definitions for the Redis Streams-based background worker system.
 */

import { hostname } from 'os';

// =============================================================================
// Redis Type
// =============================================================================

/**
 * Redis client type for Bun.redis
 */
export type RedisClient = InstanceType<typeof Bun.RedisClient>;

// =============================================================================
// Workflow Types
// =============================================================================

/**
 * Workflow instruction returned from handlers to chain tasks.
 * Inspired by Temporal's workflow patterns.
 */
export interface WorkflowInstruction {
  /** Name of the next task to execute */
  next: string;
  /** Delay in milliseconds before executing next task (Temporal-style timer) */
  delay?: number;
  /** Payload to pass to the next task */
  payload?: unknown;
}

/**
 * Handler function signature.
 * Can return void for simple tasks, or WorkflowInstruction for chaining.
 */
export type TaskHandler<T = unknown> = (
  payload: T
) => Promise<void | WorkflowInstruction>;

// =============================================================================
// Stream Message Types
// =============================================================================

/**
 * A message from a Redis Stream ready for processing.
 */
export interface StreamMessage<T = unknown> {
  /** Redis Stream message ID (e.g., "1234567890-0") */
  id: string;
  /** Name of the task to execute */
  taskName: string;
  /** Task payload data */
  payload: T;
  /** Number of times this message has been delivered */
  deliveryCount: number;
  /** Timestamp when the message was enqueued (ms since epoch) */
  enqueuedAt: number;
}

/**
 * Raw message data stored in Redis Stream.
 */
export interface StreamMessageData {
  taskName: string;
  payload: string; // JSON-serialized payload
  enqueuedAt: number;
}

// =============================================================================
// Priority Levels
// =============================================================================

/**
 * Priority levels for task streams.
 * Workers poll in order: urgent → default → low
 */
export enum Priority {
  URGENT = 'urgent',
  DEFAULT = 'default',
  LOW = 'low',
}

/**
 * Stream key prefix
 */
export const STREAM_PREFIX = 'backstage';

/**
 * Get the Redis stream key for a priority level.
 */
export function getStreamKey(priority: Priority): string {
  return `${STREAM_PREFIX}:${priority}`;
}

/**
 * Get the dead-letter stream key for a priority level.
 */
export function getDeadLetterKey(priority: Priority): string {
  return `${STREAM_PREFIX}:${priority}:dead-letter`;
}

/**
 * Get the scheduled tasks sorted set key.
 */
export function getScheduledKey(): string {
  return `${STREAM_PREFIX}:scheduled`;
}

/**
 * Broadcast stream key.
 */
export const BROADCAST_STREAM = `${STREAM_PREFIX}:broadcast`;

// =============================================================================
// Task Configuration
// =============================================================================

/**
 * Configuration for a registered task.
 */
export interface TaskConfig<T = unknown> {
  /** Task name (must be unique) */
  name: string;
  /** Handler function */
  handler: TaskHandler<T>;
  /** Default priority for this task */
  priority?: Priority;
  /** Maximum retries before dead-letter */
  maxRetries?: number;
  /** Rate limit: max executions per second */
  rateLimit?: number;
}

// =============================================================================
// Worker Configuration
// =============================================================================

/**
 * Configuration for the Backstage worker.
 */
export interface WorkerConfig {
  /** Redis host */
  host?: string;
  /** Redis port */
  port?: number;
  /** Redis password */
  password?: string;
  /** Redis database number */
  db?: number;
  /** Unique worker ID (defaults to hostname-pid) */
  workerId?: string;
  /** Consumer group name */
  consumerGroup?: string;
  /** Block timeout for XREADGROUP in milliseconds */
  blockTimeout?: number;
  /** Reclaimer check interval in milliseconds */
  reclaimerInterval?: number;
  /** Message idle time before reclaim in milliseconds */
  idleTimeout?: number;
  /** Max delivery attempts before dead-letter */
  maxDeliveries?: number;
  /** Grace period for shutdown in milliseconds */
  gracePeriod?: number;
  /** Prefetch limit: max messages to pull per XREADGROUP call (backpressure) */
  prefetch?: number;
  /** Max concurrent tasks before waiting (backpressure) */
  concurrency?: number;
}

/**
 * Get default worker ID based on hostname and PID.
 */
export function getDefaultWorkerId(): string {
  return `${hostname()}-${process.pid}`;
}

/**
 * Default worker configuration values.
 */
export const DEFAULT_WORKER_CONFIG: Required<Omit<WorkerConfig, 'workerId'>> & {
  workerId: string;
} = {
  host: 'localhost',
  port: 6379,
  password: '',
  db: 0,
  workerId: '',
  consumerGroup: 'backstage-workers',
  blockTimeout: 5000,
  reclaimerInterval: 30000,
  idleTimeout: 60000,
  maxDeliveries: 5,
  gracePeriod: 30000,
  prefetch: 10,
  concurrency: 50,
};

// =============================================================================
// Enqueue Options
// =============================================================================

/**
 * Options for enqueueing a task.
 */
export interface EnqueueOptions {
  /** Priority level */
  priority?: Priority;
  /** Delay before execution in milliseconds */
  delay?: number;
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Parse key-value pairs from Redis Stream message fields.
 */
export function parseFields(fields: unknown[]): Record<string, string> {
  const data: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) {
    const key = fields[i];
    const value = fields[i + 1];
    if (typeof key === 'string' && typeof value === 'string') {
      data[key] = value;
    }
  }
  return data;
}
