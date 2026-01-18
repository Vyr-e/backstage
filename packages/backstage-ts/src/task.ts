/**
 * Backstage SDK - Task
 *
 * Task class wrapping handlers with convenience methods for
 * enqueueing, scheduling, and broadcasting.
 */

import type { Queue } from './queue';
import type { WorkflowInstruction } from './types';

/**
 * Task handler function type.
 */
export type TaskFn<TData = unknown, TResult = void | WorkflowInstruction> = (
  data: TData,
) => Promise<TResult>;

/**
 * Task registration options.
 */
export interface TaskOptions {
  /** Task name (defaults to function name) */
  name?: string;
  /** Target queue */
  queue?: Queue | string;
  /** Soft timeout in milliseconds */
  softTimeout?: number;
  /** Hard timeout in milliseconds */
  hardTimeout?: number;
  /** Maximum retry attempts */
  maxRetries?: number;
}

/**
 * Interface for the Backstage instance (to avoid circular deps).
 */
export interface BackstageInstance {
  enqueue(
    taskName: string,
    payload: unknown,
    queueName?: string,
  ): Promise<string | null>;
  schedule(
    taskName: string,
    payload: unknown,
    delayMs: number,
    queueName?: string,
  ): Promise<string | null>;
  broadcast(taskName: string, payload: unknown): Promise<string>;
}

/**
 * Task wrapper that provides strict typing and convenience methods.
 * Allows defining tasks separately from the worker instance.
 *
 * @example
 * ```typescript
 * export const sendEmail = new Task(async (data: { to: string }) => {
 *   await emailClient.send(data.to);
 * }, { name: 'send-email' });
 *
 * // Later
 * await sendEmail.enqueue({ to: 'bob@example.com' });
 * ```
 */
export class Task<TData = unknown, TResult = void | WorkflowInstruction> {
  public readonly name: string;
  public readonly fn: TaskFn<TData, TResult>;
  public readonly queue?: Queue | string;
  public readonly softTimeout?: number;
  public readonly hardTimeout?: number;
  public readonly maxRetries: number;

  private backstage?: BackstageInstance;

  /**
   * Create a new defined task.
   *
   * @param fn - The async function that performs the work
   * @param options - Task options (name, queue, etc.)
   */
  constructor(fn: TaskFn<TData, TResult>, options: TaskOptions = {}) {
    // Name is required
    const name = options.name || fn.name;
    if (!name) {
      throw new Error(
        'Task name is required. Either provide it in options or use a named function.',
      );
    }

    this.name = name;
    this.fn = fn;
    this.queue = options.queue;
    this.softTimeout = options.softTimeout;
    this.hardTimeout = options.hardTimeout;
    this.maxRetries = options.maxRetries ?? 0;

    // Validate timeouts
    if (
      this.softTimeout &&
      this.hardTimeout &&
      this.hardTimeout <= this.softTimeout
    ) {
      throw new Error(
        `Task hard timeout (${this.hardTimeout}) must be greater than soft timeout (${this.softTimeout})`,
      );
    }
  }

  /**
   * Bind this task to a Backstage instance.
   * Called internally when registering the task.
   *
   * @param backstage - The Backstage (Worker) instance
   */
  bind(backstage: BackstageInstance): void {
    this.backstage = backstage;
  }

  /**
   * Enqueue this task for immediate execution.
   *
   * @param data - The type-safe payload for this task
   * @returns Message ID, or null if deduplicated
   */
  async enqueue(data?: TData): Promise<string | null> {
    if (!this.backstage) {
      throw new Error('Task not bound to a Backstage instance');
    }
    const queueName =
      typeof this.queue === 'string' ? this.queue : this.queue?.name;
    return this.backstage.enqueue(this.name, data, queueName);
  }

  /**
   * Schedule this task for delayed execution.
   *
   * @param delayMs - Delay in milliseconds before execution
   * @param data - The type-safe payload
   * @returns Message ID, or null if deduplicated
   */
  async enqueueAfterDelay(
    delayMs: number,
    data?: TData,
  ): Promise<string | null> {
    if (!this.backstage) {
      throw new Error('Task not bound to a Backstage instance');
    }
    const queueName =
      typeof this.queue === 'string' ? this.queue : this.queue?.name;
    return this.backstage.schedule(this.name, data, delayMs, queueName);
  }

  /**
   * Schedule this task for a specific date/time.
   *
   * @param date - The Date object for when to execute
   * @param data - The type-safe payload
   * @returns Message ID
   */
  async enqueueAt(date: Date, data?: TData): Promise<string | null> {
    const delayMs = date.getTime() - Date.now();
    if (delayMs < 0) {
      return this.enqueue(data);
    }
    return this.enqueueAfterDelay(delayMs, data);
  }

  /**
   * Broadcast this task to all workers.
   * Useful for cache invalidation or configuration updates.
   *
   * @param data - The payload
   * @returns Message ID
   */
  async broadcast(data?: TData): Promise<string> {
    if (!this.backstage) {
      throw new Error('Task not bound to a Backstage instance');
    }
    return this.backstage.broadcast(this.name, data);
  }
}
