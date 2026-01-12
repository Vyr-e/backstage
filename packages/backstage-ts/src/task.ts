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
  data: TData
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
    queueName?: string
  ): Promise<string>;
  schedule(
    taskName: string,
    payload: unknown,
    delayMs: number,
    queueName?: string
  ): Promise<string>;
  broadcast(taskName: string, payload: unknown): Promise<string>;
}

/**
 * Task class representing a registered task with its handler.
 */
export class Task<TData = unknown, TResult = void | WorkflowInstruction> {
  public readonly name: string;
  public readonly fn: TaskFn<TData, TResult>;
  public readonly queue?: Queue | string;
  public readonly softTimeout?: number;
  public readonly hardTimeout?: number;
  public readonly maxRetries: number;

  private backstage?: BackstageInstance;

  constructor(fn: TaskFn<TData, TResult>, options: TaskOptions = {}) {
    // Name is required
    const name = options.name || fn.name;
    if (!name) {
      throw new Error(
        'Task name is required. Either provide it in options or use a named function.'
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
        `Task hard timeout (${this.hardTimeout}) must be greater than soft timeout (${this.softTimeout})`
      );
    }
  }

  /**
   * Bind this task to a Backstage instance.
   * Called internally when registering the task.
   */
  bind(backstage: BackstageInstance): void {
    this.backstage = backstage;
  }

  /**
   * Enqueue this task for immediate execution.
   */
  async enqueue(data?: TData): Promise<string> {
    if (!this.backstage) {
      throw new Error('Task not bound to a Backstage instance');
    }
    const queueName =
      typeof this.queue === 'string' ? this.queue : this.queue?.name;
    return this.backstage.enqueue(this.name, data, queueName);
  }

  /**
   * Schedule this task for delayed execution.
   */
  async enqueueAfterDelay(delayMs: number, data?: TData): Promise<string> {
    if (!this.backstage) {
      throw new Error('Task not bound to a Backstage instance');
    }
    const queueName =
      typeof this.queue === 'string' ? this.queue : this.queue?.name;
    return this.backstage.schedule(this.name, data, delayMs, queueName);
  }

  /**
   * Schedule this task for a specific time.
   */
  async enqueueAt(date: Date, data?: TData): Promise<string> {
    const delayMs = date.getTime() - Date.now();
    if (delayMs < 0) {
      return this.enqueue(data);
    }
    return this.enqueueAfterDelay(delayMs, data);
  }

  /**
   * Broadcast this task to all workers.
   */
  async broadcast(data?: TData): Promise<string> {
    if (!this.backstage) {
      throw new Error('Task not bound to a Backstage instance');
    }
    return this.backstage.broadcast(this.name, data);
  }
}
