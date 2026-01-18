/**
 * Backstage SDK - Queue
 *
 * Queue abstraction with priority and timeout configuration.
 */

import { BackstageError } from './exceptions';

/**
 * Queue configuration options.
 */
export interface QueueOptions {
  /** Queue priority (lower number = higher priority) */
  priority?: number;
  /** Soft timeout in milliseconds */
  softTimeout?: number;
  /** Hard timeout in milliseconds */
  hardTimeout?: number;
  /** Maximum retry attempts */
  maxRetries?: number;
}

/**
 * Queue class representing a task queue with priority and timeouts.
 *
 * @example
 * ```typescript
 * const queue = new Queue('emails', { priority: 1 });
 * ```
 */
export class Queue {
  public readonly name: string;
  public readonly prefix: string;
  public priority: number;
  /** Soft timeout in milliseconds */
  public softTimeout?: number;
  /** Hard timeout in milliseconds */
  public hardTimeout?: number;
  /** Maximum retry attempts */
  public maxRetries?: number;

  /**
   * Create a new Queue definition.
   *
   * @param name - Unique name of the queue (alphanumeric, underscores, dashes)
   * @param options - Configuration options like priority and timeouts
   * @throws {BackstageError} If name is invalid or timeouts are inconsistent
   */
  constructor(name: string, options: QueueOptions = {}) {
    // Sanitize name
    this.name = name.replace(/[^a-zA-Z0-9_.-]/g, '');
    if (!this.name) {
      throw new BackstageError('Queue name cannot be empty');
    }

    this.prefix = 'backstage';
    this.priority = options.priority ?? -1;
    this.softTimeout = options.softTimeout;
    this.hardTimeout = options.hardTimeout;
    this.maxRetries = options.maxRetries;

    // Validate timeouts
    if (
      this.softTimeout &&
      this.hardTimeout &&
      this.hardTimeout <= this.softTimeout
    ) {
      throw new BackstageError(
        `Queue hard timeout (${this.hardTimeout}) must be greater than soft timeout (${this.softTimeout})`,
      );
    }
  }

  /**
   * Set default priority if not explicitly set.
   * Internal use primarily.
   *
   * @param lowestPriority - The lowest priority level currently in use
   */
  setDefaultPriority(lowestPriority: number): void {
    if (this.priority < 0) {
      this.priority = lowestPriority + 1;
    }
  }

  /**
   * Get the Redis stream key for this queue.
   */
  get streamKey(): string {
    return `${this.prefix}:${this.name}`;
  }

  /**
   * Get the scheduled tasks sorted set key for this queue.
   */
  get scheduledKey(): string {
    return `${this.prefix}:scheduled:${this.name}`;
  }

  /**
   * Get the dead-letter stream key for this queue.
   */
  get deadLetterKey(): string {
    return `${this.prefix}:${this.name}:dead-letter`;
  }

  /**
   * Create a Queue from various input types.
   * Helper for parsing queue definitions from config.
   *
   * @param input - Queue instance, name string, or tuple [name, priority]
   * @param existingQueues - Optional map of validation against existing queues
   * @returns A Queue instance
   */
  static create(
    input: Queue | string | [string, number] | [number, string],
    existingQueues?: Map<string, Queue>,
  ): Queue {
    if (input instanceof Queue) {
      if (existingQueues && !existingQueues.has(input.name)) {
        throw new BackstageError(`Unknown queue: ${input.name}`);
      }
      return input;
    }

    if (typeof input === 'string') {
      if (existingQueues) {
        const existing = existingQueues.get(input);
        if (existing) return existing;
        throw new BackstageError(`Unknown queue: ${input}`);
      }
      return new Queue(input);
    }

    if (Array.isArray(input) && input.length === 2) {
      const [first, second] = input;
      if (typeof first === 'number') {
        return new Queue(second as string, { priority: first });
      } else {
        return new Queue(first, { priority: second as number });
      }
    }

    throw new BackstageError(`Invalid queue input: ${JSON.stringify(input)}`);
  }
}

/**
 * Default queues for the multi-priority system.
 */
export const DefaultQueues = {
  URGENT: new Queue('urgent', { priority: 1 }),
  DEFAULT: new Queue('default', { priority: 2 }),
  LOW: new Queue('low', { priority: 3 }),
} as const;
