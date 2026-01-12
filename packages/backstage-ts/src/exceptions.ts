/**
 * Backstage SDK - Exceptions
 *
 * Custom exception classes for task execution control.
 */

/**
 * Base Backstage error class.
 */
export class BackstageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BackstageError';
    Object.setPrototypeOf(this, BackstageError.prototype);
  }
}

/**
 * Soft timeout exception.
 *
 * Thrown when a task exceeds its soft timeout.
 * The task can be retried based on maxRetries configuration.
 */
export class SoftTimeout extends BackstageError {
  constructor(message = 'soft timeout') {
    super(message);
    this.name = 'SoftTimeout';
    Object.setPrototypeOf(this, SoftTimeout.prototype);
  }
}

/**
 * Hard timeout exception.
 *
 * Thrown when a task exceeds its hard timeout.
 * The task is terminated and moved to dead-letter.
 */
export class HardTimeout extends BackstageError {
  constructor(message = 'hard timeout') {
    super(message);
    this.name = 'HardTimeout';
    Object.setPrototypeOf(this, HardTimeout.prototype);
  }
}

/**
 * Prevent task execution exception.
 *
 * Thrown from beforeTaskStarted callback to skip a task
 * without treating it as an error.
 */
export class PreventTaskExecution extends BackstageError {
  constructor(message = 'task execution prevented') {
    super(message);
    this.name = 'PreventTaskExecution';
    Object.setPrototypeOf(this, PreventTaskExecution.prototype);
  }
}

/**
 * Task not found exception.
 */
export class TaskNotFound extends BackstageError {
  public taskName: string;

  constructor(taskName: string) {
    super(`Task not found: ${taskName}`);
    this.name = 'TaskNotFound';
    this.taskName = taskName;
    Object.setPrototypeOf(this, TaskNotFound.prototype);
  }
}

/**
 * Queue not found exception.
 */
export class QueueNotFound extends BackstageError {
  public queueName: string;

  constructor(queueName: string) {
    super(`Queue not found: ${queueName}`);
    this.name = 'QueueNotFound';
    this.queueName = queueName;
    Object.setPrototypeOf(this, QueueNotFound.prototype);
  }
}

/**
 * Invalid cron schedule exception.
 */
export class InvalidCronSchedule extends BackstageError {
  public schedule: string;

  constructor(schedule: string, reason?: string) {
    super(`Invalid cron schedule: ${schedule}${reason ? ` - ${reason}` : ''}`);
    this.name = 'InvalidCronSchedule';
    this.schedule = schedule;
    Object.setPrototypeOf(this, InvalidCronSchedule.prototype);
  }
}

/**
 * Redis connection error.
 */
export class RedisConnectionError extends BackstageError {
  constructor(message: string) {
    super(`Redis connection error: ${message}`);
    this.name = 'RedisConnectionError';
    Object.setPrototypeOf(this, RedisConnectionError.prototype);
  }
}
