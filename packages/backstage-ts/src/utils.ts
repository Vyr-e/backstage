/**
 * Backstage utilities for queue inspection and management.
 */

import type { RedisClient } from './types';
import type { Queue } from './queue';

export interface QueueInfo {
  name: string;
  pending: number;
  scheduled: number;
  deadLetter: number;
}

export interface QueuesInfo {
  queues: QueueInfo[];
  totalPending: number;
  totalScheduled: number;
  totalDeadLetter: number;
}

/**
 * Get info about all queues.
 */
export async function inspect(
  redis: RedisClient,
  queues: Queue[]
): Promise<QueuesInfo> {
  const queueInfos: QueueInfo[] = [];

  for (const queue of queues) {
    const pending = await numPendingTasks(redis, queue);
    const scheduled = await numScheduledTasks(redis, queue);
    const deadLetter = await numDeadLetterTasks(redis, queue);

    queueInfos.push({
      name: queue.name,
      pending,
      scheduled,
      deadLetter,
    });
  }

  return {
    queues: queueInfos,
    totalPending: queueInfos.reduce((sum, q) => sum + q.pending, 0),
    totalScheduled: queueInfos.reduce((sum, q) => sum + q.scheduled, 0),
    totalDeadLetter: queueInfos.reduce((sum, q) => sum + q.deadLetter, 0),
  };
}

/**
 * Count pending tasks in a queue.
 */
export async function numPendingTasks(
  redis: RedisClient,
  queue: Queue
): Promise<number> {
  const result = await redis.send('XLEN', [queue.streamKey]);
  return (result as number) ?? 0;
}

/**
 * Count scheduled tasks for a queue.
 */
export async function numScheduledTasks(
  redis: RedisClient,
  queue: Queue
): Promise<number> {
  const result = await redis.send('ZCARD', [queue.scheduledKey]);
  return (result as number) ?? 0;
}

/**
 * Count dead-letter tasks for a queue.
 */
export async function numDeadLetterTasks(
  redis: RedisClient,
  queue: Queue
): Promise<number> {
  const result = await redis.send('XLEN', [queue.deadLetterKey]);
  return (result as number) ?? 0;
}

/**
 * Get pending tasks from a queue.
 */
export async function getPendingTasks(
  redis: RedisClient,
  queue: Queue,
  count: number = 10
): Promise<unknown[]> {
  const result = await redis.send('XRANGE', [
    queue.streamKey,
    '-',
    '+',
    'COUNT',
    String(count),
  ]);
  return (result as unknown[]) ?? [];
}

/**
 * Get scheduled tasks for a queue.
 */
export async function getScheduledTasks(
  redis: RedisClient,
  queue: Queue,
  count: number = 10
): Promise<unknown[]> {
  const result = await redis.send('ZRANGE', [
    queue.scheduledKey,
    '0',
    String(count - 1),
    'WITHSCORES',
  ]);
  return (result as unknown[]) ?? [];
}

/**
 * Purge all pending tasks from a queue.
 */
export async function purgeQueue(
  redis: RedisClient,
  queue: Queue
): Promise<number> {
  const len = await numPendingTasks(redis, queue);
  if (len > 0) {
    await redis.send('DEL', [queue.streamKey]);
  }
  return len;
}

/**
 * Purge all scheduled tasks for a queue.
 */
export async function purgeScheduled(
  redis: RedisClient,
  queue: Queue
): Promise<number> {
  const len = await numScheduledTasks(redis, queue);
  if (len > 0) {
    await redis.send('DEL', [queue.scheduledKey]);
  }
  return len;
}

/**
 * Purge dead-letter queue.
 */
export async function purgeDeadLetter(
  redis: RedisClient,
  queue: Queue
): Promise<number> {
  const len = await numDeadLetterTasks(redis, queue);
  if (len > 0) {
    await redis.send('DEL', [queue.deadLetterKey]);
  }
  return len;
}

/**
 * Get consumer group info.
 */
export async function getConsumerGroups(
  redis: RedisClient,
  queue: Queue
): Promise<unknown[]> {
  try {
    const result = await redis.send('XINFO', ['GROUPS', queue.streamKey]);
    return (result as unknown[]) ?? [];
  } catch {
    return [];
  }
}

/**
 * Get connected consumers in a group.
 */
export async function getConsumers(
  redis: RedisClient,
  queue: Queue,
  groupName: string
): Promise<unknown[]> {
  try {
    const result = await redis.send('XINFO', [
      'CONSUMERS',
      queue.streamKey,
      groupName,
    ]);
    return (result as unknown[]) ?? [];
  } catch {
    return [];
  }
}
