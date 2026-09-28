import type { BackoffConfig } from './types';

/**
 * Shared backoff calculation used by core retry decisions and the Redis
 * reclaim loop. Matches today's Reclaimer.calculateBackoff semantics.
 *
 * `deliveryCount` is the PEL delivery count (1 = first delivery).
 * Retries so far = max(0, deliveryCount - 1).
 */
export function computeBackoff(
  config: BackoffConfig,
  deliveryCount: number,
): number {
  const retries = Math.max(0, deliveryCount - 1);

  if (config.type === 'fixed') {
    return config.delay;
  }

  if (config.type === 'exponential') {
    const delay = config.delay * Math.pow(2, Math.max(0, retries - 1));
    const max = config.maxDelay ?? 3_600_000;
    return Math.min(delay, max);
  }

  return 0;
}
