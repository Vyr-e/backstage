/**
 * Backstage SDK
 *
 * A Redis Streams-based background worker system for TypeScript.
 */

// Export everything from core
export * from './core';

// Export Worker and Scheduler (high-level abstractions)
export { Worker } from './worker';
export { Scheduler, type SchedulerConfig } from './scheduler';
