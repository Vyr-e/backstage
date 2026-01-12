/**
 * Cron parsing edge cases
 */
import { describe, test, expect } from 'bun:test';
import { CronTask } from '../src/cron';

describe('CronTask Edge Cases', () => {
  test('every minute', () => {
    const task = new CronTask('* * * * *', 'every.minute');
    const now = new Date('2024-01-10T12:00:00Z');
    const next = task.getNextRun(now);
    expect(next.getMinutes()).toBe(1);
  });

  test('every 5 minutes', () => {
    const task = new CronTask('*/5 * * * *', 'every.5.minutes');
    const now = new Date('2024-01-10T12:03:00Z');
    const next = task.getNextRun(now);
    expect(next.getMinutes()).toBe(5);
  });

  test('every hour', () => {
    const task = new CronTask('0 * * * *', 'every.hour');
    const now = new Date('2024-01-10T12:30:00Z');
    const next = task.getNextRun(now);
    expect(next.getMinutes()).toBe(0);
    expect(next.getHours()).toBe(13);
  });

  test('daily at midnight', () => {
    const task = new CronTask('0 0 * * *', 'daily.midnight');
    const now = new Date('2024-01-10T12:00:00Z');
    const next = task.getNextRun(now);
    expect(next.getHours()).toBe(0);
    expect(next.getMinutes()).toBe(0);
  });

  test('weekdays only', () => {
    const task = new CronTask('0 9 * * 1-5', 'weekdays.9am');
    const friday = new Date('2024-01-12T08:00:00Z'); // Friday
    const next = task.getNextRun(friday);
    expect(next.getDay()).toBeGreaterThanOrEqual(1);
    expect(next.getDay()).toBeLessThanOrEqual(5);
  });

  test('weekends only', () => {
    const task = new CronTask('0 10 * * 0,6', 'weekends');
    const wednesday = new Date('2024-01-10T08:00:00Z');
    const next = task.getNextRun(wednesday);
    expect([0, 6]).toContain(next.getDay());
  });

  test('specific day of month', () => {
    const task = new CronTask('0 0 15 * *', 'monthly.15th');
    const now = new Date('2024-01-10T12:00:00Z');
    const next = task.getNextRun(now);
    expect(next.getDate()).toBe(15);
  });

  test('specific months', () => {
    const task = new CronTask('0 0 1 1,7 *', 'jan.jul');
    const now = new Date('2024-03-01T00:00:00Z');
    const next = task.getNextRun(now);
    expect([1, 7]).toContain(next.getMonth() + 1);
  });

  test('complex schedule', () => {
    const task = new CronTask('30 9-17 * * 1-5', 'business.hours');
    const now = new Date('2024-01-10T08:00:00Z'); // Wednesday 8am
    const next = task.getNextRun(now);
    expect(next.getMinutes()).toBe(30);
    expect(next.getHours()).toBeGreaterThanOrEqual(9);
    expect(next.getHours()).toBeLessThanOrEqual(17);
  });

  test('step in range', () => {
    const task = new CronTask('0-30/10 * * * *', 'step.range');
    const now = new Date('2024-01-10T12:05:00Z');
    const next = task.getNextRun(now);
    expect([0, 10, 20, 30]).toContain(next.getMinutes());
  });

  test('throws on invalid schedule - wrong field count', () => {
    expect(() => new CronTask('* * *', 'invalid')).toThrow();
  });

  test('throws on invalid schedule - bad step', () => {
    expect(() => new CronTask('*/0 * * * *', 'zero.step')).toThrow();
  });

  test('throws on invalid schedule - invalid value', () => {
    expect(() => new CronTask('abc * * * *', 'letters')).toThrow();
  });

  test('marks run correctly', () => {
    const task = new CronTask('* * * * *', 'test');
    const now = new Date();
    task.markRun(now);
    // Should calculate next from marked time
    const next = task.getNextRun(now);
    expect(next > now).toBe(true);
  });

  test('handles year boundary', () => {
    const task = new CronTask('0 0 1 1 *', 'new.year');
    const now = new Date('2024-12-31T23:00:00Z');
    const next = task.getNextRun(now);
    expect(next.getFullYear()).toBe(2025);
  });
});
