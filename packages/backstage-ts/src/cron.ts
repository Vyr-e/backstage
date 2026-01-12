/**
 * Backstage cron task definitions.
 */

import { InvalidCronSchedule } from './exceptions';
import type { Queue } from './queue';

interface CronFields {
  minute: number[];
  hour: number[];
  dayOfMonth: number[];
  month: number[];
  dayOfWeek: number[];
}

export class CronTask {
  public readonly schedule: string;
  public readonly taskName: string;
  public readonly queue?: Queue;
  public readonly args: unknown[];

  private fields: CronFields;
  private lastRun?: Date;

  constructor(
    schedule: string,
    taskName: string,
    queue?: Queue,
    args: unknown[] = []
  ) {
    this.schedule = schedule;
    this.taskName = taskName;
    this.queue = queue;
    this.args = args;
    this.fields = this.parseCron(schedule);
  }

  private parseCron(schedule: string): CronFields {
    const parts = schedule.trim().split(/\s+/);

    if (parts.length !== 5) {
      throw new InvalidCronSchedule(
        schedule,
        'Expected 5 fields: minute hour dayOfMonth month dayOfWeek'
      );
    }

    return {
      minute: this.parseField(parts[0]!, 0, 59),
      hour: this.parseField(parts[1]!, 0, 23),
      dayOfMonth: this.parseField(parts[2]!, 1, 31),
      month: this.parseField(parts[3]!, 1, 12),
      dayOfWeek: this.parseField(parts[4]!, 0, 6),
    };
  }

  private parseField(field: string, min: number, max: number): number[] {
    const values: Set<number> = new Set();
    const parts = field.split(',');

    for (const part of parts) {
      if (part === '*') {
        for (let i = min; i <= max; i++) {
          values.add(i);
        }
      } else if (part.includes('/')) {
        const [range, stepStr] = part.split('/');
        const step = parseInt(stepStr ?? '1', 10);
        if (isNaN(step) || step <= 0) {
          throw new InvalidCronSchedule(this.schedule, `Invalid step: ${part}`);
        }

        let start = min;
        let end = max;

        if (range && range !== '*') {
          if (range.includes('-')) {
            const [s, e] = range.split('-');
            start = parseInt(s ?? String(min), 10);
            end = parseInt(e ?? String(max), 10);
          } else {
            start = parseInt(range, 10);
          }
        }

        for (let i = start; i <= end; i += step) {
          values.add(i);
        }
      } else if (part.includes('-')) {
        const [startStr, endStr] = part.split('-');
        const start = parseInt(startStr ?? String(min), 10);
        const end = parseInt(endStr ?? String(max), 10);

        if (isNaN(start) || isNaN(end)) {
          throw new InvalidCronSchedule(
            this.schedule,
            `Invalid range: ${part}`
          );
        }

        for (let i = start; i <= end; i++) {
          values.add(i);
        }
      } else {
        const value = parseInt(part, 10);
        if (isNaN(value)) {
          throw new InvalidCronSchedule(
            this.schedule,
            `Invalid value: ${part}`
          );
        }
        values.add(value);
      }
    }

    return Array.from(values).sort((a, b) => a - b);
  }

  getNextRun(after: Date = new Date()): Date {
    const next = new Date(after.getTime());
    next.setSeconds(0, 0);
    next.setMinutes(next.getMinutes() + 1);

    const maxIterations = 525600;
    let iterations = 0;

    while (iterations < maxIterations) {
      const minute = next.getMinutes();
      const hour = next.getHours();
      const dayOfMonth = next.getDate();
      const month = next.getMonth() + 1;
      const dayOfWeek = next.getDay();

      if (
        this.fields.minute.includes(minute) &&
        this.fields.hour.includes(hour) &&
        this.fields.dayOfMonth.includes(dayOfMonth) &&
        this.fields.month.includes(month) &&
        this.fields.dayOfWeek.includes(dayOfWeek)
      ) {
        return next;
      }

      next.setMinutes(next.getMinutes() + 1);
      iterations++;
    }

    throw new InvalidCronSchedule(
      this.schedule,
      'Could not find next run within a year'
    );
  }

  shouldRun(now: Date = new Date()): boolean {
    if (!this.lastRun) return true;
    const next = this.getNextRun(this.lastRun);
    return now >= next;
  }

  markRun(at: Date = new Date()): void {
    this.lastRun = at;
  }

  reset(to?: Date): void {
    this.lastRun = to;
  }
}
