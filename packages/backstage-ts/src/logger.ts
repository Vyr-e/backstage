/**
 * Backstage SDK - Logger
 *
 * Winston-based logging with optional disable and custom handlers.
 */

import winston from 'winston';

export enum LogLevel {
  DEBUG = 'debug',
  INFO = 'info',
  WARN = 'warn',
  ERROR = 'error',
}

export interface LogEntry {
  level: LogLevel;
  message: string;
  timestamp: string;
  payload?: Record<string, unknown>;
  error?: string;
  stack?: string;
}

export type LogHandler = (entry: LogEntry) => void | Promise<void>;

export interface LoggerConfig {
  level?: LogLevel;
  file?: string;
  isChild?: boolean;
  isScheduler?: boolean;
  handler?: LogHandler;
  /** Disable all logging */
  silent?: boolean;
}

export class Logger {
  private winston: winston.Logger;
  private handler?: LogHandler;
  private silent: boolean;
  private isChild: boolean;
  private prefix: string;

  constructor(config: LoggerConfig = {}) {
    this.handler = config.handler;
    this.silent = config.silent ?? false;
    this.isChild = config.isChild ?? false;
    this.prefix = config.isScheduler ? 'Scheduler' : 'Worker';

    const transports: winston.transport[] = [];

    if (!this.silent) {
      transports.push(
        new winston.transports.Console({
          format: winston.format.combine(
            winston.format.timestamp(),
            winston.format.printf(({ timestamp, level, message }) => {
              return `${timestamp} ${level.toUpperCase().padEnd(5)} [${
                this.prefix
              }]: ${message}`;
            })
          ),
        })
      );
    }

    if (config.file) {
      transports.push(
        new winston.transports.File({
          filename: config.file,
          format: winston.format.combine(
            winston.format.timestamp(),
            winston.format.json()
          ),
        })
      );
    }

    this.winston = winston.createLogger({
      level: config.level ?? LogLevel.INFO,
      silent: this.silent && !config.file && !this.handler,
      transports,
    });
  }

  setHandler(handler: LogHandler): void {
    this.handler = handler;
  }

  setSilent(silent: boolean): void {
    this.silent = silent;
    this.winston.silent = silent && !this.handler;
  }

  private callHandler(entry: LogEntry): void {
    if (this.handler) {
      try {
        this.handler(entry);
      } catch {
        // Don't let handler errors break logging
      }
    }
  }

  debug(message: string, payload?: Record<string, unknown>): void {
    const entry: LogEntry = {
      level: LogLevel.DEBUG,
      message,
      timestamp: new Date().toISOString(),
      payload,
    };
    this.callHandler(entry);
    this.winston.debug(message, { payload });
  }

  info(message: string, payload?: Record<string, unknown>): void {
    const entry: LogEntry = {
      level: LogLevel.INFO,
      message,
      timestamp: new Date().toISOString(),
      payload,
    };
    this.callHandler(entry);
    this.winston.info(message, { payload });
  }

  warn(message: string, payload?: Record<string, unknown>): void {
    const entry: LogEntry = {
      level: LogLevel.WARN,
      message,
      timestamp: new Date().toISOString(),
      payload,
    };
    this.callHandler(entry);
    this.winston.warn(message, { payload });
  }

  error(message: string | Error, payload?: Record<string, unknown>): void {
    if (message instanceof Error) {
      const entry: LogEntry = {
        level: LogLevel.ERROR,
        message: message.message,
        timestamp: new Date().toISOString(),
        error: message.name,
        stack: message.stack,
        payload,
      };
      this.callHandler(entry);
      this.winston.error(message.message, {
        error: message.name,
        stack: message.stack,
        payload,
      });
    } else {
      const entry: LogEntry = {
        level: LogLevel.ERROR,
        message,
        timestamp: new Date().toISOString(),
        payload,
      };
      this.callHandler(entry);
      this.winston.error(message, { payload });
    }
  }

  async flush(): Promise<void> {
    // Winston handles flushing internally
  }

  async close(): Promise<void> {
    this.winston.close();
  }
}

export function createLogger(config?: LoggerConfig): Logger {
  return new Logger(config);
}
