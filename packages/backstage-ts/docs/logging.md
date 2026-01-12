# Logging

Backstage uses Winston for structured logging with optional custom handlers.

## Default (Console)

```typescript
const worker = new Worker(); // Logs to console
```

## Silent Mode

```typescript
const worker = new Worker({}, { silent: true });
```

## Custom Handler

Send logs to external services:

```typescript
const worker = new Worker(
  {},
  {
    handler: (entry) => {
      // entry: { level, message, timestamp, payload?, error?, stack? }
      datadogLogs.log(entry);
    },
  }
);
```

## Log Levels

```typescript
import { LogLevel } from '@backstage/core';

const worker = new Worker(
  {},
  {
    level: LogLevel.DEBUG, // DEBUG, INFO, WARN, ERROR
  }
);
```

## Standalone Logger

```typescript
import { createLogger, LogLevel, type LogHandler } from '@backstage/core';

const handler: LogHandler = (entry) => {
  fetch('https://logs.example.com', {
    method: 'POST',
    body: JSON.stringify(entry),
  });
};

const logger = createLogger({
  level: LogLevel.INFO,
  handler,
  silent: false,
});

logger.info('Processing started', { orderId: '123' });
logger.error(new Error('Payment failed'));
```

## Log Entry Structure

```typescript
interface LogEntry {
  level: 'debug' | 'info' | 'warn' | 'error';
  message: string;
  timestamp: string; // ISO 8601
  payload?: Record<string, unknown>;
  error?: string; // Error name
  stack?: string; // Stack trace
}
```

## File Output

```typescript
const logger = createLogger({
  file: '/var/log/backstage.log',
});
```

Writes JSON lines to file (useful for log aggregators).
