# Logger

Backstage uses Go's slog for structured logging.

## Default

```go
client := backstage.New(config)  // Logs to stdout
```

## Silent Mode

```go
logger := backstage.NewLogger("Worker", backstage.LoggerConfig{
    Silent: true,
})
```

## Custom Handler

Send logs to external services:

```go
logger := backstage.NewLogger("Worker", backstage.LoggerConfig{
    Handler: func(level slog.Level, msg string, attrs ...slog.Attr) {
        // Send to Loki, DataDog, etc.
        lokiClient.Push(level.String(), msg)
    },
})
```

## Log Level

```go
import "log/slog"

logger := backstage.NewLogger("Worker", backstage.LoggerConfig{
    Level: slog.LevelDebug,  // Debug, Info, Warn, Error
})
```

## Structured Logging

```go
logger.Info("Processing order", "orderId", "123", "amount", 99.99)
// Output: time=... level=INFO component=Worker msg="Processing order" orderId=123 amount=99.99
```

## Child Logger

Add context with `.With()`:

```go
reqLogger := logger.With("request_id", "abc123")
reqLogger.Info("Handling request")
// Output: ... component=Worker request_id=abc123 msg="Handling request"
```

## Context-Aware

```go
ctx := context.Background()
logger.InfoContext(ctx, "Processing", "key", "value")
```

## Custom Output

```go
var buf bytes.Buffer
logger := backstage.NewLogger("Test", backstage.LoggerConfig{
    Output: &buf,
})
```
