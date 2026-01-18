// Package backstage logger.
// Provides structured logging using log/slog.
package backstage

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// LogHandler is the interface for custom log handlers.
type LogHandler func(level slog.Level, msg string, attrs ...slog.Attr)

type Logger struct {
	slog    *slog.Logger
	handler LogHandler
	silent  bool
}

type LoggerConfig struct {
	Level   slog.Level
	Handler LogHandler
	Silent  bool
	Output  io.Writer
}

func NewLogger(prefix string, config ...LoggerConfig) *Logger {
	cfg := LoggerConfig{Level: slog.LevelInfo}
	if len(config) > 0 {
		cfg = config[0]
	}

	output := cfg.Output
	if output == nil {
		output = os.Stdout
	}

	opts := &slog.HandlerOptions{Level: cfg.Level}
	var slogHandler slog.Handler

	if cfg.Silent && cfg.Handler == nil {
		slogHandler = slog.NewTextHandler(io.Discard, opts)
	} else {
		slogHandler = slog.NewTextHandler(output, opts)
	}

	return &Logger{
		slog:    slog.New(slogHandler).With("component", prefix),
		handler: cfg.Handler,
		silent:  cfg.Silent,
	}
}

func (l *Logger) SetHandler(handler LogHandler) {
	l.handler = handler
}

func (l *Logger) SetSilent(silent bool) {
	l.silent = silent
}

func (l *Logger) Debug(msg string, args ...any) {
	if l.handler != nil {
		l.handler(slog.LevelDebug, msg)
	}
	if !l.silent {
		l.slog.Debug(msg, args...)
	}
}

func (l *Logger) Info(msg string, args ...any) {
	if l.handler != nil {
		l.handler(slog.LevelInfo, msg)
	}
	if !l.silent {
		l.slog.Info(msg, args...)
	}
}

func (l *Logger) Warn(msg string, args ...any) {
	if l.handler != nil {
		l.handler(slog.LevelWarn, msg)
	}
	if !l.silent {
		l.slog.Warn(msg, args...)
	}
}

func (l *Logger) Error(msg string, args ...any) {
	if l.handler != nil {
		l.handler(slog.LevelError, msg)
	}
	if !l.silent {
		l.slog.Error(msg, args...)
	}
}

func (l *Logger) With(args ...any) *Logger {
	return &Logger{
		slog:    l.slog.With(args...),
		handler: l.handler,
		silent:  l.silent,
	}
}

// LogEntry for structured logging compatibility
type LogEntry struct {
	Level   string
	Message string
	Attrs   map[string]any
}

// DefaultLogger for package-level logging
var DefaultLogger = NewLogger("Backstage")

// Context-aware logging
func (l *Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.slog.DebugContext(ctx, msg, args...)
}

func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.slog.InfoContext(ctx, msg, args...)
}

func (l *Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.slog.WarnContext(ctx, msg, args...)
}

func (l *Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.slog.ErrorContext(ctx, msg, args...)
}
