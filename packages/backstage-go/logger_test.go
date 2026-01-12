package backstage

import (
	"bytes"
	"log/slog"
	"testing"
)

func TestLoggerLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("Test", LoggerConfig{
		Level:  slog.LevelDebug,
		Output: &buf,
	})

	logger.Debug("debug msg")
	logger.Info("info msg")
	logger.Warn("warn msg")
	logger.Error("error msg")

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("debug msg")) {
		t.Error("should contain debug")
	}
	if !bytes.Contains([]byte(output), []byte("info msg")) {
		t.Error("should contain info")
	}
	if !bytes.Contains([]byte(output), []byte("warn msg")) {
		t.Error("should contain warn")
	}
	if !bytes.Contains([]byte(output), []byte("error msg")) {
		t.Error("should contain error")
	}
}

func TestLoggerFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("Test", LoggerConfig{
		Level:  slog.LevelWarn,
		Output: &buf,
	})

	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")

	output := buf.String()

	if bytes.Contains([]byte(output), []byte("debug")) {
		t.Error("debug should be filtered")
	}
	if bytes.Contains([]byte(output), []byte("info")) {
		t.Error("info should be filtered")
	}
	if !bytes.Contains([]byte(output), []byte("warn")) {
		t.Error("warn should appear")
	}
	if !bytes.Contains([]byte(output), []byte("error")) {
		t.Error("error should appear")
	}
}

func TestLoggerSilent(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("Test", LoggerConfig{
		Level:  slog.LevelInfo,
		Output: &buf,
		Silent: true,
	})

	logger.Info("should not appear")

	if buf.Len() > 0 {
		t.Error("silent mode should produce no output")
	}
}

func TestLoggerCustomHandler(t *testing.T) {
	var called bool
	var capturedMsg string

	logger := NewLogger("Test", LoggerConfig{
		Level: slog.LevelInfo,
		Handler: func(level slog.Level, msg string, attrs ...slog.Attr) {
			called = true
			capturedMsg = msg
		},
		Silent: true,
	})

	logger.Info("test message")

	if !called {
		t.Error("handler should be called")
	}
	if capturedMsg != "test message" {
		t.Errorf("expected 'test message', got '%s'", capturedMsg)
	}
}

func TestLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger("Test", LoggerConfig{
		Level:  slog.LevelInfo,
		Output: &buf,
	})

	childLogger := logger.With("request_id", "abc123")
	childLogger.Info("request handled")

	output := buf.String()
	if !bytes.Contains([]byte(output), []byte("request_id")) {
		t.Error("should contain request_id attribute")
	}
}

func TestDefaultLogger(t *testing.T) {
	if DefaultLogger == nil {
		t.Error("DefaultLogger should not be nil")
	}
}
