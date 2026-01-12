package backstage

import (
	"testing"
)

func TestErrors(t *testing.T) {
	tests := []struct {
		err      error
		expected string
	}{
		{ErrTaskNotFound, "task not found"},
		{ErrQueueNotFound, "queue not found"},
		{ErrSoftTimeout, "soft timeout"},
		{ErrHardTimeout, "hard timeout"},
		{ErrPreventExecution, "task execution prevented"},
		{ErrInvalidCron, "invalid cron schedule"},
		{ErrRedisConnection, "redis connection error"},
	}

	for _, tc := range tests {
		if tc.err.Error() != tc.expected {
			t.Errorf("expected '%s', got '%s'", tc.expected, tc.err.Error())
		}
	}
}

func TestBackstageError(t *testing.T) {
	err := NewError(ErrTaskNotFound, "task 'foo' not found")

	if err.Error() != "task 'foo' not found" {
		t.Errorf("unexpected error message: %s", err.Error())
	}

	if err.Unwrap() != ErrTaskNotFound {
		t.Error("unwrap should return original error")
	}
}

func TestBackstageErrorWithTaskID(t *testing.T) {
	err := &BackstageError{
		Err:     ErrSoftTimeout,
		Message: "task timed out",
		TaskID:  "12345-0",
	}

	expected := "task timed out (task: 12345-0)"
	if err.Error() != expected {
		t.Errorf("expected '%s', got '%s'", expected, err.Error())
	}
}
