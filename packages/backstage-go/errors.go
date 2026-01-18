// Package backstage error definitions.
// Defines custom error types for task processing, such as PreventTaskExecution.
package backstage

import "errors"

var (
	ErrTaskNotFound     = errors.New("task not found")
	ErrQueueNotFound    = errors.New("queue not found")
	ErrSoftTimeout      = errors.New("soft timeout")
	ErrHardTimeout      = errors.New("hard timeout")
	ErrPreventExecution = errors.New("task execution prevented")
	ErrInvalidCron      = errors.New("invalid cron schedule")
	ErrRedisConnection  = errors.New("redis connection error")
)

type BackstageError struct {
	Err     error
	Message string
	TaskID  string
}

func (e *BackstageError) Error() string {
	if e.TaskID != "" {
		return e.Message + " (task: " + e.TaskID + ")"
	}
	return e.Message
}

func (e *BackstageError) Unwrap() error {
	return e.Err
}

func NewError(err error, msg string) *BackstageError {
	return &BackstageError{Err: err, Message: msg}
}
