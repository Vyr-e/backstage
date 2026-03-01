// Package backstage queue definitions.
// Defines Queue structures and standard priority management.
package backstage

import "fmt"

type QueueOptions struct {
	Priority    int
	SoftTimeout int64 // milliseconds
	HardTimeout int64
	MaxRetries  int
}

type Queue struct {
	Name        string
	Prefix      string
	Priority    int
	SoftTimeout int64
	HardTimeout int64
	MaxRetries  int
}

type QueueOption func(*Queue)

func NewQueue(name string, opts ...QueueOption) *Queue {
	q := &Queue{
		Name:   name,
		Prefix: StreamPrefix,
	}

	for _, opt := range opts {
		opt(q)
	}

	return q
}

func WithPriority(p int) QueueOption {
	return func(q *Queue) { q.Priority = p }
}

func WithPrefix(prefix string) QueueOption {
	return func(q *Queue) { q.Prefix = prefix }
}

func WithTimeouts(soft, hard int64) QueueOption {
	return func(q *Queue) {
		q.SoftTimeout = soft
		q.HardTimeout = hard
	}
}

func WithMaxRetries(retries int) QueueOption {
	return func(q *Queue) { q.MaxRetries = retries }
}

func (q *Queue) StreamKey() string {
	return fmt.Sprintf("%s:%s", q.Prefix, q.Name)
}

func (q *Queue) ScheduledKey() string {
	return fmt.Sprintf("%s:scheduled:%s", q.Prefix, q.Name)
}

func (q *Queue) DeadLetterKey() string {
	return fmt.Sprintf("%s:%s:dead-letter", q.Prefix, q.Name)
}

// Default queues
var (
	QueueUrgent  = NewQueue("urgent", WithPriority(1))
	QueueDefault = NewQueue("default", WithPriority(2))
	QueueLow     = NewQueue("low", WithPriority(3))
)
