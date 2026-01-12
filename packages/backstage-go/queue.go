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

func NewQueue(name string, opts ...QueueOptions) *Queue {
	q := &Queue{
		Name:     name,
		Prefix:   StreamPrefix,
		Priority: 0,
	}

	if len(opts) > 0 {
		q.Priority = opts[0].Priority
		q.SoftTimeout = opts[0].SoftTimeout
		q.HardTimeout = opts[0].HardTimeout
		q.MaxRetries = opts[0].MaxRetries
	}

	return q
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
	QueueUrgent  = NewQueue("urgent", QueueOptions{Priority: 1})
	QueueDefault = NewQueue("default", QueueOptions{Priority: 2})
	QueueLow     = NewQueue("low", QueueOptions{Priority: 3})
)
