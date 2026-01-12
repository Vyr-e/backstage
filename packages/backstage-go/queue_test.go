package backstage

import (
	"testing"
)

func TestQueueCreation(t *testing.T) {
	q := NewQueue("test")
	if q.Name != "test" {
		t.Errorf("expected name 'test', got '%s'", q.Name)
	}
	if q.Prefix != StreamPrefix {
		t.Errorf("expected prefix '%s', got '%s'", StreamPrefix, q.Prefix)
	}
}

func TestQueueWithOptions(t *testing.T) {
	q := NewQueue("urgent", QueueOptions{
		Priority:    1,
		SoftTimeout: 5000,
		HardTimeout: 30000,
		MaxRetries:  3,
	})

	if q.Priority != 1 {
		t.Errorf("expected priority 1, got %d", q.Priority)
	}
	if q.SoftTimeout != 5000 {
		t.Errorf("expected soft timeout 5000, got %d", q.SoftTimeout)
	}
	if q.HardTimeout != 30000 {
		t.Errorf("expected hard timeout 30000, got %d", q.HardTimeout)
	}
	if q.MaxRetries != 3 {
		t.Errorf("expected max retries 3, got %d", q.MaxRetries)
	}
}

func TestQueueStreamKey(t *testing.T) {
	q := NewQueue("urgent")
	expected := "backstage:urgent"
	if q.StreamKey() != expected {
		t.Errorf("expected '%s', got '%s'", expected, q.StreamKey())
	}
}

func TestQueueScheduledKey(t *testing.T) {
	q := NewQueue("urgent")
	expected := "backstage:scheduled:urgent"
	if q.ScheduledKey() != expected {
		t.Errorf("expected '%s', got '%s'", expected, q.ScheduledKey())
	}
}

func TestQueueDeadLetterKey(t *testing.T) {
	q := NewQueue("urgent")
	expected := "backstage:urgent:dead-letter"
	if q.DeadLetterKey() != expected {
		t.Errorf("expected '%s', got '%s'", expected, q.DeadLetterKey())
	}
}

func TestDefaultQueues(t *testing.T) {
	if QueueUrgent.Name != "urgent" {
		t.Errorf("urgent queue name: %s", QueueUrgent.Name)
	}
	if QueueUrgent.Priority != 1 {
		t.Errorf("urgent priority: %d", QueueUrgent.Priority)
	}

	if QueueDefault.Name != "default" {
		t.Errorf("default queue name: %s", QueueDefault.Name)
	}
	if QueueDefault.Priority != 2 {
		t.Errorf("default priority: %d", QueueDefault.Priority)
	}

	if QueueLow.Name != "low" {
		t.Errorf("low queue name: %s", QueueLow.Name)
	}
	if QueueLow.Priority != 3 {
		t.Errorf("low priority: %d", QueueLow.Priority)
	}
}

func TestQueuePriorityOrder(t *testing.T) {
	if QueueUrgent.Priority >= QueueDefault.Priority {
		t.Error("urgent should have lower priority number than default")
	}
	if QueueDefault.Priority >= QueueLow.Priority {
		t.Error("default should have lower priority number than low")
	}
}
