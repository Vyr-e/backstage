package backstage

import (
	"testing"
	"time"
)

func TestCronEveryMinute(t *testing.T) {
	task, err := NewCronTask("* * * * *", "every.minute", nil)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)
	next := task.NextRun(now)

	if next.Minute() != 1 {
		t.Errorf("expected minute 1, got %d", next.Minute())
	}
}

func TestCronEveryFiveMinutes(t *testing.T) {
	task, _ := NewCronTask("*/5 * * * *", "every.5.min", nil)
	now := time.Date(2024, 1, 10, 12, 3, 0, 0, time.UTC)
	next := task.NextRun(now)

	if next.Minute()%5 != 0 {
		t.Errorf("expected multiple of 5, got %d", next.Minute())
	}
}

func TestCronEveryHour(t *testing.T) {
	task, _ := NewCronTask("0 * * * *", "every.hour", nil)
	now := time.Date(2024, 1, 10, 12, 30, 0, 0, time.UTC)
	next := task.NextRun(now)

	if next.Minute() != 0 {
		t.Errorf("expected minute 0, got %d", next.Minute())
	}
	if next.Hour() != 13 {
		t.Errorf("expected hour 13, got %d", next.Hour())
	}
}

func TestCronDailyMidnight(t *testing.T) {
	task, _ := NewCronTask("0 0 * * *", "daily.midnight", nil)
	now := time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)
	next := task.NextRun(now)

	if next.Hour() != 0 || next.Minute() != 0 {
		t.Errorf("expected 00:00, got %02d:%02d", next.Hour(), next.Minute())
	}
}

func TestCronBusinessHours(t *testing.T) {
	task, _ := NewCronTask("0 9-17 * * 1-5", "business", nil)
	now := time.Date(2024, 1, 10, 8, 0, 0, 0, time.UTC) // Wednesday 8am
	next := task.NextRun(now)

	if next.Hour() < 9 || next.Hour() > 17 {
		t.Errorf("expected 9-17, got %d", next.Hour())
	}
	if next.Weekday() < 1 || next.Weekday() > 5 {
		t.Errorf("expected weekday, got %v", next.Weekday())
	}
}

func TestCronWeekends(t *testing.T) {
	task, _ := NewCronTask("0 10 * * 0,6", "weekends", nil)
	now := time.Date(2024, 1, 10, 8, 0, 0, 0, time.UTC) // Wednesday
	next := task.NextRun(now)

	day := int(next.Weekday())
	if day != 0 && day != 6 {
		t.Errorf("expected weekend, got %v", next.Weekday())
	}
}

func TestCronSpecificDayOfMonth(t *testing.T) {
	task, _ := NewCronTask("0 0 15 * *", "monthly.15th", nil)
	now := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	next := task.NextRun(now)

	if next.Day() != 15 {
		t.Errorf("expected day 15, got %d", next.Day())
	}
}

func TestCronStepInRange(t *testing.T) {
	task, _ := NewCronTask("0-30/10 * * * *", "step.range", nil)
	now := time.Date(2024, 1, 10, 12, 5, 0, 0, time.UTC)
	next := task.NextRun(now)

	valid := []int{0, 10, 20, 30}
	found := false
	for _, v := range valid {
		if next.Minute() == v {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected one of %v, got %d", valid, next.Minute())
	}
}

func TestCronYearBoundary(t *testing.T) {
	task, _ := NewCronTask("0 0 1 1 *", "new.year", nil)
	now := time.Date(2024, 12, 31, 23, 0, 0, 0, time.UTC)
	next := task.NextRun(now)

	if next.Year() != 2025 {
		t.Errorf("expected 2025, got %d", next.Year())
	}
	if next.Month() != 1 || next.Day() != 1 {
		t.Errorf("expected Jan 1, got %v", next)
	}
}

func TestCronInvalidSchedules(t *testing.T) {
	testCases := []struct {
		schedule string
		desc     string
	}{
		{"invalid", "single word"},
		{"* * *", "3 fields"},
		{"* * * * * *", "6 fields"},
		{"60 * * * *", "minute 60"},
		{"* 24 * * *", "hour 24"},
		{"* * 32 * *", "day 32"},
		{"* * * 13 *", "month 13"},
		{"* * * * 7", "weekday 7"},
		{"*/0 * * * *", "step 0"},
		{"abc * * * *", "letters"},
	}

	for _, tc := range testCases {
		_, err := NewCronTask(tc.schedule, "test", nil)
		if err == nil {
			t.Errorf("expected error for %s (%s)", tc.schedule, tc.desc)
		}
	}
}

func TestCronMarkRun(t *testing.T) {
	task, _ := NewCronTask("* * * * *", "test", nil)
	now := time.Now()
	task.MarkRun(now)

	if task.lastRun != now {
		t.Error("lastRun not set")
	}
}
