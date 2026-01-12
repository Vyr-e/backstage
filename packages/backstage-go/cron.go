package backstage

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type CronTask struct {
	Schedule string
	TaskName string
	Queue    *Queue
	Args     []interface{}
	fields   cronFields
	lastRun  time.Time
}

type cronFields struct {
	minute     []int
	hour       []int
	dayOfMonth []int
	month      []int
	dayOfWeek  []int
}

func NewCronTask(schedule, taskName string, queue *Queue, args ...interface{}) (*CronTask, error) {
	ct := &CronTask{
		Schedule: schedule,
		TaskName: taskName,
		Queue:    queue,
		Args:     args,
	}

	fields, err := parseCron(schedule)
	if err != nil {
		return nil, err
	}
	ct.fields = fields

	return ct, nil
}

func parseCron(schedule string) (cronFields, error) {
	parts := strings.Fields(schedule)
	if len(parts) != 5 {
		return cronFields{}, fmt.Errorf("invalid cron: expected 5 fields, got %d", len(parts))
	}

	minute, err := parseField(parts[0], 0, 59)
	if err != nil {
		return cronFields{}, err
	}

	hour, err := parseField(parts[1], 0, 23)
	if err != nil {
		return cronFields{}, err
	}

	dayOfMonth, err := parseField(parts[2], 1, 31)
	if err != nil {
		return cronFields{}, err
	}

	month, err := parseField(parts[3], 1, 12)
	if err != nil {
		return cronFields{}, err
	}

	dayOfWeek, err := parseField(parts[4], 0, 6)
	if err != nil {
		return cronFields{}, err
	}

	return cronFields{
		minute:     minute,
		hour:       hour,
		dayOfMonth: dayOfMonth,
		month:      month,
		dayOfWeek:  dayOfWeek,
	}, nil
}

func parseField(field string, min, max int) ([]int, error) {
	values := make(map[int]bool)

	for _, part := range strings.Split(field, ",") {
		if part == "*" {
			for i := min; i <= max; i++ {
				values[i] = true
			}
		} else if strings.Contains(part, "/") {
			split := strings.Split(part, "/")
			step, err := strconv.Atoi(split[1])
			if err != nil || step <= 0 {
				return nil, fmt.Errorf("invalid step: %s", part)
			}

			start, end := min, max
			if split[0] != "*" {
				if strings.Contains(split[0], "-") {
					rangeParts := strings.Split(split[0], "-")
					start, _ = strconv.Atoi(rangeParts[0])
					end, _ = strconv.Atoi(rangeParts[1])
				} else {
					start, _ = strconv.Atoi(split[0])
				}
			}

			for i := start; i <= end; i += step {
				values[i] = true
			}
		} else if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			start, _ := strconv.Atoi(rangeParts[0])
			end, _ := strconv.Atoi(rangeParts[1])
			for i := start; i <= end; i++ {
				values[i] = true
			}
		} else {
			val, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid value: %s", part)
			}
			values[val] = true
		}
	}

	result := make([]int, 0, len(values))
	for v := range values {
		if v < min || v > max {
			return nil, fmt.Errorf("value %d out of range [%d-%d]", v, min, max)
		}
		result = append(result, v)
	}
	return result, nil
}

func (ct *CronTask) NextRun(after time.Time) time.Time {
	next := after.Truncate(time.Minute).Add(time.Minute)

	for i := 0; i < 525600; i++ { // max 1 year
		if ct.matches(next) {
			return next
		}
		next = next.Add(time.Minute)
	}

	return time.Time{}
}

func (ct *CronTask) matches(t time.Time) bool {
	return contains(ct.fields.minute, t.Minute()) &&
		contains(ct.fields.hour, t.Hour()) &&
		contains(ct.fields.dayOfMonth, t.Day()) &&
		contains(ct.fields.month, int(t.Month())) &&
		contains(ct.fields.dayOfWeek, int(t.Weekday()))
}

func contains(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func (ct *CronTask) MarkRun(at time.Time) {
	ct.lastRun = at
}
