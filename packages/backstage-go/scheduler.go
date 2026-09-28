// Package backstage scheduler implementation.
// Manages cron-like schedules and moves delayed/scheduled tasks to active queues.
package backstage

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// Scheduler manages cron-like recurrent tasks.
// It checks properly configured schedules and enqueues tasks when they are due.
// It also handles moving delayed/scheduled tasks from the ZSET to the active stream
// when they become ready for processing.
type Scheduler struct {
	redis         *redis.Client
	schedules     []*CronTask
	queues        map[string]*Queue
	logger        *Logger
	running       bool
	prefix        string
	provider      Provider
	resolved      ResolvedCapabilities
	redisProvider *RedisStreamsProvider
}

// SchedulerConfig configuration for the Scheduler.
type SchedulerConfig struct {
	Host            string
	Port            int
	Password        string
	DB              int
	// Schedules list of cron tasks to run.
	Schedules       []*CronTask
	// Queues definitions for custom queues (used for resolving stream keys).
	Queues          []*Queue
	LogLevel        slog.Level
	Silent          bool
	Prefix          string // Stream key prefix (default: "backstage")
	DefaultPriority string // Default priority name (default: "default")
	Provider        Provider
	Capabilities    *Capabilities
}

// Lua script for atomic scheduled task processing
// Prevents race conditions when multiple schedulers run
const processScheduledLua = `
local zsetKey = KEYS[1]
local cutoff = tonumber(ARGV[1])
local prefix = ARGV[2]
local defaultPriority = ARGV[3]

local tasks = redis.call('ZRANGEBYSCORE', zsetKey, '-inf', cutoff)
local processed = 0

for _, taskData in ipairs(tasks) do
    local ok, task = pcall(cjson.decode, taskData)
    if ok and task then
        local priority = task.priority or defaultPriority
        local streamKey = prefix .. ':' .. priority
        
        redis.call('XADD', streamKey, '*',
            'taskName', task.taskName or '',
            'payload', task.payload or '{}',
            'enqueuedAt', tostring(task.enqueuedAt or 0)
        )
        
        redis.call('ZREM', zsetKey, taskData)
        processed = processed + 1
    end
end

return processed
`

func NewScheduler(cfg SchedulerConfig) *Scheduler {
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = StreamPrefix
	}
	queues := make(map[string]*Queue)
	for _, q := range cfg.Queues {
		q.Prefix = prefix
		queues[q.Name] = q
	}

	s := &Scheduler{
		schedules: cfg.Schedules,
		queues:    queues,
		logger:    NewLogger("Scheduler", LoggerConfig{Level: cfg.LogLevel, Silent: cfg.Silent}),
		prefix:    prefix,
	}

	if cfg.Provider != nil {
		s.provider = cfg.Provider
		if rp, ok := cfg.Provider.(*RedisStreamsProvider); ok {
			s.redisProvider = rp
			s.redis = rp.Redis()
		}
	} else {
		rp := NewRedisStreamsProvider(RedisStreamsProviderConfig{
			Host: cfg.Host, Port: cfg.Port, Password: cfg.Password, DB: cfg.DB, Prefix: prefix,
		})
		s.provider = rp
		s.redisProvider = rp
		s.redis = rp.Redis()
	}
	resolved, _ := ResolveCapabilities(s.provider, cfg.Capabilities)
	s.resolved = resolved
	_ = s.provider.Init(context.Background(), ProviderContext{Capabilities: resolved, Logger: s.logger})
	return s
}

// Start runs the scheduler loop.
// It manages two main activities:
// 1. Enqueueing recurrrent cron tasks when their schedule matches.
// 2. Waiting for the next scheduled run.
// Note: Moving scheduled tasks (ZSET -> Stream) is typically handled by
// calling ProcessScheduledTasks periodically, or by a separate routine.
func (s *Scheduler) Start(ctx context.Context) error {
	if len(s.schedules) == 0 {
		s.logger.Error("No schedules configured")
		return nil
	}

	s.logger.Info("Starting scheduler", "tasks", len(s.schedules))
	s.running = true

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		s.logger.Info("Shutting down...")
		s.running = false
	}()

	var upcoming []*CronTask

	for s.running {
		now := time.Now()

		for _, task := range upcoming {
			s.enqueueTask(ctx, task)
			task.MarkRun(now)
		}

		minDelay := time.Hour * 24
		upcoming = nil

		for _, task := range s.schedules {
			next := task.NextRun(now)
			delay := next.Sub(now)

			if delay < minDelay {
				minDelay = delay
				upcoming = []*CronTask{task}
			} else if delay == minDelay {
				upcoming = append(upcoming, task)
			}
		}

		s.logger.Debug("Sleeping until next task", "delay", minDelay)
		select {
		case <-time.After(minDelay):
		case <-ctx.Done():
			return nil
		}
	}

	s.logger.Info("Scheduler stopped")
	return nil
}

func (s *Scheduler) Stop() {
	s.running = false
}

func (s *Scheduler) enqueueTask(ctx context.Context, task *CronTask) {
	queueName := "default"
	if task.Queue != nil {
		queueName = task.Queue.Name
	}
	_, err := s.resolved.Jobs.Publish(ctx, OutgoingJob{
		Queue: queueName, TaskName: task.TaskName, Payload: map[string]interface{}{},
		EnqueuedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		s.logger.Error("Failed to enqueue scheduled task", "task", task.TaskName, "error", err)
		return
	}
	s.logger.Info("Enqueued scheduled task", "task", task.TaskName)
}

// ProcessScheduledTasks atomically moves due tasks from ZSET to streams.
// It effectively "wakes up" tasks that were scheduled with a delay.
// Uses a Lua script to identify tasks with score <= now, moves them to their
// target stream, and removes them from the ZSET in one atomic operation.
func (s *Scheduler) ProcessScheduledTasks(ctx context.Context, defaultPriority string) (int64, error) {
	if s.redisProvider != nil {
		return s.redisProvider.PromoteCrossProvider(ctx)
	}
	scheduledKey := ScheduledKey(s.prefix)
	now := time.Now().UnixMilli()
	if defaultPriority == "" {
		defaultPriority = "default"
	}
	result, err := s.redis.Eval(ctx, ProcessScheduledLua, []string{scheduledKey},
		now, s.prefix, defaultPriority,
	).Result()
	if err != nil {
		return 0, err
	}
	count, _ := result.(int64)
	return count, nil
}
