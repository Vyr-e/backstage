package backstage

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Scheduler manages cron-like recurrent tasks via a Provider.
type Scheduler struct {
	provider  Provider
	schedules []*CronTask
	queues    map[string]*Queue
	logger    *Logger
	running   bool
	prefix    string
}

// SchedulerConfig configuration for the Scheduler.
type SchedulerConfig struct {
	Host            string
	Port            int
	Password        string
	DB              int
	Schedules       []*CronTask
	Queues          []*Queue
	LogLevel        slog.Level
	Silent          bool
	Prefix          string
	DefaultPriority string
	Provider        Provider
}

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

	var provider Provider
	if cfg.Provider != nil {
		provider = cfg.Provider
	} else {
		provider = NewRedisStreamsProvider(RedisStreamsProviderConfig{
			Host:     cfg.Host,
			Port:     cfg.Port,
			Password: cfg.Password,
			DB:       cfg.DB,
			Prefix:   prefix,
		})
	}

	return &Scheduler{
		provider:  provider,
		schedules: cfg.Schedules,
		queues:    queues,
		logger:    NewLogger("Scheduler", LoggerConfig{Level: cfg.LogLevel, Silent: cfg.Silent}),
		prefix:    prefix,
	}
}

// Start runs the scheduler loop, enqueueing cron tasks when due.
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
	opts := PublishOptions{}
	if task.Queue != nil {
		opts.Queue = task.Queue.Name
	}
	if _, err := s.provider.Publish(ctx, task.TaskName, map[string]interface{}{}, opts); err != nil {
		s.logger.Error("Failed to enqueue scheduled task", "task", task.TaskName, "error", err)
		return
	}
	s.logger.Info("Enqueued scheduled task", "task", task.TaskName)
}

// ProcessScheduledTasks moves due delayed tasks into their work queues via the Provider.
func (s *Scheduler) ProcessScheduledTasks(ctx context.Context, defaultPriority string) (int64, error) {
	_ = defaultPriority
	return s.provider.PromoteDueScheduled(ctx, 0)
}

// Close closes the underlying provider.
func (s *Scheduler) Close() error {
	if s.provider != nil {
		return s.provider.Close()
	}
	return nil
}
