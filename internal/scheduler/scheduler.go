package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/queue"
	"github.com/bird0711/GoTaskQueue/internal/task"
)

type Store interface {
	DueDispatchable(context.Context, time.Time, int) ([]*task.Task, error)
	Transition(context.Context, string, task.Status, task.Status, *string) (*task.Task, error)
}

type Publisher interface {
	PublishTask(context.Context, queue.TaskMessage) error
}

type Scheduler struct {
	store     Store
	publisher Publisher
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

func New(store Store, publisher Publisher, logger *slog.Logger, interval time.Duration, batchSize int) *Scheduler {
	return &Scheduler{
		store:     store,
		publisher: publisher,
		logger:    logger,
		interval:  interval,
		batchSize: batchSize,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.logger.Info("scheduler started", "interval", s.interval.String(), "batch_size", s.batchSize)

	if err := s.Tick(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler stopped")
			return nil
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil {
				s.logger.Error("scheduler tick failed", "error", err)
			}
		}
	}
}

func (s *Scheduler) Tick(ctx context.Context) error {
	dueTasks, err := s.store.DueDispatchable(ctx, time.Now().UTC(), s.batchSize)
	if err != nil {
		return fmt.Errorf("scan due dispatchable tasks: %w", err)
	}

	for _, dueTask := range dueTasks {
		if err := s.scheduleTask(ctx, dueTask); err != nil {
			s.logger.Error("schedule due task", "task_id", dueTask.ID, "error", err)
		}
	}

	return nil
}

func (s *Scheduler) scheduleTask(ctx context.Context, dueTask *task.Task) error {
	if dueTask.Status != task.StatusScheduled && dueTask.Status != task.StatusRetrying {
		return fmt.Errorf("unsupported dispatchable task status %s", dueTask.Status)
	}

	pendingTask, err := s.store.Transition(ctx, dueTask.ID, dueTask.Status, task.StatusPending, nil)
	if errors.Is(err, task.ErrNotFound) {
		s.logger.Info("dispatchable task already claimed or missing", "task_id", dueTask.ID, "status", dueTask.Status)
		return nil
	}
	if err != nil {
		return fmt.Errorf("transition dispatchable task to pending: %w", err)
	}

	if err := s.publisher.PublishTask(ctx, queue.TaskMessage{
		ID:   pendingTask.ID,
		Type: pendingTask.Type,
	}); err != nil {
		return fmt.Errorf("publish dispatchable task: %w", err)
	}

	s.logger.Info("dispatchable task published", "task_id", pendingTask.ID, "task_type", pendingTask.Type, "from_status", dueTask.Status)
	return nil
}
