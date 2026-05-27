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
	Get(context.Context, string) (*task.Task, error)
	ExpiredRunning(context.Context, time.Time, int) ([]*task.Task, error)
	DueRetrying(context.Context, time.Time, int) ([]*task.Task, error)
	Transition(context.Context, string, task.Status, task.Status, *string) (*task.Task, error)
	MarkFailed(context.Context, string, string) (*task.Task, error)
	CompleteFailure(context.Context, string, task.FailureDecision, string) (*task.Task, error)
}

type Publisher interface {
	PublishTask(context.Context, queue.TaskMessage) error
}

type DeadPublisher interface {
	PublishDead(context.Context, queue.DeadTaskMessage) error
}

type ScheduledQueue interface {
	DueTaskIDs(context.Context, time.Time, int) ([]string, error)
	Remove(context.Context, string) error
}

type Scheduler struct {
	store          Store
	publisher      Publisher
	scheduledQueue ScheduledQueue
	deadPublisher  DeadPublisher
	logger         *slog.Logger
	interval       time.Duration
	batchSize      int
}

func New(store Store, publisher Publisher, scheduledQueue ScheduledQueue, logger *slog.Logger, interval time.Duration, batchSize int) *Scheduler {
	return &Scheduler{
		store:          store,
		publisher:      publisher,
		scheduledQueue: scheduledQueue,
		logger:         logger,
		interval:       interval,
		batchSize:      batchSize,
	}
}

func (s *Scheduler) WithDeadPublisher(publisher DeadPublisher) *Scheduler {
	s.deadPublisher = publisher
	return s
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
	now := time.Now().UTC()
	if err := s.recoverExpiredRunning(ctx, now); err != nil {
		return err
	}

	if err := s.scheduleDueZSetTasks(ctx, now); err != nil {
		return err
	}

	dueRetryingTasks, err := s.store.DueRetrying(ctx, now, s.batchSize)
	if err != nil {
		return fmt.Errorf("scan due retrying tasks: %w", err)
	}

	for _, dueTask := range dueRetryingTasks {
		if err := s.scheduleTask(ctx, dueTask); err != nil {
			s.logger.Error("schedule due task", "task_id", dueTask.ID, "error", err)
		}
	}

	return nil
}

func (s *Scheduler) recoverExpiredRunning(ctx context.Context, now time.Time) error {
	expiredTasks, err := s.store.ExpiredRunning(ctx, now, s.batchSize)
	if err != nil {
		return fmt.Errorf("scan expired running tasks: %w", err)
	}

	for _, expiredTask := range expiredTasks {
		if err := s.recoverRunningTask(ctx, now, expiredTask); err != nil {
			s.logger.Error("recover expired running task", "task_id", expiredTask.ID, "error", err)
		}
	}

	return nil
}

func (s *Scheduler) recoverRunningTask(ctx context.Context, now time.Time, expiredTask *task.Task) error {
	failure := fmt.Sprintf("running timeout recovery: task exceeded timeout_seconds=%d", expiredTask.TimeoutSeconds)
	failedTask, err := s.store.MarkFailed(ctx, expiredTask.ID, failure)
	if errors.Is(err, task.ErrNotFound) {
		s.logger.Info("expired running task already completed or missing", "task_id", expiredTask.ID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("mark expired running task failed: %w", err)
	}

	decision := task.DecideFailure(now, failedTask.RetryCount, failedTask.MaxRetries)
	finalTask, err := s.store.CompleteFailure(ctx, failedTask.ID, decision, failure)
	if errors.Is(err, task.ErrNotFound) {
		s.logger.Info("expired running task failure already finalized or missing", "task_id", failedTask.ID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("complete expired running task failure: %w", err)
	}

	s.logger.Info("expired running task recovered", "task_id", finalTask.ID, "status", finalTask.Status, "retry_count", finalTask.RetryCount)

	if finalTask.Status == task.StatusDead && s.deadPublisher != nil {
		traceID := ""
		if expiredTask.TraceID != nil {
			traceID = *expiredTask.TraceID
		}
		if err := s.deadPublisher.PublishDead(ctx, queue.DeadTaskMessage{
			ID:         finalTask.ID,
			Type:       expiredTask.Type,
			TraceID:    traceID,
			LastError:  failure,
			RetryCount: finalTask.RetryCount,
		}); err != nil {
			s.logger.Warn("publish dead task to dead stream", "task_id", finalTask.ID, "trace_id", traceID, "error", err)
		}
	}

	return nil
}

func (s *Scheduler) scheduleDueZSetTasks(ctx context.Context, now time.Time) error {
	dueTaskIDs, err := s.scheduledQueue.DueTaskIDs(ctx, now, s.batchSize)
	if err != nil {
		return fmt.Errorf("scan due scheduled zset tasks: %w", err)
	}

	for _, taskID := range dueTaskIDs {
		if err := s.scheduleZSetTask(ctx, taskID); err != nil {
			s.logger.Error("schedule zset task", "task_id", taskID, "error", err)
		}
	}

	return nil
}

func (s *Scheduler) scheduleZSetTask(ctx context.Context, taskID string) error {
	dueTask, err := s.store.Get(ctx, taskID)
	if errors.Is(err, task.ErrNotFound) {
		if removeErr := s.scheduledQueue.Remove(ctx, taskID); removeErr != nil {
			return removeErr
		}
		s.logger.Info("scheduled zset task missing and removed", "task_id", taskID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("get scheduled zset task: %w", err)
	}
	if dueTask.Status != task.StatusScheduled {
		if removeErr := s.scheduledQueue.Remove(ctx, taskID); removeErr != nil {
			return removeErr
		}
		s.logger.Info("scheduled zset task no longer scheduled and removed", "task_id", taskID, "status", dueTask.Status)
		return nil
	}

	if err := s.scheduleTask(ctx, dueTask); err != nil {
		return err
	}
	if err := s.scheduledQueue.Remove(ctx, taskID); err != nil {
		return err
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

	traceID := ""
	if pendingTask.TraceID != nil {
		traceID = *pendingTask.TraceID
	}
	if err := s.publisher.PublishTask(ctx, queue.TaskMessage{
		ID:      pendingTask.ID,
		Type:    pendingTask.Type,
		TraceID: traceID,
	}); err != nil {
		return fmt.Errorf("publish dispatchable task: %w", err)
	}

	s.logger.Info("dispatchable task published", "task_id", pendingTask.ID, "task_type", pendingTask.Type, "from_status", dueTask.Status)
	return nil
}
