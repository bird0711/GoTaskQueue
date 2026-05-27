package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/metrics"
	"github.com/bird0711/GoTaskQueue/internal/queue"
	"github.com/bird0711/GoTaskQueue/internal/task"
)

const defaultWorkerConcurrency = 4

type Consumer interface {
	EnsureGroup(context.Context) error
	Read(context.Context) (queue.StreamMessage, error)
	ClaimPending(context.Context, time.Duration, int64) ([]queue.StreamMessage, error)
	Ack(context.Context, string) error
}

type DeadPublisher interface {
	PublishDead(context.Context, queue.DeadTaskMessage) error
}

type TaskStore interface {
	Get(context.Context, string) (*task.Task, error)
	Transition(context.Context, string, task.Status, task.Status, *string) (*task.Task, error)
	MarkFailed(context.Context, string, string) (*task.Task, error)
	CompleteFailure(context.Context, string, task.FailureDecision, string) (*task.Task, error)
}

type Worker struct {
	consumer            Consumer
	store               TaskStore
	handlers            *HandlerRegistry
	logger              *slog.Logger
	name                string
	metrics             *metrics.Registry
	deadPublisher       DeadPublisher
	concurrency         int
	sem                 chan struct{}
	wg                  sync.WaitGroup
	pendingMinIdle      time.Duration
	pendingClaimBatch   int64
	pendingRecoveryTick time.Duration
	lastPendingRecovery time.Time
}

func New(consumer Consumer, store TaskStore, logger *slog.Logger, name string) *Worker {
	return &Worker{
		consumer:    consumer,
		store:       store,
		handlers:    NewHandlerRegistry(),
		logger:      logger,
		name:        name,
		concurrency: defaultWorkerConcurrency,
		sem:         make(chan struct{}, defaultWorkerConcurrency),

		pendingMinIdle:      30 * time.Second,
		pendingClaimBatch:   10,
		pendingRecoveryTick: 5 * time.Second,
	}
}

func (w *Worker) WithConcurrency(n int) *Worker {
	if n > 0 {
		w.concurrency = n
		w.sem = make(chan struct{}, n)
	}
	return w
}

func (w *Worker) WithHandlerRegistry(registry *HandlerRegistry) *Worker {
	if registry != nil {
		w.handlers = registry
	}
	return w
}

func (w *Worker) WithPendingRecovery(minIdle time.Duration, batch int64, tick time.Duration) *Worker {
	w.pendingMinIdle = minIdle
	w.pendingClaimBatch = batch
	w.pendingRecoveryTick = tick
	return w
}

func (w *Worker) WithMetrics(registry *metrics.Registry) *Worker {
	w.metrics = registry
	return w
}

func (w *Worker) WithDeadPublisher(publisher DeadPublisher) *Worker {
	w.deadPublisher = publisher
	return w
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.consumer.EnsureGroup(ctx); err != nil {
		return err
	}
	w.logger.Info("worker started", "consumer", w.name, "concurrency", w.concurrency)

	defer w.wg.Wait()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker stopping, waiting for in-flight tasks", "consumer", w.name)
			return nil
		default:
		}

		if w.shouldRecoverPending(time.Now()) {
			if err := w.recoverPending(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					w.logger.Info("worker stopping, waiting for in-flight tasks", "consumer", w.name)
					return nil
				}
				w.logger.Error("recover pending worker messages", "error", err)
				wait(ctx, 500*time.Millisecond)
				continue
			}
		}

		message, err := w.consumer.Read(ctx)
		if errors.Is(err, queue.ErrNoMessages) {
			continue
		}
		if errors.Is(err, context.Canceled) {
			w.logger.Info("worker stopping, waiting for in-flight tasks", "consumer", w.name)
			return nil
		}
		if err != nil {
			w.logger.Error("read worker message", "error", err)
			wait(ctx, 500*time.Millisecond)
			continue
		}

		if !w.acquireSlot(ctx) {
			w.logger.Info("worker stopping, waiting for in-flight tasks", "consumer", w.name)
			return nil
		}
		w.spawnHandle(ctx, message)
	}
}

func (w *Worker) acquireSlot(ctx context.Context) bool {
	select {
	case w.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *Worker) spawnHandle(ctx context.Context, message queue.StreamMessage) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer func() { <-w.sem }()
		if err := w.handleMessage(ctx, message); err != nil {
			w.logger.Error("handle worker message", "redis_id", message.RedisID, "task_id", message.Task.ID, "error", err)
		}
	}()
}

func (w *Worker) shouldRecoverPending(now time.Time) bool {
	if w.pendingMinIdle <= 0 || w.pendingClaimBatch <= 0 || w.pendingRecoveryTick <= 0 {
		return false
	}
	if w.lastPendingRecovery.IsZero() || now.Sub(w.lastPendingRecovery) >= w.pendingRecoveryTick {
		w.lastPendingRecovery = now
		return true
	}
	return false
}

func (w *Worker) recoverPending(ctx context.Context) error {
	messages, err := w.consumer.ClaimPending(ctx, w.pendingMinIdle, w.pendingClaimBatch)
	if err != nil {
		return err
	}
	for _, message := range messages {
		w.logger.Info("pending task message claimed", "redis_id", message.RedisID, "task_id", message.Task.ID)
		if !w.acquireSlot(ctx) {
			return ctx.Err()
		}
		w.spawnHandle(ctx, message)
	}
	return nil
}

func wait(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (w *Worker) handleMessage(ctx context.Context, message queue.StreamMessage) error {
	workerID := w.name
	traceID := message.Task.TraceID
	currentTask, err := w.store.Get(ctx, message.Task.ID)
	if err != nil {
		return err
	}
	if traceID == "" && currentTask.TraceID != nil {
		traceID = *currentTask.TraceID
	}
	if currentTask.Status.IsTerminal() {
		w.logger.Info("terminal task message skipped", "task_id", currentTask.ID, "trace_id", traceID, "status", currentTask.Status, "redis_id", message.RedisID)
		return w.consumer.Ack(ctx, message.RedisID)
	}

	var runningTask *task.Task
	if message.Recovered && currentTask.Status == task.StatusRunning {
		runningTask = currentTask
		if w.metrics != nil {
			w.metrics.TaskStarted(time.Since(runningTask.CreatedAt))
		}
	} else {
		runningTask, err = w.store.Transition(ctx, message.Task.ID, task.StatusPending, task.StatusRunning, &workerID)
		if err == nil && w.metrics != nil {
			w.metrics.TaskStarted(time.Since(runningTask.CreatedAt))
		}
	}
	if err != nil {
		return err
	}
	w.logger.Info("task running", "task_id", message.Task.ID, "trace_id", traceID, "task_type", message.Task.Type)

	executionStartedAt := time.Now()
	if err := w.executeWithTimeout(ctx, runningTask); err != nil {
		if failureErr := w.handleExecutionFailure(ctx, runningTask, traceID, err); failureErr != nil {
			return failureErr
		}
		if w.metrics != nil {
			w.metrics.TaskFailed(time.Since(executionStartedAt))
		}
		return w.consumer.Ack(ctx, message.RedisID)
	}

	if _, err := w.store.Transition(ctx, message.Task.ID, task.StatusRunning, task.StatusSuccess, &workerID); err != nil {
		return err
	}
	if w.metrics != nil {
		w.metrics.TaskSucceeded(time.Since(executionStartedAt))
	}
	w.logger.Info("task succeeded", "task_id", message.Task.ID, "trace_id", traceID, "task_type", message.Task.Type)

	if err := w.consumer.Ack(ctx, message.RedisID); err != nil {
		return err
	}
	w.logger.Info("task message acked", "task_id", message.Task.ID, "trace_id", traceID, "redis_id", message.RedisID)

	return nil
}

func (w *Worker) RegisterHandler(taskType string, handler Handler) error {
	return w.handlers.Register(taskType, handler)
}

func (w *Worker) executeWithTimeout(ctx context.Context, runningTask *task.Task) error {
	timeout := time.Duration(runningTask.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 300 * time.Second
	}

	executionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := w.handlers.Handle(executionCtx, runningTask)
	if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("task execution timed out after %s", timeout)
	}
	return err
}

func (w *Worker) handleExecutionFailure(ctx context.Context, runningTask *task.Task, traceID string, executionErr error) error {
	failure := executionErr.Error()
	failedTask, err := w.store.MarkFailed(ctx, runningTask.ID, failure)
	if err != nil {
		return err
	}
	w.logger.Info("task failed", "task_id", failedTask.ID, "trace_id", traceID, "retry_count", failedTask.RetryCount, "max_retries", failedTask.MaxRetries, "error", failure)

	decision := task.DecideFailure(time.Now().UTC(), failedTask.RetryCount, failedTask.MaxRetries)
	finalTask, err := w.store.CompleteFailure(ctx, failedTask.ID, decision, failure)
	if err != nil {
		return err
	}
	if w.metrics != nil {
		switch finalTask.Status {
		case task.StatusRetrying:
			w.metrics.TaskRetried()
		case task.StatusDead:
			w.metrics.TaskDead()
		}
	}
	w.logger.Info("task failure finalized", "task_id", finalTask.ID, "trace_id", traceID, "status", finalTask.Status, "retry_count", finalTask.RetryCount)

	if finalTask.Status == task.StatusDead && w.deadPublisher != nil {
		if err := w.deadPublisher.PublishDead(ctx, queue.DeadTaskMessage{
			ID:         runningTask.ID,
			Type:       runningTask.Type,
			TraceID:    traceID,
			LastError:  failure,
			RetryCount: finalTask.RetryCount,
		}); err != nil {
			w.logger.Warn("publish dead task to dead stream", "task_id", runningTask.ID, "trace_id", traceID, "error", err)
		}
	}

	return nil
}
