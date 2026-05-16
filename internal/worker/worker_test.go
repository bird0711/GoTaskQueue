package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/metrics"
	"github.com/bird0711/GoTaskQueue/internal/queue"
	"github.com/bird0711/GoTaskQueue/internal/task"
)

type fakeConsumer struct {
	acked           []string
	pendingMessages []queue.StreamMessage
	claimMinIdle    time.Duration
	claimBatch      int64
}

func (c *fakeConsumer) EnsureGroup(context.Context) error {
	return nil
}

func (c *fakeConsumer) Read(context.Context) (queue.StreamMessage, error) {
	return queue.StreamMessage{}, queue.ErrNoMessages
}

func (c *fakeConsumer) ClaimPending(_ context.Context, minIdle time.Duration, batch int64) ([]queue.StreamMessage, error) {
	c.claimMinIdle = minIdle
	c.claimBatch = batch
	return c.pendingMessages, nil
}

func (c *fakeConsumer) Ack(_ context.Context, redisID string) error {
	c.acked = append(c.acked, redisID)
	return nil
}

type transitionCall struct {
	from task.Status
	to   task.Status
}

type fakeTaskStore struct {
	calls          []transitionCall
	markFailed     bool
	lastFailure    string
	completeStatus task.Status
	runningTask    *task.Task
	currentTask    *task.Task
}

func (s *fakeTaskStore) Get(_ context.Context, id string) (*task.Task, error) {
	if s.currentTask != nil {
		return s.currentTask, nil
	}
	return &task.Task{
		ID:     id,
		Status: task.StatusPending,
	}, nil
}

func (s *fakeTaskStore) Transition(_ context.Context, _ string, from task.Status, to task.Status, _ *string) (*task.Task, error) {
	s.calls = append(s.calls, transitionCall{from: from, to: to})
	if to == task.StatusRunning && s.runningTask != nil {
		s.runningTask.Status = to
		return s.runningTask, nil
	}
	return &task.Task{Status: to}, nil
}

func (s *fakeTaskStore) MarkFailed(_ context.Context, id string, failure string) (*task.Task, error) {
	s.markFailed = true
	s.lastFailure = failure
	retryCount := 0
	maxRetries := 3
	if s.runningTask != nil {
		retryCount = s.runningTask.RetryCount
		maxRetries = s.runningTask.MaxRetries
	}
	return &task.Task{
		ID:         id,
		Status:     task.StatusFailed,
		RetryCount: retryCount,
		MaxRetries: maxRetries,
		LastError:  &failure,
	}, nil
}

func (s *fakeTaskStore) CompleteFailure(_ context.Context, _ string, decision task.FailureDecision, _ string) (*task.Task, error) {
	s.completeStatus = decision.Status
	return &task.Task{
		Status:      decision.Status,
		RetryCount:  decision.RetryCount,
		NextRetryAt: decision.NextRetryAt,
	}, nil
}

func TestHandleMessageTransitionsToSuccessAndAcks(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{}
	worker := New(consumer, store, slog.Default(), "test-worker")

	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task: queue.TaskMessage{
			ID:   "task_123",
			Type: "email.send",
		},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	expectedTransitions := []transitionCall{
		{from: task.StatusPending, to: task.StatusRunning},
		{from: task.StatusRunning, to: task.StatusSuccess},
	}
	if len(store.calls) != len(expectedTransitions) {
		t.Fatalf("expected %d transitions, got %d", len(expectedTransitions), len(store.calls))
	}
	for index, expected := range expectedTransitions {
		if store.calls[index] != expected {
			t.Fatalf("transition %d: expected %+v, got %+v", index, expected, store.calls[index])
		}
	}

	if len(consumer.acked) != 1 || consumer.acked[0] != "1-0" {
		t.Fatalf("expected redis message ack, got %#v", consumer.acked)
	}
}

func TestHandleMessageRecordsSuccessMetrics(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:             "task_metrics",
			Type:           "email.send",
			Status:         task.StatusPending,
			TimeoutSeconds: 30,
			CreatedAt:      time.Now().UTC().Add(-2 * time.Second),
		},
	}
	registry := metrics.NewRegistry()
	worker := New(consumer, store, slog.Default(), "test-worker").WithMetrics(registry)

	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task: queue.TaskMessage{
			ID:   "task_metrics",
			Type: "email.send",
		},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	assertMetricContains(t, registry, "gotaskqueue_tasks_started_total 1")
	assertMetricContains(t, registry, "gotaskqueue_tasks_succeeded_total 1")
	assertMetricContains(t, registry, "gotaskqueue_task_execution_duration_seconds_count 1")
	assertMetricContains(t, registry, "gotaskqueue_task_wait_duration_seconds_count 1")
}

type countingExecutor struct {
	count int
}

func (e *countingExecutor) Execute(context.Context, *task.Task) error {
	e.count++
	return nil
}

func TestHandleMessageTerminalTaskAcksWithoutExecuting(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		currentTask: &task.Task{
			ID:     "task_done",
			Type:   "email.send",
			Status: task.StatusSuccess,
		},
	}
	executor := &countingExecutor{}
	worker := New(consumer, store, slog.Default(), "test-worker").WithExecutor(executor)

	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task: queue.TaskMessage{
			ID:   "task_done",
			Type: "email.send",
		},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	if executor.count != 0 {
		t.Fatalf("expected terminal task not to execute, executed %d times", executor.count)
	}
	if len(store.calls) != 0 {
		t.Fatalf("expected no status transitions, got %#v", store.calls)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "1-0" {
		t.Fatalf("expected redis message ack, got %#v", consumer.acked)
	}
}

func TestRecoverPendingClaimsAndProcessesRunningTask(t *testing.T) {
	consumer := &fakeConsumer{
		pendingMessages: []queue.StreamMessage{
			{
				RedisID:   "1-0",
				Recovered: true,
				Task: queue.TaskMessage{
					ID:   "task_recovered",
					Type: "email.send",
				},
			},
		},
	}
	store := &fakeTaskStore{
		currentTask: &task.Task{
			ID:             "task_recovered",
			Type:           "email.send",
			Status:         task.StatusRunning,
			TimeoutSeconds: 30,
		},
	}
	executor := &countingExecutor{}
	worker := New(consumer, store, slog.Default(), "test-worker").WithExecutor(executor).
		WithPendingRecovery(10*time.Second, 5, time.Second)

	err := worker.recoverPending(context.Background())
	if err != nil {
		t.Fatalf("recover pending: %v", err)
	}

	if consumer.claimMinIdle != 10*time.Second {
		t.Fatalf("expected claim min idle 10s, got %s", consumer.claimMinIdle)
	}
	if consumer.claimBatch != 5 {
		t.Fatalf("expected claim batch 5, got %d", consumer.claimBatch)
	}
	if executor.count != 1 {
		t.Fatalf("expected recovered task to execute once, executed %d times", executor.count)
	}
	expectedTransitions := []transitionCall{
		{from: task.StatusRunning, to: task.StatusSuccess},
	}
	if len(store.calls) != len(expectedTransitions) {
		t.Fatalf("expected %d transitions, got %d", len(expectedTransitions), len(store.calls))
	}
	for index, expected := range expectedTransitions {
		if store.calls[index] != expected {
			t.Fatalf("transition %d: expected %+v, got %+v", index, expected, store.calls[index])
		}
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "1-0" {
		t.Fatalf("expected redis message ack, got %#v", consumer.acked)
	}
}

type failingExecutor struct{}

func (failingExecutor) Execute(context.Context, *task.Task) error {
	return errors.New("execution failed")
}

func TestHandleMessageFailureTransitionsToRetryingAndAcks(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:         "task_123",
			Status:     task.StatusPending,
			RetryCount: 0,
			MaxRetries: 3,
			CreatedAt:  time.Now().UTC(),
		},
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithExecutor(failingExecutor{})

	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task: queue.TaskMessage{
			ID:   "task_123",
			Type: "email.send",
		},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	if !store.markFailed {
		t.Fatal("expected task to be marked failed")
	}
	if store.completeStatus != task.StatusRetrying {
		t.Fatalf("expected retrying final status, got %q", store.completeStatus)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "1-0" {
		t.Fatalf("expected redis message ack, got %#v", consumer.acked)
	}
}

func TestHandleMessageRecordsFailureMetrics(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:             "task_failed",
			Type:           "email.send",
			Status:         task.StatusPending,
			TimeoutSeconds: 30,
			RetryCount:     0,
			MaxRetries:     3,
			CreatedAt:      time.Now().UTC(),
		},
	}
	registry := metrics.NewRegistry()
	worker := New(consumer, store, slog.Default(), "test-worker").WithExecutor(failingExecutor{}).WithMetrics(registry)

	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task: queue.TaskMessage{
			ID:   "task_failed",
			Type: "email.send",
		},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	assertMetricContains(t, registry, "gotaskqueue_tasks_failed_total 1")
	assertMetricContains(t, registry, "gotaskqueue_tasks_retried_total 1")
	assertMetricContains(t, registry, "gotaskqueue_task_execution_duration_seconds_count 1")
}

func TestHandleMessageRecordsDeadMetrics(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:             "task_dead",
			Type:           "email.send",
			Status:         task.StatusPending,
			TimeoutSeconds: 30,
			RetryCount:     3,
			MaxRetries:     3,
			CreatedAt:      time.Now().UTC(),
		},
	}
	registry := metrics.NewRegistry()
	worker := New(consumer, store, slog.Default(), "test-worker").WithExecutor(failingExecutor{}).WithMetrics(registry)

	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task: queue.TaskMessage{
			ID:   "task_dead",
			Type: "email.send",
		},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	assertMetricContains(t, registry, "gotaskqueue_tasks_failed_total 1")
	assertMetricContains(t, registry, "gotaskqueue_tasks_dead_total 1")
}

type blockingExecutor struct{}

func (blockingExecutor) Execute(ctx context.Context, _ *task.Task) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestHandleMessageTimeoutTransitionsToRetryingAndAcks(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:             "task_timeout",
			Status:         task.StatusPending,
			TimeoutSeconds: 1,
			RetryCount:     0,
			MaxRetries:     3,
			CreatedAt:      time.Now().UTC(),
		},
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithExecutor(blockingExecutor{})

	start := time.Now()
	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task: queue.TaskMessage{
			ID:   "task_timeout",
			Type: "email.send",
		},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}
	if time.Since(start) < time.Second {
		t.Fatal("expected task to wait for timeout")
	}

	if !store.markFailed {
		t.Fatal("expected timeout task to be marked failed")
	}
	if store.lastFailure != "task execution timed out after 1s" {
		t.Fatalf("expected timeout failure message, got %q", store.lastFailure)
	}
	if store.completeStatus != task.StatusRetrying {
		t.Fatalf("expected retrying final status, got %q", store.completeStatus)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "1-0" {
		t.Fatalf("expected redis message ack, got %#v", consumer.acked)
	}
}

func assertMetricContains(t *testing.T, registry *metrics.Registry, expected string) {
	t.Helper()

	var body strings.Builder
	if err := registry.WritePrometheus(context.Background(), &body); err != nil {
		t.Fatalf("write metrics: %v", err)
	}
	if !strings.Contains(body.String(), expected) {
		t.Fatalf("expected metrics to contain %q, got:\n%s", expected, body.String())
	}
}
