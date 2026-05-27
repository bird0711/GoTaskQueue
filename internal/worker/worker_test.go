package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:             "task_123",
			Type:           "email.send",
			Status:         task.StatusPending,
			TimeoutSeconds: 30,
			CreatedAt:      time.Now().UTC(),
		},
	}
	registry := NewHandlerRegistry()
	if err := registry.Register("email.send", HandlerFunc(func(context.Context, *task.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithHandlerRegistry(registry)

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
	handlerRegistry := NewHandlerRegistry()
	if err := handlerRegistry.Register("email.send", HandlerFunc(func(context.Context, *task.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithHandlerRegistry(handlerRegistry).WithMetrics(registry)

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

type countingHandler struct {
	count int
}

func (h *countingHandler) Handle(context.Context, *task.Task) error {
	h.count++
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
	registry := NewHandlerRegistry()
	handler := &countingHandler{}
	if err := registry.Register("email.send", handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithHandlerRegistry(registry)

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

	if handler.count != 0 {
		t.Fatalf("expected terminal task not to execute, executed %d times", handler.count)
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
	registry := NewHandlerRegistry()
	handler := &countingHandler{}
	if err := registry.Register("email.send", handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithHandlerRegistry(registry).
		WithPendingRecovery(10*time.Second, 5, time.Second)

	err := worker.recoverPending(context.Background())
	if err != nil {
		t.Fatalf("recover pending: %v", err)
	}
	worker.wg.Wait()

	if consumer.claimMinIdle != 10*time.Second {
		t.Fatalf("expected claim min idle 10s, got %s", consumer.claimMinIdle)
	}
	if consumer.claimBatch != 5 {
		t.Fatalf("expected claim batch 5, got %d", consumer.claimBatch)
	}
	if handler.count != 1 {
		t.Fatalf("expected recovered task to execute once, executed %d times", handler.count)
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

type failingHandler struct{}

func (failingHandler) Handle(context.Context, *task.Task) error {
	return errors.New("execution failed")
}

func TestHandleMessageFailureTransitionsToRetryingAndAcks(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:             "task_123",
			Type:           "email.send",
			Status:         task.StatusPending,
			TimeoutSeconds: 30,
			RetryCount:     0,
			MaxRetries:     3,
			CreatedAt:      time.Now().UTC(),
		},
	}
	registry := NewHandlerRegistry()
	if err := registry.Register("email.send", failingHandler{}); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithHandlerRegistry(registry)

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
	handlerRegistry := NewHandlerRegistry()
	if err := handlerRegistry.Register("email.send", failingHandler{}); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithHandlerRegistry(handlerRegistry).WithMetrics(registry)

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

type recordingDeadPublisher struct {
	messages []queue.DeadTaskMessage
	err      error
}

func (p *recordingDeadPublisher) PublishDead(_ context.Context, message queue.DeadTaskMessage) error {
	p.messages = append(p.messages, message)
	return p.err
}

func TestHandleMessageDeadPublishesToDeadStream(t *testing.T) {
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
	registry := NewHandlerRegistry()
	if err := registry.Register("email.send", failingHandler{}); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	deadPublisher := &recordingDeadPublisher{}
	worker := New(consumer, store, slog.Default(), "test-worker").
		WithHandlerRegistry(registry).
		WithDeadPublisher(deadPublisher)

	traceID := "trace_dead"
	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task: queue.TaskMessage{
			ID:      "task_dead",
			Type:    "email.send",
			TraceID: traceID,
		},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	if len(deadPublisher.messages) != 1 {
		t.Fatalf("expected one dead publish, got %d", len(deadPublisher.messages))
	}
	got := deadPublisher.messages[0]
	if got.ID != "task_dead" || got.Type != "email.send" || got.TraceID != traceID {
		t.Fatalf("unexpected dead message: %#v", got)
	}
	if got.LastError != "execution failed" {
		t.Fatalf("expected last_error 'execution failed', got %q", got.LastError)
	}
}

func TestHandleMessageDeadPublishFailureDoesNotAffectAck(t *testing.T) {
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
	registry := NewHandlerRegistry()
	if err := registry.Register("email.send", failingHandler{}); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	deadPublisher := &recordingDeadPublisher{err: errors.New("redis unavailable")}
	worker := New(consumer, store, slog.Default(), "test-worker").
		WithHandlerRegistry(registry).
		WithDeadPublisher(deadPublisher)

	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task:    queue.TaskMessage{ID: "task_dead", Type: "email.send"},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	if store.completeStatus != task.StatusDead {
		t.Fatalf("expected dead final status despite publish failure, got %q", store.completeStatus)
	}
	if len(consumer.acked) != 1 || consumer.acked[0] != "1-0" {
		t.Fatalf("expected ack despite publish failure, got %#v", consumer.acked)
	}
}

func TestHandleMessageRetryingDoesNotPublishDead(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:             "task_retry",
			Type:           "email.send",
			Status:         task.StatusPending,
			TimeoutSeconds: 30,
			RetryCount:     0,
			MaxRetries:     3,
			CreatedAt:      time.Now().UTC(),
		},
	}
	registry := NewHandlerRegistry()
	if err := registry.Register("email.send", failingHandler{}); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	deadPublisher := &recordingDeadPublisher{}
	worker := New(consumer, store, slog.Default(), "test-worker").
		WithHandlerRegistry(registry).
		WithDeadPublisher(deadPublisher)

	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task:    queue.TaskMessage{ID: "task_retry", Type: "email.send"},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	if len(deadPublisher.messages) != 0 {
		t.Fatalf("expected no dead publishes for retrying task, got %d", len(deadPublisher.messages))
	}
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
	handlerRegistry := NewHandlerRegistry()
	if err := handlerRegistry.Register("email.send", failingHandler{}); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithHandlerRegistry(handlerRegistry).WithMetrics(registry)

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

type blockingHandler struct{}

func (blockingHandler) Handle(ctx context.Context, _ *task.Task) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestHandleMessageTimeoutTransitionsToRetryingAndAcks(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:             "task_timeout",
			Type:           "email.send",
			Status:         task.StatusPending,
			TimeoutSeconds: 1,
			RetryCount:     0,
			MaxRetries:     3,
			CreatedAt:      time.Now().UTC(),
		},
	}
	registry := NewHandlerRegistry()
	if err := registry.Register("email.send", blockingHandler{}); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	worker := New(consumer, store, slog.Default(), "test-worker").WithHandlerRegistry(registry)

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

func TestHandleMessageUnknownTaskTypeTransitionsToRetryingAndAcks(t *testing.T) {
	consumer := &fakeConsumer{}
	store := &fakeTaskStore{
		runningTask: &task.Task{
			ID:         "task_unknown",
			Type:       "unknown.type",
			Status:     task.StatusPending,
			RetryCount: 0,
			MaxRetries: 3,
			CreatedAt:  time.Now().UTC(),
		},
	}
	worker := New(consumer, store, slog.Default(), "test-worker")

	err := worker.handleMessage(context.Background(), queue.StreamMessage{
		RedisID: "1-0",
		Task: queue.TaskMessage{
			ID:   "task_unknown",
			Type: "unknown.type",
		},
	})
	if err != nil {
		t.Fatalf("handle message: %v", err)
	}

	if !store.markFailed {
		t.Fatal("expected task to be marked failed")
	}
	if store.lastFailure != "no handler registered for task_type \"unknown.type\"" {
		t.Fatalf("expected unknown task type failure message, got %q", store.lastFailure)
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

type channelConsumer struct {
	messages chan queue.StreamMessage
	mu       sync.Mutex
	acked    []string
}

func (c *channelConsumer) EnsureGroup(context.Context) error { return nil }

func (c *channelConsumer) Read(ctx context.Context) (queue.StreamMessage, error) {
	select {
	case <-ctx.Done():
		return queue.StreamMessage{}, ctx.Err()
	case message, ok := <-c.messages:
		if !ok {
			return queue.StreamMessage{}, queue.ErrNoMessages
		}
		return message, nil
	}
}

func (c *channelConsumer) ClaimPending(context.Context, time.Duration, int64) ([]queue.StreamMessage, error) {
	return nil, nil
}

func (c *channelConsumer) Ack(_ context.Context, redisID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acked = append(c.acked, redisID)
	return nil
}

func (c *channelConsumer) ackedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.acked)
}

type concurrentStore struct {
	mu       sync.Mutex
	statuses map[string]task.Status
}

func newConcurrentStore() *concurrentStore {
	return &concurrentStore{statuses: make(map[string]task.Status)}
}

func (s *concurrentStore) Get(_ context.Context, id string) (*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.statuses[id]
	if !ok {
		status = task.StatusPending
	}
	return &task.Task{
		ID:             id,
		Type:           "concurrent.test",
		Status:         status,
		TimeoutSeconds: 60,
		MaxRetries:     3,
		CreatedAt:      time.Now().UTC(),
	}, nil
}

func (s *concurrentStore) Transition(_ context.Context, id string, _ task.Status, to task.Status, _ *string) (*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[id] = to
	return &task.Task{
		ID:             id,
		Type:           "concurrent.test",
		Status:         to,
		TimeoutSeconds: 60,
		MaxRetries:     3,
		CreatedAt:      time.Now().UTC(),
	}, nil
}

func (s *concurrentStore) MarkFailed(_ context.Context, id string, failure string) (*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[id] = task.StatusFailed
	return &task.Task{
		ID:         id,
		Type:       "concurrent.test",
		Status:     task.StatusFailed,
		MaxRetries: 3,
		LastError:  &failure,
	}, nil
}

func (s *concurrentStore) CompleteFailure(_ context.Context, id string, decision task.FailureDecision, _ string) (*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[id] = decision.Status
	return &task.Task{
		ID:          id,
		Status:      decision.Status,
		RetryCount:  decision.RetryCount,
		NextRetryAt: decision.NextRetryAt,
	}, nil
}

type gateHandler struct {
	started chan struct{}
	release chan struct{}
}

func (h *gateHandler) Handle(ctx context.Context, _ *task.Task) error {
	h.started <- struct{}{}
	select {
	case <-h.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRunFanOutRespectsConcurrencyLimit(t *testing.T) {
	consumer := &channelConsumer{messages: make(chan queue.StreamMessage, 10)}
	store := newConcurrentStore()
	handler := &gateHandler{
		started: make(chan struct{}, 10),
		release: make(chan struct{}),
	}
	registry := NewHandlerRegistry()
	if err := registry.Register("concurrent.test", handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	worker := New(consumer, store, slog.Default(), "test-worker").
		WithHandlerRegistry(registry).
		WithConcurrency(2).
		WithPendingRecovery(0, 0, 0)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(ctx) }()

	for index := 0; index < 4; index++ {
		consumer.messages <- queue.StreamMessage{
			RedisID: fmt.Sprintf("msg-%d", index),
			Task:    queue.TaskMessage{ID: fmt.Sprintf("task-%d", index), Type: "concurrent.test"},
		}
	}

	for index := 0; index < 2; index++ {
		select {
		case <-handler.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected %d handlers to start, only saw %d", 2, index)
		}
	}

	select {
	case <-handler.started:
		t.Fatal("third handler started before any slot was released; concurrency limit not enforced")
	case <-time.After(150 * time.Millisecond):
	}

	close(handler.release)

	for index := 0; index < 2; index++ {
		select {
		case <-handler.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("remaining handler did not start after slots freed (saw %d)", index)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for consumer.ackedCount() < 4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := consumer.ackedCount(); got != 4 {
		t.Fatalf("expected 4 acks, got %d", got)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("worker run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker run did not return after cancel")
	}
}

type stickyHandler struct {
	started chan struct{}
	release chan struct{}
}

func (h *stickyHandler) Handle(_ context.Context, _ *task.Task) error {
	h.started <- struct{}{}
	<-h.release
	return nil
}

func TestRunWaitsForInFlightTasksOnCancel(t *testing.T) {
	consumer := &channelConsumer{messages: make(chan queue.StreamMessage, 4)}
	store := newConcurrentStore()
	handler := &stickyHandler{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	registry := NewHandlerRegistry()
	if err := registry.Register("concurrent.test", handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	worker := New(consumer, store, slog.Default(), "test-worker").
		WithHandlerRegistry(registry).
		WithConcurrency(2).
		WithPendingRecovery(0, 0, 0)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(ctx) }()

	for index := 0; index < 2; index++ {
		consumer.messages <- queue.StreamMessage{
			RedisID: fmt.Sprintf("msg-%d", index),
			Task:    queue.TaskMessage{ID: fmt.Sprintf("task-%d", index), Type: "concurrent.test"},
		}
	}

	for index := 0; index < 2; index++ {
		select {
		case <-handler.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected handler %d to start", index)
		}
	}

	cancel()

	select {
	case <-runDone:
		t.Fatal("worker run returned before in-flight tasks completed")
	case <-time.After(200 * time.Millisecond):
	}

	close(handler.release)

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("worker run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker run did not return after handlers released")
	}

	if got := consumer.ackedCount(); got != 2 {
		t.Fatalf("expected 2 acks for completed tasks, got %d", got)
	}
}
