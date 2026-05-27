package task

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/metrics"
	"github.com/bird0711/GoTaskQueue/internal/queue"
)

type fakeStore struct {
	created *Task
}

func (s *fakeStore) Create(context.Context, CreateTaskParams) (*Task, error) {
	return s.created, nil
}

type capturingStore struct {
	created    *Task
	lastParams CreateTaskParams
}

func (s *capturingStore) Create(_ context.Context, params CreateTaskParams) (*Task, error) {
	s.lastParams = params
	return s.created, nil
}

func (s *capturingStore) Get(context.Context, string) (*Task, error) {
	return nil, ErrNotFound
}

func (s *capturingStore) MetricsSnapshot(context.Context, time.Time) (metrics.Snapshot, error) {
	return metrics.Snapshot{}, nil
}

func (s *capturingStore) CountByStatus(context.Context) (map[Status]int64, error) {
	return nil, nil
}

func (s *capturingStore) RecentByStatus(context.Context, Status, int) ([]*Task, error) {
	return nil, nil
}

func (s *capturingStore) Recent(context.Context, int) ([]*Task, error) {
	return nil, nil
}

func (s *fakeStore) Get(context.Context, string) (*Task, error) {
	return nil, ErrNotFound
}

func (s *fakeStore) MetricsSnapshot(context.Context, time.Time) (metrics.Snapshot, error) {
	return metrics.Snapshot{}, nil
}

func (s *fakeStore) CountByStatus(context.Context) (map[Status]int64, error) {
	return nil, nil
}

func (s *fakeStore) RecentByStatus(context.Context, Status, int) ([]*Task, error) {
	return nil, nil
}

func (s *fakeStore) Recent(context.Context, int) ([]*Task, error) {
	return nil, nil
}

type fakePublisher struct {
	messages []queue.TaskMessage
	err      error
}

func (p *fakePublisher) PublishTask(_ context.Context, message queue.TaskMessage) error {
	p.messages = append(p.messages, message)
	return p.err
}

type fakeScheduler struct {
	taskID string
	runAt  time.Time
	err    error
}

func (s *fakeScheduler) ScheduleTask(_ context.Context, taskID string, runAt time.Time) error {
	s.taskID = taskID
	s.runAt = runAt
	return s.err
}

func TestServicePublishesPendingTask(t *testing.T) {
	task := &Task{
		ID:           "task_123",
		Type:         "email.send",
		Payload:      json.RawMessage(`{}`),
		Status:       StatusPending,
		NewlyCreated: true,
		RunAt:        time.Now().UTC(),
	}
	publisher := &fakePublisher{}
	service := NewService(&fakeStore{created: task}, publisher, &fakeScheduler{})

	created, err := service.Create(context.Background(), CreateTaskParams{})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if created.ID != task.ID {
		t.Fatalf("expected task id %q, got %q", task.ID, created.ID)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("expected one published message, got %d", len(publisher.messages))
	}
	if publisher.messages[0].ID != task.ID {
		t.Fatalf("expected published task id %q, got %q", task.ID, publisher.messages[0].ID)
	}
	if publisher.messages[0].Type != task.Type {
		t.Fatalf("expected published task type %q, got %q", task.Type, publisher.messages[0].Type)
	}
}

func TestServiceRecordsSubmittedMetric(t *testing.T) {
	task := &Task{
		ID:           "task_123",
		Type:         "email.send",
		Payload:      json.RawMessage(`{}`),
		Status:       StatusPending,
		NewlyCreated: true,
		RunAt:        time.Now().UTC(),
	}
	publisher := &fakePublisher{}
	registry := metrics.NewRegistry()
	service := NewService(&fakeStore{created: task}, publisher, &fakeScheduler{}).WithMetrics(registry)

	if _, err := service.Create(context.Background(), CreateTaskParams{}); err != nil {
		t.Fatalf("create task: %v", err)
	}

	var body strings.Builder
	if err := registry.WritePrometheus(context.Background(), &body); err != nil {
		t.Fatalf("write metrics: %v", err)
	}
	if !strings.Contains(body.String(), "gotaskqueue_tasks_submitted_total 1") {
		t.Fatalf("expected submitted metric, got:\n%s", body.String())
	}
}

func TestServiceDoesNotPublishScheduledTask(t *testing.T) {
	task := &Task{
		ID:           "task_123",
		Type:         "email.send",
		Payload:      json.RawMessage(`{}`),
		Status:       StatusScheduled,
		NewlyCreated: true,
		RunAt:        time.Now().UTC().Add(time.Hour),
	}
	publisher := &fakePublisher{}
	scheduler := &fakeScheduler{}
	service := NewService(&fakeStore{created: task}, publisher, scheduler)

	if _, err := service.Create(context.Background(), CreateTaskParams{}); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("expected no published messages, got %d", len(publisher.messages))
	}
	if scheduler.taskID != task.ID {
		t.Fatalf("expected scheduled task id %q, got %q", task.ID, scheduler.taskID)
	}
	if !scheduler.runAt.Equal(task.RunAt) {
		t.Fatalf("expected scheduled run_at %s, got %s", task.RunAt, scheduler.runAt)
	}
}

func TestServiceReturnsPublishError(t *testing.T) {
	task := &Task{
		ID:           "task_123",
		Type:         "email.send",
		Payload:      json.RawMessage(`{}`),
		Status:       StatusPending,
		NewlyCreated: true,
		RunAt:        time.Now().UTC(),
	}
	publisher := &fakePublisher{err: errors.New("redis unavailable")}
	service := NewService(&fakeStore{created: task}, publisher, &fakeScheduler{})

	if _, err := service.Create(context.Background(), CreateTaskParams{}); err == nil {
		t.Fatal("expected publish error")
	}
}

func TestServiceDoesNotPublishExistingIdempotentTask(t *testing.T) {
	key := "request-123"
	task := &Task{
		ID:             "task_123",
		Type:           "email.send",
		Payload:        json.RawMessage(`{}`),
		Status:         StatusPending,
		NewlyCreated:   false,
		IdempotencyKey: &key,
		RunAt:          time.Now().UTC(),
	}
	publisher := &fakePublisher{}
	service := NewService(&fakeStore{created: task}, publisher, &fakeScheduler{})

	created, err := service.Create(context.Background(), CreateTaskParams{IdempotencyKey: &key})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if created.ID != task.ID {
		t.Fatalf("expected existing task id %q, got %q", task.ID, created.ID)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("expected no published messages for existing idempotent task, got %d", len(publisher.messages))
	}
}

func TestServiceGeneratesTraceIDWhenAbsent(t *testing.T) {
	traceID := "trace_generated"
	stored := &Task{
		ID:           "task_123",
		Type:         "email.send",
		Payload:      json.RawMessage(`{}`),
		Status:       StatusPending,
		NewlyCreated: true,
		TraceID:      &traceID,
		RunAt:        time.Now().UTC(),
	}
	store := &capturingStore{created: stored}
	publisher := &fakePublisher{}
	service := NewService(store, publisher, &fakeScheduler{})

	if _, err := service.Create(context.Background(), CreateTaskParams{}); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if store.lastParams.TraceID == nil || !strings.HasPrefix(*store.lastParams.TraceID, "trace_") {
		t.Fatalf("expected generated trace id, got %#v", store.lastParams.TraceID)
	}
	if len(publisher.messages) != 1 || publisher.messages[0].TraceID != traceID {
		t.Fatalf("expected publisher to receive trace id %q, got %#v", traceID, publisher.messages)
	}
}

func TestServicePreservesProvidedTraceID(t *testing.T) {
	provided := "trace_external"
	stored := &Task{
		ID:           "task_123",
		Type:         "email.send",
		Payload:      json.RawMessage(`{}`),
		Status:       StatusPending,
		NewlyCreated: true,
		TraceID:      &provided,
		RunAt:        time.Now().UTC(),
	}
	store := &capturingStore{created: stored}
	publisher := &fakePublisher{}
	service := NewService(store, publisher, &fakeScheduler{})

	if _, err := service.Create(context.Background(), CreateTaskParams{TraceID: &provided}); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if store.lastParams.TraceID == nil || *store.lastParams.TraceID != provided {
		t.Fatalf("expected preserved trace id %q, got %#v", provided, store.lastParams.TraceID)
	}
	if len(publisher.messages) != 1 || publisher.messages[0].TraceID != provided {
		t.Fatalf("expected publisher to receive trace id %q, got %#v", provided, publisher.messages)
	}
}

func TestServiceReturnsScheduleError(t *testing.T) {
	task := &Task{
		ID:           "task_123",
		Type:         "email.send",
		Payload:      json.RawMessage(`{}`),
		Status:       StatusScheduled,
		NewlyCreated: true,
		RunAt:        time.Now().UTC().Add(time.Hour),
	}
	scheduler := &fakeScheduler{err: errors.New("redis unavailable")}
	service := NewService(&fakeStore{created: task}, &fakePublisher{}, scheduler)

	if _, err := service.Create(context.Background(), CreateTaskParams{}); err == nil {
		t.Fatal("expected schedule error")
	}
}
