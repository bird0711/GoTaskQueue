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

type fakePublisher struct {
	messages []queue.TaskMessage
	err      error
}

func (p *fakePublisher) PublishTask(_ context.Context, message queue.TaskMessage) error {
	p.messages = append(p.messages, message)
	return p.err
}

func TestServicePublishesPendingTask(t *testing.T) {
	task := &Task{
		ID:      "task_123",
		Type:    "email.send",
		Payload: json.RawMessage(`{}`),
		Status:  StatusPending,
		RunAt:   time.Now().UTC(),
	}
	publisher := &fakePublisher{}
	service := NewService(&fakeStore{created: task}, publisher)

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
		ID:      "task_123",
		Type:    "email.send",
		Payload: json.RawMessage(`{}`),
		Status:  StatusPending,
		RunAt:   time.Now().UTC(),
	}
	publisher := &fakePublisher{}
	registry := metrics.NewRegistry()
	service := NewService(&fakeStore{created: task}, publisher).WithMetrics(registry)

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
		ID:      "task_123",
		Type:    "email.send",
		Payload: json.RawMessage(`{}`),
		Status:  StatusScheduled,
		RunAt:   time.Now().UTC().Add(time.Hour),
	}
	publisher := &fakePublisher{}
	service := NewService(&fakeStore{created: task}, publisher)

	if _, err := service.Create(context.Background(), CreateTaskParams{}); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("expected no published messages, got %d", len(publisher.messages))
	}
}

func TestServiceReturnsPublishError(t *testing.T) {
	task := &Task{
		ID:      "task_123",
		Type:    "email.send",
		Payload: json.RawMessage(`{}`),
		Status:  StatusPending,
		RunAt:   time.Now().UTC(),
	}
	publisher := &fakePublisher{err: errors.New("redis unavailable")}
	service := NewService(&fakeStore{created: task}, publisher)

	if _, err := service.Create(context.Background(), CreateTaskParams{}); err == nil {
		t.Fatal("expected publish error")
	}
}
