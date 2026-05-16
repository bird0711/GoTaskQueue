package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/queue"
	"github.com/bird0711/GoTaskQueue/internal/task"
)

type fakeStore struct {
	due         []*task.Task
	transitions []transitionCall
	err         error
}

type transitionCall struct {
	id   string
	from task.Status
	to   task.Status
}

func (s *fakeStore) DueDispatchable(context.Context, time.Time, int) ([]*task.Task, error) {
	return s.due, nil
}

func (s *fakeStore) Transition(_ context.Context, id string, from task.Status, to task.Status, _ *string) (*task.Task, error) {
	s.transitions = append(s.transitions, transitionCall{id: id, from: from, to: to})
	if s.err != nil {
		return nil, s.err
	}
	return &task.Task{
		ID:     id,
		Type:   "email.send",
		Status: to,
	}, nil
}

type fakePublisher struct {
	messages []queue.TaskMessage
	err      error
}

func (p *fakePublisher) PublishTask(_ context.Context, message queue.TaskMessage) error {
	p.messages = append(p.messages, message)
	return p.err
}

func TestTickPublishesDueTasksAfterTransition(t *testing.T) {
	store := &fakeStore{
		due: []*task.Task{
			{ID: "task_1", Type: "email.send", Status: task.StatusScheduled},
		},
	}
	publisher := &fakePublisher{}
	scheduler := New(store, publisher, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if len(store.transitions) != 1 || store.transitions[0].id != "task_1" {
		t.Fatalf("expected task transition, got %#v", store.transitions)
	}
	if store.transitions[0].from != task.StatusScheduled || store.transitions[0].to != task.StatusPending {
		t.Fatalf("expected task transition, got %#v", store.transitions)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("expected one published message, got %d", len(publisher.messages))
	}
	if publisher.messages[0].ID != "task_1" {
		t.Fatalf("expected task_1 message, got %q", publisher.messages[0].ID)
	}
}

func TestTickPublishesDueRetryingTaskAfterTransition(t *testing.T) {
	nextRetryAt := time.Now().UTC().Add(-1 * time.Second)
	store := &fakeStore{
		due: []*task.Task{
			{ID: "task_retry", Type: "email.send", Status: task.StatusRetrying, NextRetryAt: &nextRetryAt},
		},
	}
	publisher := &fakePublisher{}
	scheduler := New(store, publisher, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if len(store.transitions) != 1 {
		t.Fatalf("expected one transition, got %#v", store.transitions)
	}
	if store.transitions[0].from != task.StatusRetrying || store.transitions[0].to != task.StatusPending {
		t.Fatalf("expected retrying -> pending transition, got %#v", store.transitions[0])
	}
	if len(publisher.messages) != 1 || publisher.messages[0].ID != "task_retry" {
		t.Fatalf("expected retry task message, got %#v", publisher.messages)
	}
}

func TestTickDoesNotPublishWhenTaskAlreadyClaimed(t *testing.T) {
	store := &fakeStore{
		due: []*task.Task{
			{ID: "task_1", Type: "email.send", Status: task.StatusScheduled},
		},
		err: task.ErrNotFound,
	}
	publisher := &fakePublisher{}
	scheduler := New(store, publisher, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("expected no published messages, got %d", len(publisher.messages))
	}
}

func TestTickLogsPublishErrorAndContinues(t *testing.T) {
	store := &fakeStore{
		due: []*task.Task{
			{ID: "task_1", Type: "email.send", Status: task.StatusScheduled},
		},
	}
	publisher := &fakePublisher{err: errors.New("redis unavailable")}
	scheduler := New(store, publisher, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("expected publish attempt, got %d", len(publisher.messages))
	}
}
