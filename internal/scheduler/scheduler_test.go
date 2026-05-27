package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/queue"
	"github.com/bird0711/GoTaskQueue/internal/task"
)

type fakeStore struct {
	tasks       map[string]*task.Task
	expired     []*task.Task
	dueRetrying []*task.Task
	transitions []transitionCall
	markFailed  []string
	failures    []string
	completed   []task.FailureDecision
	err         error
}

type transitionCall struct {
	id   string
	from task.Status
	to   task.Status
}

func (s *fakeStore) Get(_ context.Context, id string) (*task.Task, error) {
	found, ok := s.tasks[id]
	if !ok {
		return nil, task.ErrNotFound
	}
	return found, nil
}

func (s *fakeStore) DueRetrying(context.Context, time.Time, int) ([]*task.Task, error) {
	return s.dueRetrying, nil
}

func (s *fakeStore) ExpiredRunning(context.Context, time.Time, int) ([]*task.Task, error) {
	return s.expired, nil
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

func (s *fakeStore) MarkFailed(_ context.Context, id string, failure string) (*task.Task, error) {
	s.markFailed = append(s.markFailed, id)
	s.failures = append(s.failures, failure)
	if s.err != nil {
		return nil, s.err
	}

	expiredTask := s.tasks[id]
	if expiredTask == nil {
		for _, candidate := range s.expired {
			if candidate.ID == id {
				expiredTask = candidate
				break
			}
		}
	}
	if expiredTask == nil {
		return nil, task.ErrNotFound
	}

	return &task.Task{
		ID:         expiredTask.ID,
		Type:       expiredTask.Type,
		Status:     task.StatusFailed,
		RetryCount: expiredTask.RetryCount,
		MaxRetries: expiredTask.MaxRetries,
	}, nil
}

func (s *fakeStore) CompleteFailure(_ context.Context, id string, decision task.FailureDecision, failure string) (*task.Task, error) {
	s.completed = append(s.completed, decision)
	s.failures = append(s.failures, failure)
	if s.err != nil {
		return nil, s.err
	}
	return &task.Task{
		ID:         id,
		Status:     decision.Status,
		RetryCount: decision.RetryCount,
	}, nil
}

type fakeScheduledQueue struct {
	dueIDs  []string
	removed []string
}

func (q *fakeScheduledQueue) DueTaskIDs(context.Context, time.Time, int) ([]string, error) {
	return q.dueIDs, nil
}

func (q *fakeScheduledQueue) Remove(_ context.Context, taskID string) error {
	q.removed = append(q.removed, taskID)
	return nil
}

type fakePublisher struct {
	messages []queue.TaskMessage
	err      error
}

func (p *fakePublisher) PublishTask(_ context.Context, message queue.TaskMessage) error {
	p.messages = append(p.messages, message)
	return p.err
}

func TestTickPublishesDueZSetTaskAfterTransition(t *testing.T) {
	store := &fakeStore{
		tasks: map[string]*task.Task{
			"task_1": {ID: "task_1", Type: "email.send", Status: task.StatusScheduled},
		},
	}
	scheduledQueue := &fakeScheduledQueue{dueIDs: []string{"task_1"}}
	publisher := &fakePublisher{}
	scheduler := New(store, publisher, scheduledQueue, slog.Default(), time.Second, 100)

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
	if len(scheduledQueue.removed) != 1 || scheduledQueue.removed[0] != "task_1" {
		t.Fatalf("expected zset task removal, got %#v", scheduledQueue.removed)
	}
}

func TestTickRecoversExpiredRunningTaskToRetrying(t *testing.T) {
	startedAt := time.Now().UTC().Add(-10 * time.Second)
	store := &fakeStore{
		expired: []*task.Task{
			{
				ID:             "task_expired",
				Type:           "email.send",
				Status:         task.StatusRunning,
				TimeoutSeconds: 1,
				RetryCount:     0,
				MaxRetries:     3,
				StartedAt:      &startedAt,
			},
		},
	}
	scheduler := New(store, &fakePublisher{}, &fakeScheduledQueue{}, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if len(store.markFailed) != 1 || store.markFailed[0] != "task_expired" {
		t.Fatalf("expected expired task marked failed, got %#v", store.markFailed)
	}
	if len(store.completed) != 1 {
		t.Fatalf("expected one failure completion, got %#v", store.completed)
	}
	if store.completed[0].Status != task.StatusRetrying {
		t.Fatalf("expected retrying decision, got %q", store.completed[0].Status)
	}
	if !strings.Contains(store.failures[0], "running timeout recovery") {
		t.Fatalf("expected running timeout recovery failure, got %q", store.failures[0])
	}
}

func TestTickRecoversExpiredRunningTaskToDead(t *testing.T) {
	startedAt := time.Now().UTC().Add(-10 * time.Second)
	store := &fakeStore{
		expired: []*task.Task{
			{
				ID:             "task_dead",
				Type:           "email.send",
				Status:         task.StatusRunning,
				TimeoutSeconds: 1,
				RetryCount:     3,
				MaxRetries:     3,
				StartedAt:      &startedAt,
			},
		},
	}
	scheduler := New(store, &fakePublisher{}, &fakeScheduledQueue{}, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if len(store.completed) != 1 {
		t.Fatalf("expected one failure completion, got %#v", store.completed)
	}
	if store.completed[0].Status != task.StatusDead {
		t.Fatalf("expected dead decision, got %q", store.completed[0].Status)
	}
}

type recordingDeadPublisher struct {
	messages []queue.DeadTaskMessage
	err      error
}

func (p *recordingDeadPublisher) PublishDead(_ context.Context, message queue.DeadTaskMessage) error {
	p.messages = append(p.messages, message)
	return p.err
}

func TestTickPublishesDeadStreamWhenRecoveryReachesDead(t *testing.T) {
	startedAt := time.Now().UTC().Add(-10 * time.Second)
	traceID := "trace_recover_dead"
	store := &fakeStore{
		expired: []*task.Task{
			{
				ID:             "task_dead",
				Type:           "email.send",
				Status:         task.StatusRunning,
				TimeoutSeconds: 1,
				RetryCount:     3,
				MaxRetries:     3,
				StartedAt:      &startedAt,
				TraceID:        &traceID,
			},
		},
	}
	deadPublisher := &recordingDeadPublisher{}
	scheduler := New(store, &fakePublisher{}, &fakeScheduledQueue{}, slog.Default(), time.Second, 100).
		WithDeadPublisher(deadPublisher)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if len(deadPublisher.messages) != 1 {
		t.Fatalf("expected one dead publish, got %d", len(deadPublisher.messages))
	}
	got := deadPublisher.messages[0]
	if got.ID != "task_dead" || got.Type != "email.send" || got.TraceID != traceID {
		t.Fatalf("unexpected dead message: %#v", got)
	}
	if !strings.Contains(got.LastError, "running timeout recovery") {
		t.Fatalf("expected last_error to mention recovery, got %q", got.LastError)
	}
}

func TestTickRetryingRecoveryDoesNotPublishDead(t *testing.T) {
	startedAt := time.Now().UTC().Add(-10 * time.Second)
	store := &fakeStore{
		expired: []*task.Task{
			{
				ID:             "task_retry",
				Type:           "email.send",
				Status:         task.StatusRunning,
				TimeoutSeconds: 1,
				RetryCount:     0,
				MaxRetries:     3,
				StartedAt:      &startedAt,
			},
		},
	}
	deadPublisher := &recordingDeadPublisher{}
	scheduler := New(store, &fakePublisher{}, &fakeScheduledQueue{}, slog.Default(), time.Second, 100).
		WithDeadPublisher(deadPublisher)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(deadPublisher.messages) != 0 {
		t.Fatalf("expected no dead publishes for retrying recovery, got %d", len(deadPublisher.messages))
	}
}

func TestTickDoesNotRecoverWhenNoExpiredRunningTasks(t *testing.T) {
	store := &fakeStore{}
	scheduler := New(store, &fakePublisher{}, &fakeScheduledQueue{}, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(store.markFailed) != 0 {
		t.Fatalf("expected no running recovery, got %#v", store.markFailed)
	}
}

func TestTickPublishesDueRetryingTaskAfterTransition(t *testing.T) {
	nextRetryAt := time.Now().UTC().Add(-1 * time.Second)
	store := &fakeStore{
		dueRetrying: []*task.Task{
			{ID: "task_retry", Type: "email.send", Status: task.StatusRetrying, NextRetryAt: &nextRetryAt},
		},
	}
	publisher := &fakePublisher{}
	scheduler := New(store, publisher, &fakeScheduledQueue{}, slog.Default(), time.Second, 100)

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

func TestTickDoesNotPublishWhenZSetTaskAlreadyClaimed(t *testing.T) {
	store := &fakeStore{
		tasks: map[string]*task.Task{
			"task_1": {ID: "task_1", Type: "email.send", Status: task.StatusScheduled},
		},
		err: task.ErrNotFound,
	}
	scheduledQueue := &fakeScheduledQueue{dueIDs: []string{"task_1"}}
	publisher := &fakePublisher{}
	scheduler := New(store, publisher, scheduledQueue, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("expected no published messages, got %d", len(publisher.messages))
	}
	if len(scheduledQueue.removed) != 1 || scheduledQueue.removed[0] != "task_1" {
		t.Fatalf("expected already claimed zset task removal, got %#v", scheduledQueue.removed)
	}
}

func TestTickLogsPublishErrorAndContinues(t *testing.T) {
	store := &fakeStore{
		tasks: map[string]*task.Task{
			"task_1": {ID: "task_1", Type: "email.send", Status: task.StatusScheduled},
		},
	}
	scheduledQueue := &fakeScheduledQueue{dueIDs: []string{"task_1"}}
	publisher := &fakePublisher{err: errors.New("redis unavailable")}
	scheduler := New(store, publisher, scheduledQueue, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("expected publish attempt, got %d", len(publisher.messages))
	}
	if len(scheduledQueue.removed) != 0 {
		t.Fatalf("expected zset member to remain after publish failure, got %#v", scheduledQueue.removed)
	}
}

func TestTickRemovesDirtyZSetMembers(t *testing.T) {
	store := &fakeStore{
		tasks: map[string]*task.Task{
			"task_success": {ID: "task_success", Type: "email.send", Status: task.StatusSuccess},
		},
	}
	scheduledQueue := &fakeScheduledQueue{dueIDs: []string{"task_missing", "task_success"}}
	publisher := &fakePublisher{}
	scheduler := New(store, publisher, scheduledQueue, slog.Default(), time.Second, 100)

	if err := scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("expected no published messages, got %d", len(publisher.messages))
	}

	expectedRemoved := []string{"task_missing", "task_success"}
	if len(scheduledQueue.removed) != len(expectedRemoved) {
		t.Fatalf("expected removed zset members %#v, got %#v", expectedRemoved, scheduledQueue.removed)
	}
	for index, expected := range expectedRemoved {
		if scheduledQueue.removed[index] != expected {
			t.Fatalf("removed zset member %d: expected %q, got %q", index, expected, scheduledQueue.removed[index])
		}
	}
}
