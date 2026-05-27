package task

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/metrics"
	"github.com/bird0711/GoTaskQueue/internal/queue"
)

type CreatorGetter interface {
	Create(context.Context, CreateTaskParams) (*Task, error)
	Get(context.Context, string) (*Task, error)
	MetricsSnapshot(context.Context, time.Time) (metrics.Snapshot, error)
	CountByStatus(context.Context) (map[Status]int64, error)
	RecentByStatus(context.Context, Status, int) ([]*Task, error)
	Recent(context.Context, int) ([]*Task, error)
}

type Publisher interface {
	PublishTask(context.Context, queue.TaskMessage) error
}

type Scheduler interface {
	ScheduleTask(context.Context, string, time.Time) error
}

type Service struct {
	store     CreatorGetter
	publisher Publisher
	scheduler Scheduler
	metrics   *metrics.Registry
}

func NewService(store CreatorGetter, publisher Publisher, scheduler Scheduler) *Service {
	return &Service{
		store:     store,
		publisher: publisher,
		scheduler: scheduler,
	}
}

func (s *Service) WithMetrics(registry *metrics.Registry) *Service {
	s.metrics = registry
	return s
}

func (s *Service) Create(ctx context.Context, params CreateTaskParams) (*Task, error) {
	if params.TraceID == nil || strings.TrimSpace(*params.TraceID) == "" {
		generated := newTraceID()
		params.TraceID = &generated
	} else {
		trimmed := strings.TrimSpace(*params.TraceID)
		params.TraceID = &trimmed
	}

	task, err := s.store.Create(ctx, params)
	if err != nil {
		return nil, err
	}

	if !task.NewlyCreated {
		return task, nil
	}

	if task.Status != StatusPending {
		if task.Status == StatusScheduled {
			if err := s.scheduler.ScheduleTask(ctx, task.ID, task.RunAt); err != nil {
				return nil, fmt.Errorf("schedule created task: %w", err)
			}
		}
		if s.metrics != nil {
			s.metrics.TaskSubmitted()
		}
		return task, nil
	}

	if err := s.publisher.PublishTask(ctx, queue.TaskMessage{
		ID:      task.ID,
		Type:    task.Type,
		TraceID: derefString(task.TraceID),
	}); err != nil {
		return nil, fmt.Errorf("publish created task: %w", err)
	}
	if s.metrics != nil {
		s.metrics.TaskSubmitted()
	}

	return task, nil
}

func newTraceID() string {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(fmt.Sprintf("generate trace id: %v", err))
	}
	return "trace_" + hex.EncodeToString(bytes[:])
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (s *Service) Get(ctx context.Context, id string) (*Task, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) MetricsSnapshot(ctx context.Context, now time.Time) (metrics.Snapshot, error) {
	return s.store.MetricsSnapshot(ctx, now)
}

func (s *Service) CountByStatus(ctx context.Context) (map[Status]int64, error) {
	return s.store.CountByStatus(ctx)
}

func (s *Service) RecentByStatus(ctx context.Context, status Status, limit int) ([]*Task, error) {
	return s.store.RecentByStatus(ctx, status, limit)
}

func (s *Service) Recent(ctx context.Context, limit int) ([]*Task, error) {
	return s.store.Recent(ctx, limit)
}
