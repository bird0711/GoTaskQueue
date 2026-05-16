package task

import (
	"context"
	"fmt"
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
}

type Publisher interface {
	PublishTask(context.Context, queue.TaskMessage) error
}

type Service struct {
	store     CreatorGetter
	publisher Publisher
	metrics   *metrics.Registry
}

func NewService(store CreatorGetter, publisher Publisher) *Service {
	return &Service{
		store:     store,
		publisher: publisher,
	}
}

func (s *Service) WithMetrics(registry *metrics.Registry) *Service {
	s.metrics = registry
	return s
}

func (s *Service) Create(ctx context.Context, params CreateTaskParams) (*Task, error) {
	task, err := s.store.Create(ctx, params)
	if err != nil {
		return nil, err
	}

	if task.Status != StatusPending {
		if s.metrics != nil {
			s.metrics.TaskSubmitted()
		}
		return task, nil
	}

	if err := s.publisher.PublishTask(ctx, queue.TaskMessage{
		ID:   task.ID,
		Type: task.Type,
	}); err != nil {
		return nil, fmt.Errorf("publish created task: %w", err)
	}
	if s.metrics != nil {
		s.metrics.TaskSubmitted()
	}

	return task, nil
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
