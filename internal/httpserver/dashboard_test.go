package httpserver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/metrics"
	"github.com/bird0711/GoTaskQueue/internal/task"
)

type fakeDashboardStore struct {
	counts map[task.Status]int64
	tasks  map[string]*task.Task
	recent []*task.Task
	failed []*task.Task
	dead   []*task.Task
}

func (s *fakeDashboardStore) Create(context.Context, task.CreateTaskParams) (*task.Task, error) {
	return nil, nil
}

func (s *fakeDashboardStore) Get(_ context.Context, id string) (*task.Task, error) {
	if s.tasks == nil {
		return nil, task.ErrNotFound
	}
	found, ok := s.tasks[id]
	if !ok {
		return nil, task.ErrNotFound
	}
	return found, nil
}

func (s *fakeDashboardStore) MetricsSnapshot(context.Context, time.Time) (metrics.Snapshot, error) {
	return metrics.Snapshot{
		QueueBacklog: 4,
		RunningTasks: 2,
	}, nil
}

func (s *fakeDashboardStore) CountByStatus(context.Context) (map[task.Status]int64, error) {
	return s.counts, nil
}

func (s *fakeDashboardStore) RecentByStatus(_ context.Context, status task.Status, _ int) ([]*task.Task, error) {
	switch status {
	case task.StatusFailed:
		return s.failed, nil
	case task.StatusDead:
		return s.dead, nil
	default:
		return nil, nil
	}
}

func (s *fakeDashboardStore) Recent(context.Context, int) ([]*task.Task, error) {
	return s.recent, nil
}

func TestDashboardEndpointRendersTaskOverview(t *testing.T) {
	failure := "smtp unavailable"
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	payload := []byte(`{"message":"hello"}`)
	store := &fakeDashboardStore{
		counts: map[task.Status]int64{
			task.StatusPending: 3,
			task.StatusRunning: 2,
			task.StatusSuccess: 8,
			task.StatusFailed:  1,
			task.StatusDead:    1,
		},
		tasks: map[string]*task.Task{
			"task_detail": {
				ID:             "task_detail",
				Type:           "demo.echo",
				Status:         task.StatusSuccess,
				Payload:        payload,
				RunAt:          now,
				TimeoutSeconds: 30,
				MaxRetries:     3,
				RetryCount:     1,
				CreatedAt:      now.Add(-time.Minute),
				UpdatedAt:      now,
			},
		},
		recent: []*task.Task{
			{
				ID:        "task_detail",
				Type:      "demo.echo",
				Status:    task.StatusSuccess,
				Payload:   payload,
				RunAt:     now,
				UpdatedAt: now,
			},
		},
		failed: []*task.Task{
			{
				ID:         "task_failed",
				Type:       "email.send",
				Status:     task.StatusFailed,
				RetryCount: 1,
				MaxRetries: 3,
				LastError:  &failure,
				UpdatedAt:  now,
			},
		},
		dead: []*task.Task{
			{
				ID:         "task_dead",
				Type:       "image.resize",
				Status:     task.StatusDead,
				RetryCount: 4,
				MaxRetries: 3,
				LastError:  &failure,
				UpdatedAt:  now,
			},
		},
	}
	server := New(Config{Addr: ":0"}, store, metrics.NewRegistry())

	request := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	response := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(response, request)

	result := response.Result()
	defer result.Body.Close()
	if result.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", result.StatusCode)
	}
	if contentType := result.Header.Get("Content-Type"); !strings.HasPrefix(contentType, "text/html") {
		t.Fatalf("expected text/html content type, got %q", contentType)
	}

	bodyBytes, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	body := string(bodyBytes)
	expectedContent := []string{
		"GoTaskQueue",
		"Total tasks",
		">15<",
		"Queue backlog",
		">4<",
		"Running tasks",
		">2<",
		"task_failed",
		"task_dead",
		"Recent Tasks",
		"/dashboard/tasks/task_failed",
		"/dashboard/tasks/task_dead",
		"/dashboard/tasks/task_detail",
		"smtp unavailable",
	}
	for _, expected := range expectedContent {
		if !strings.Contains(body, expected) {
			t.Fatalf("expected dashboard to contain %q, got:\n%s", expected, body)
		}
	}
}

func TestDashboardTaskDetailRendersTaskFields(t *testing.T) {
	failure := "handler failed"
	workerID := "worker-1"
	idempotencyKey := "request-1"
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	startedAt := now.Add(-2 * time.Minute)
	finishedAt := now.Add(-time.Minute)
	nextRetryAt := now.Add(time.Minute)
	store := &fakeDashboardStore{
		tasks: map[string]*task.Task{
			"task_detail": {
				ID:             "task_detail",
				Type:           "demo.echo",
				Status:         task.StatusDead,
				Payload:        []byte(`{"message":"hello","ok":true}`),
				IdempotencyKey: &idempotencyKey,
				RunAt:          now.Add(-3 * time.Minute),
				TimeoutSeconds: 45,
				MaxRetries:     3,
				RetryCount:     3,
				NextRetryAt:    &nextRetryAt,
				LastError:      &failure,
				WorkerID:       &workerID,
				StartedAt:      &startedAt,
				FinishedAt:     &finishedAt,
				CreatedAt:      now.Add(-5 * time.Minute),
				UpdatedAt:      now,
			},
		},
	}
	server := New(Config{Addr: ":0"}, store, metrics.NewRegistry())

	request := httptest.NewRequest(http.MethodGet, "/dashboard/tasks/task_detail", nil)
	request.SetPathValue("id", "task_detail")
	response := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(response, request)

	result := response.Result()
	defer result.Body.Close()
	if result.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", result.StatusCode)
	}
	if contentType := result.Header.Get("Content-Type"); !strings.HasPrefix(contentType, "text/html") {
		t.Fatalf("expected text/html content type, got %q", contentType)
	}

	bodyBytes, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	body := string(bodyBytes)
	expectedContent := []string{
		"Back to dashboard",
		"task_detail",
		"demo.echo",
		"handler failed",
		"worker-1",
		"request-1",
		`&#34;message&#34;: &#34;hello&#34;`,
		`&#34;ok&#34;: true`,
	}
	for _, expected := range expectedContent {
		if !strings.Contains(body, expected) {
			t.Fatalf("expected task detail to contain %q, got:\n%s", expected, body)
		}
	}
}
