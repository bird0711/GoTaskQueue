package metrics

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

type fakeSnapshotProvider struct{}

func (fakeSnapshotProvider) MetricsSnapshot(context.Context, time.Time) (Snapshot, error) {
	return Snapshot{
		QueueBacklog: 7,
		RunningTasks: 2,
	}, nil
}

func TestRegistryWritesPrometheusMetrics(t *testing.T) {
	registry := NewRegistry().WithSnapshotProvider(fakeSnapshotProvider{})
	registry.TaskSubmitted()
	registry.TaskStarted(2 * time.Second)
	registry.TaskSucceeded(1500 * time.Millisecond)
	registry.TaskFailed(500 * time.Millisecond)
	registry.TaskRetried()
	registry.TaskDead()

	var body bytes.Buffer
	if err := registry.WritePrometheus(context.Background(), &body); err != nil {
		t.Fatalf("write prometheus metrics: %v", err)
	}

	output := body.String()
	expectedLines := []string{
		"gotaskqueue_tasks_submitted_total 1",
		"gotaskqueue_tasks_started_total 1",
		"gotaskqueue_tasks_succeeded_total 1",
		"gotaskqueue_tasks_failed_total 1",
		"gotaskqueue_tasks_retried_total 1",
		"gotaskqueue_tasks_dead_total 1",
		"gotaskqueue_queue_backlog 7",
		"gotaskqueue_worker_running_tasks 2",
		"gotaskqueue_task_wait_duration_seconds_sum 2",
		"gotaskqueue_task_wait_duration_seconds_count 1",
		"gotaskqueue_task_execution_duration_seconds_sum 2",
		"gotaskqueue_task_execution_duration_seconds_count 2",
	}
	for _, expected := range expectedLines {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected metrics output to contain %q, got:\n%s", expected, output)
		}
	}
}
