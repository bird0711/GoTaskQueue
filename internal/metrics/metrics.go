package metrics

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

type Snapshot struct {
	QueueBacklog int64
	RunningTasks int64
}

type SnapshotProvider interface {
	MetricsSnapshot(context.Context, time.Time) (Snapshot, error)
}

type Registry struct {
	mu        sync.Mutex
	counters  map[string]float64
	gauges    map[string]float64
	summaries map[string]summary
	provider  SnapshotProvider
}

type summary struct {
	count float64
	sum   float64
}

func NewRegistry() *Registry {
	return &Registry{
		counters: map[string]float64{
			"gotaskqueue_tasks_submitted_total": 0,
			"gotaskqueue_tasks_started_total":   0,
			"gotaskqueue_tasks_succeeded_total": 0,
			"gotaskqueue_tasks_failed_total":    0,
			"gotaskqueue_tasks_retried_total":   0,
			"gotaskqueue_tasks_dead_total":      0,
		},
		gauges: map[string]float64{
			"gotaskqueue_queue_backlog":        0,
			"gotaskqueue_worker_running_tasks": 0,
		},
		summaries: map[string]summary{
			"gotaskqueue_task_execution_duration_seconds": {},
			"gotaskqueue_task_wait_duration_seconds":      {},
		},
	}
}

func (r *Registry) WithSnapshotProvider(provider SnapshotProvider) *Registry {
	r.provider = provider
	return r
}

func (r *Registry) TaskSubmitted() {
	r.addCounter("gotaskqueue_tasks_submitted_total", 1)
}

func (r *Registry) TaskStarted(wait time.Duration) {
	r.addCounter("gotaskqueue_tasks_started_total", 1)
	r.observe("gotaskqueue_task_wait_duration_seconds", wait)
}

func (r *Registry) TaskSucceeded(execution time.Duration) {
	r.addCounter("gotaskqueue_tasks_succeeded_total", 1)
	r.observe("gotaskqueue_task_execution_duration_seconds", execution)
}

func (r *Registry) TaskFailed(execution time.Duration) {
	r.addCounter("gotaskqueue_tasks_failed_total", 1)
	r.observe("gotaskqueue_task_execution_duration_seconds", execution)
}

func (r *Registry) TaskRetried() {
	r.addCounter("gotaskqueue_tasks_retried_total", 1)
}

func (r *Registry) TaskDead() {
	r.addCounter("gotaskqueue_tasks_dead_total", 1)
}

func (r *Registry) WritePrometheus(ctx context.Context, w io.Writer) error {
	if r.provider != nil {
		snapshot, err := r.provider.MetricsSnapshot(ctx, time.Now().UTC())
		if err != nil {
			return err
		}
		r.setGauge("gotaskqueue_queue_backlog", float64(snapshot.QueueBacklog))
		r.setGauge("gotaskqueue_worker_running_tasks", float64(snapshot.RunningTasks))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, name := range sortedKeys(r.counters) {
		if err := writeMetric(w, name, "counter", r.counters[name]); err != nil {
			return err
		}
	}
	for _, name := range sortedKeys(r.gauges) {
		if err := writeMetric(w, name, "gauge", r.gauges[name]); err != nil {
			return err
		}
	}
	for _, name := range sortedSummaryKeys(r.summaries) {
		value := r.summaries[name]
		if _, err := fmt.Fprintf(w, "# TYPE %s summary\n%s_sum %g\n%s_count %g\n", name, name, value.sum, name, value.count); err != nil {
			return err
		}
	}

	return nil
}

func (r *Registry) addCounter(name string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters[name] += value
}

func (r *Registry) setGauge(name string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = value
}

func (r *Registry) observe(name string, duration time.Duration) {
	if duration < 0 {
		duration = 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	value := r.summaries[name]
	value.count++
	value.sum += duration.Seconds()
	r.summaries[name] = value
}

func sortedKeys(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedSummaryKeys(values map[string]summary) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func writeMetric(w io.Writer, name, metricType string, value float64) error {
	_, err := fmt.Fprintf(w, "# TYPE %s %s\n%s %g\n", name, metricType, name, value)
	return err
}
