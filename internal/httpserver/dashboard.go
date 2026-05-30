package httpserver

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/task"
)

const recentDashboardLimit = 5
const recentTaskListLimit = 10

//go:embed templates/*.html.tmpl
var dashboardTemplatesFS embed.FS

var dashboardTemplateFuncs = template.FuncMap{
	"formatTime":      formatDashboardTime,
	"lastError":       dashboardLastError,
	"taskDetailPath":  dashboardTaskDetailPath,
	"taskStatusClass": dashboardTaskStatusClass,
}

var dashboardDetailTemplateFuncs = template.FuncMap{
	"formatTime":         formatDashboardTime,
	"formatOptionalTime": formatDashboardOptionalTime,
	"lastError":          dashboardLastError,
	"stringValue":        dashboardStringValue,
	"taskStatusClass":    dashboardTaskStatusClass,
	"prettyJSON":         dashboardPrettyJSON,
}

var dashboardTemplate = template.Must(
	template.New("dashboard.html.tmpl").
		Funcs(dashboardTemplateFuncs).
		ParseFS(dashboardTemplatesFS, "templates/dashboard.html.tmpl"),
)

var dashboardTaskDetailTemplate = template.Must(
	template.New("dashboard_task_detail.html.tmpl").
		Funcs(dashboardDetailTemplateFuncs).
		ParseFS(dashboardTemplatesFS, "templates/dashboard_task_detail.html.tmpl"),
)

type dashboardView struct {
	GeneratedAt  time.Time
	TotalTasks   int64
	StatusCounts []statusCountView
	Snapshot     dashboardSnapshot
	RecentTasks  []*task.Task
	RecentFailed []*task.Task
	RecentDead   []*task.Task
}

type dashboardTaskDetailView struct {
	Task *task.Task
}

type statusCountView struct {
	Status task.Status
	Count  int64
}

type dashboardSnapshot struct {
	QueueBacklog int64
	RunningTasks int64
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	counts, err := s.tasks.CountByStatus(r.Context())
	if err != nil {
		http.Error(w, "load dashboard status counts failed", http.StatusInternalServerError)
		return
	}
	snapshot, err := s.tasks.MetricsSnapshot(r.Context(), now)
	if err != nil {
		http.Error(w, "load dashboard metrics snapshot failed", http.StatusInternalServerError)
		return
	}
	recentFailed, err := s.tasks.RecentByStatus(r.Context(), task.StatusFailed, recentDashboardLimit)
	if err != nil {
		http.Error(w, "load recent failed tasks failed", http.StatusInternalServerError)
		return
	}
	recentDead, err := s.tasks.RecentByStatus(r.Context(), task.StatusDead, recentDashboardLimit)
	if err != nil {
		http.Error(w, "load recent dead tasks failed", http.StatusInternalServerError)
		return
	}
	recentTasks, err := s.tasks.Recent(r.Context(), recentTaskListLimit)
	if err != nil {
		http.Error(w, "load recent tasks failed", http.StatusInternalServerError)
		return
	}

	view := dashboardView{
		GeneratedAt:  now,
		StatusCounts: dashboardStatusCounts(counts),
		Snapshot: dashboardSnapshot{
			QueueBacklog: snapshot.QueueBacklog,
			RunningTasks: snapshot.RunningTasks,
		},
		RecentTasks:  recentTasks,
		RecentFailed: recentFailed,
		RecentDead:   recentDead,
	}
	for _, count := range view.StatusCounts {
		view.TotalTasks += count.Count
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTemplate.Execute(w, view); err != nil {
		http.Error(w, "render dashboard failed", http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleDashboardTaskDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		http.Error(w, "task id is required", http.StatusBadRequest)
		return
	}

	found, err := s.tasks.Get(r.Context(), id)
	if errors.Is(err, task.ErrNotFound) {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "load task detail failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTaskDetailTemplate.Execute(w, dashboardTaskDetailView{Task: found}); err != nil {
		http.Error(w, "render task detail failed", http.StatusInternalServerError)
		return
	}
}

func dashboardStatusCounts(counts map[task.Status]int64) []statusCountView {
	statuses := []task.Status{
		task.StatusScheduled,
		task.StatusPending,
		task.StatusRunning,
		task.StatusSuccess,
		task.StatusFailed,
		task.StatusRetrying,
		task.StatusDead,
	}
	result := make([]statusCountView, 0, len(statuses))
	for _, status := range statuses {
		result = append(result, statusCountView{
			Status: status,
			Count:  counts[status],
		})
	}
	return result
}

func formatDashboardTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format("2006-01-02 15:04:05 UTC")
}

func formatDashboardOptionalTime(value *time.Time) string {
	if value == nil {
		return "-"
	}
	return formatDashboardTime(*value)
}

func dashboardLastError(value *string) string {
	if value == nil || *value == "" {
		return "-"
	}
	return *value
}

func dashboardTaskDetailPath(id string) string {
	return "/dashboard/tasks/" + id
}

func dashboardTaskStatusClass(status task.Status) string {
	return string(status)
}

func dashboardStringValue(value *string) string {
	if value == nil || *value == "" {
		return "-"
	}
	return *value
}

func dashboardPrettyJSON(payload []byte) string {
	if len(payload) == 0 {
		return "{}"
	}

	var formatted bytes.Buffer
	if err := json.Indent(&formatted, payload, "", "  "); err != nil {
		return string(payload)
	}
	return formatted.String()
}
