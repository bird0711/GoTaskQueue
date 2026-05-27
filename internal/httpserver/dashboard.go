package httpserver

import (
	"bytes"
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

var dashboardTemplate = template.Must(template.New("dashboard").Funcs(template.FuncMap{
	"formatTime":      formatDashboardTime,
	"lastError":       dashboardLastError,
	"taskDetailPath":  dashboardTaskDetailPath,
	"taskStatusClass": dashboardTaskStatusClass,
}).Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GoTaskQueue Dashboard</title>
<style>
:root {
	color-scheme: light;
	--bg: #f7f8fa;
	--panel: #ffffff;
	--text: #17202a;
	--muted: #5d6977;
	--line: #dfe4ea;
	--accent: #0f766e;
	--danger: #b42318;
	--warn: #b54708;
	--ink-soft: #eef6f5;
}
* { box-sizing: border-box; }
body {
	margin: 0;
	background: var(--bg);
	color: var(--text);
	font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
	font-size: 14px;
	line-height: 1.4;
}
main {
	max-width: 1180px;
	margin: 0 auto;
	padding: 28px 20px 40px;
}
header {
	display: flex;
	align-items: baseline;
	justify-content: space-between;
	gap: 16px;
	margin-bottom: 22px;
}
h1, h2 { margin: 0; letter-spacing: 0; }
h1 { font-size: 28px; font-weight: 700; }
h2 { font-size: 16px; font-weight: 700; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.muted { color: var(--muted); }
.overview {
	display: grid;
	grid-template-columns: repeat(3, minmax(0, 1fr));
	gap: 12px;
	margin-bottom: 18px;
}
.metric, .panel {
	background: var(--panel);
	border: 1px solid var(--line);
	border-radius: 8px;
}
.metric { padding: 16px; }
.metric .label {
	color: var(--muted);
	font-size: 12px;
	text-transform: uppercase;
	font-weight: 700;
}
.metric .value {
	font-size: 30px;
	font-weight: 750;
	margin-top: 6px;
}
.grid {
	display: grid;
	grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.1fr);
	gap: 16px;
	align-items: start;
}
.panel { padding: 16px; }
.status-list {
	display: grid;
	grid-template-columns: repeat(2, minmax(0, 1fr));
	gap: 8px;
	margin-top: 14px;
}
.status {
	display: flex;
	justify-content: space-between;
	gap: 12px;
	padding: 10px 12px;
	border: 1px solid var(--line);
	border-radius: 6px;
	background: #fbfcfd;
}
.status code {
	font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
}
table {
	width: 100%;
	border-collapse: collapse;
	margin-top: 12px;
}
th, td {
	text-align: left;
	border-bottom: 1px solid var(--line);
	padding: 10px 8px;
	vertical-align: top;
}
th {
	color: var(--muted);
	font-size: 12px;
	font-weight: 700;
	text-transform: uppercase;
}
td code {
	font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
	font-size: 12px;
	word-break: break-word;
}
.error {
	color: var(--danger);
	max-width: 360px;
	word-break: break-word;
}
.stack {
	display: grid;
	gap: 16px;
}
.table-wrap {
	margin-top: 12px;
	overflow-x: auto;
}
.task-link {
	font-weight: 700;
}
.status-pill {
	display: inline-flex;
	align-items: center;
	padding: 3px 8px;
	border-radius: 999px;
	font-size: 12px;
	font-weight: 700;
	border: 1px solid var(--line);
	background: #fbfcfd;
}
.status-pill.success { color: #027a48; background: #ecfdf3; border-color: #abefc6; }
.status-pill.failed, .status-pill.dead { color: var(--danger); background: #fef3f2; border-color: #fecdca; }
.status-pill.retrying, .status-pill.scheduled { color: var(--warn); background: #fffaeb; border-color: #fedf89; }
.status-pill.running { color: #175cd3; background: #eff8ff; border-color: #b2ddff; }
.status-pill.pending { color: #344054; background: var(--ink-soft); border-color: #cfe7e4; }
.empty {
	margin-top: 12px;
	padding: 14px;
	color: var(--muted);
	background: #fbfcfd;
	border: 1px solid var(--line);
	border-radius: 6px;
}
@media (max-width: 820px) {
	header { display: block; }
	.overview, .grid, .status-list { grid-template-columns: 1fr; }
	main { padding: 22px 14px 32px; }
}
</style>
</head>
<body>
<main>
	<header>
		<div>
			<h1>GoTaskQueue</h1>
			<div class="muted">Read-only dashboard</div>
		</div>
		<div class="muted">Updated {{ formatTime .GeneratedAt }}</div>
	</header>

	<section class="overview" aria-label="Task overview">
		<div class="metric">
			<div class="label">Total tasks</div>
			<div class="value">{{ .TotalTasks }}</div>
		</div>
		<div class="metric">
			<div class="label">Queue backlog</div>
			<div class="value">{{ .Snapshot.QueueBacklog }}</div>
		</div>
		<div class="metric">
			<div class="label">Running tasks</div>
			<div class="value">{{ .Snapshot.RunningTasks }}</div>
		</div>
	</section>

	<section class="grid">
		<div class="panel">
			<h2>Task Status</h2>
			<div class="status-list">
				{{ range .StatusCounts }}
				<div class="status">
					<code>{{ .Status }}</code>
					<strong>{{ .Count }}</strong>
				</div>
				{{ end }}
			</div>
		</div>

		<div class="stack">
			<div class="panel">
				<h2>Recent Failed Tasks</h2>
				{{ template "taskTable" .RecentFailed }}
			</div>
			<div class="panel">
				<h2>Recent Dead Tasks</h2>
				{{ template "taskTable" .RecentDead }}
			</div>
		</div>
	</section>

	<section class="panel" style="margin-top: 16px;">
		<h2>Recent Tasks</h2>
		{{ template "recentTaskTable" .RecentTasks }}
	</section>
</main>
</body>
</html>
{{ define "taskTable" }}
{{ if . }}
<div class="table-wrap">
<table>
	<thead>
		<tr>
			<th>ID</th>
			<th>Type</th>
			<th>Retries</th>
			<th>Updated</th>
			<th>Error</th>
		</tr>
	</thead>
	<tbody>
		{{ range . }}
		<tr>
			<td><a class="task-link" href="{{ taskDetailPath .ID }}"><code>{{ .ID }}</code></a></td>
			<td>{{ .Type }}</td>
			<td>{{ .RetryCount }} / {{ .MaxRetries }}</td>
			<td>{{ formatTime .UpdatedAt }}</td>
			<td class="error">{{ lastError .LastError }}</td>
		</tr>
		{{ end }}
	</tbody>
</table>
</div>
{{ else }}
<div class="empty">No tasks found.</div>
{{ end }}
{{ end }}
{{ define "recentTaskTable" }}
{{ if . }}
<div class="table-wrap">
<table>
	<thead>
		<tr>
			<th>ID</th>
			<th>Status</th>
			<th>Type</th>
			<th>Run At</th>
			<th>Updated</th>
		</tr>
	</thead>
	<tbody>
		{{ range . }}
		<tr>
			<td><a class="task-link" href="{{ taskDetailPath .ID }}"><code>{{ .ID }}</code></a></td>
			<td><span class="status-pill {{ taskStatusClass .Status }}">{{ .Status }}</span></td>
			<td>{{ .Type }}</td>
			<td>{{ formatTime .RunAt }}</td>
			<td>{{ formatTime .UpdatedAt }}</td>
		</tr>
		{{ end }}
	</tbody>
</table>
</div>
{{ else }}
<div class="empty">No tasks found.</div>
{{ end }}
{{ end }}`))

var dashboardTaskDetailTemplate = template.Must(template.New("dashboard-task-detail").Funcs(template.FuncMap{
	"formatTime":         formatDashboardTime,
	"formatOptionalTime": formatDashboardOptionalTime,
	"lastError":          dashboardLastError,
	"stringValue":        dashboardStringValue,
	"taskStatusClass":    dashboardTaskStatusClass,
	"prettyJSON":         dashboardPrettyJSON,
}).Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Task {{ .Task.ID }} - GoTaskQueue</title>
<style>
:root {
	color-scheme: light;
	--bg: #f7f8fa;
	--panel: #ffffff;
	--text: #17202a;
	--muted: #5d6977;
	--line: #dfe4ea;
	--accent: #0f766e;
	--danger: #b42318;
	--warn: #b54708;
	--ink-soft: #eef6f5;
}
* { box-sizing: border-box; }
body {
	margin: 0;
	background: var(--bg);
	color: var(--text);
	font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
	font-size: 14px;
	line-height: 1.45;
}
main {
	max-width: 920px;
	margin: 0 auto;
	padding: 28px 20px 40px;
}
h1, h2 { margin: 0; }
h1 { font-size: 28px; font-weight: 700; }
h2 { font-size: 16px; font-weight: 700; margin-bottom: 12px; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.muted { color: var(--muted); }
.panel {
	background: var(--panel);
	border: 1px solid var(--line);
	border-radius: 8px;
	padding: 16px;
	margin-top: 16px;
}
.status-pill {
	display: inline-flex;
	align-items: center;
	padding: 4px 9px;
	border-radius: 999px;
	font-size: 12px;
	font-weight: 700;
	border: 1px solid var(--line);
	background: #fbfcfd;
}
.status-pill.success { color: #027a48; background: #ecfdf3; border-color: #abefc6; }
.status-pill.failed, .status-pill.dead { color: var(--danger); background: #fef3f2; border-color: #fecdca; }
.status-pill.retrying, .status-pill.scheduled { color: var(--warn); background: #fffaeb; border-color: #fedf89; }
.status-pill.running { color: #175cd3; background: #eff8ff; border-color: #b2ddff; }
.status-pill.pending { color: #344054; background: var(--ink-soft); border-color: #cfe7e4; }
.meta {
	display: grid;
	grid-template-columns: repeat(2, minmax(0, 1fr));
	gap: 12px 20px;
}
.field-label {
	color: var(--muted);
	font-size: 12px;
	font-weight: 700;
	text-transform: uppercase;
	margin-bottom: 4px;
}
code, pre {
	font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
}
pre {
	margin: 0;
	padding: 14px;
	border-radius: 8px;
	border: 1px solid var(--line);
	background: #fbfcfd;
	overflow-x: auto;
	white-space: pre-wrap;
	word-break: break-word;
}
@media (max-width: 720px) {
	main { padding: 22px 14px 32px; }
	.meta { grid-template-columns: 1fr; }
}
</style>
</head>
<body>
<main>
	<div class="muted"><a href="/dashboard">Back to dashboard</a></div>
	<div style="margin-top: 12px;">
		<h1><code>{{ .Task.ID }}</code></h1>
		<div class="muted" style="margin-top: 8px;">
			<span class="status-pill {{ taskStatusClass .Task.Status }}">{{ .Task.Status }}</span>
			<span style="margin-left: 10px;">{{ .Task.Type }}</span>
		</div>
	</div>

	<section class="panel">
		<h2>Task Details</h2>
		<div class="meta">
			<div><div class="field-label">Task Type</div><div>{{ .Task.Type }}</div></div>
			<div><div class="field-label">Idempotency Key</div><div>{{ if .Task.IdempotencyKey }}<code>{{ stringValue .Task.IdempotencyKey }}</code>{{ else }}-{{ end }}</div></div>
			<div><div class="field-label">Retries</div><div>{{ .Task.RetryCount }} / {{ .Task.MaxRetries }}</div></div>
			<div><div class="field-label">Timeout Seconds</div><div>{{ .Task.TimeoutSeconds }}</div></div>
			<div><div class="field-label">Worker ID</div><div>{{ if .Task.WorkerID }}<code>{{ stringValue .Task.WorkerID }}</code>{{ else }}-{{ end }}</div></div>
			<div><div class="field-label">Last Error</div><div>{{ lastError .Task.LastError }}</div></div>
			<div><div class="field-label">Run At</div><div>{{ formatTime .Task.RunAt }}</div></div>
			<div><div class="field-label">Next Retry At</div><div>{{ formatOptionalTime .Task.NextRetryAt }}</div></div>
			<div><div class="field-label">Started At</div><div>{{ formatOptionalTime .Task.StartedAt }}</div></div>
			<div><div class="field-label">Finished At</div><div>{{ formatOptionalTime .Task.FinishedAt }}</div></div>
			<div><div class="field-label">Created At</div><div>{{ formatTime .Task.CreatedAt }}</div></div>
			<div><div class="field-label">Updated At</div><div>{{ formatTime .Task.UpdatedAt }}</div></div>
		</div>
	</section>

	<section class="panel">
		<h2>Payload</h2>
		<pre>{{ prettyJSON .Task.Payload }}</pre>
	</section>
</main>
</body>
</html>`))

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
