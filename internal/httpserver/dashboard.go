package httpserver

import (
	"html/template"
	"net/http"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/task"
)

const recentDashboardLimit = 5

var dashboardTemplate = template.Must(template.New("dashboard").Funcs(template.FuncMap{
	"formatTime": formatDashboardTime,
	"lastError":  dashboardLastError,
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
	table { display: block; overflow-x: auto; white-space: nowrap; }
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
</main>
</body>
</html>
{{ define "taskTable" }}
{{ if . }}
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
			<td><code>{{ .ID }}</code></td>
			<td>{{ .Type }}</td>
			<td>{{ .RetryCount }} / {{ .MaxRetries }}</td>
			<td>{{ formatTime .UpdatedAt }}</td>
			<td class="error">{{ lastError .LastError }}</td>
		</tr>
		{{ end }}
	</tbody>
</table>
{{ else }}
<div class="empty">No tasks found.</div>
{{ end }}
{{ end }}`))

type dashboardView struct {
	GeneratedAt  time.Time
	TotalTasks   int64
	StatusCounts []statusCountView
	Snapshot     dashboardSnapshot
	RecentFailed []*task.Task
	RecentDead   []*task.Task
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

	view := dashboardView{
		GeneratedAt:  now,
		StatusCounts: dashboardStatusCounts(counts),
		Snapshot: dashboardSnapshot{
			QueueBacklog: snapshot.QueueBacklog,
			RunningTasks: snapshot.RunningTasks,
		},
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

func dashboardLastError(value *string) string {
	if value == nil || *value == "" {
		return "-"
	}
	return *value
}
