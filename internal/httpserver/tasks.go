package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/metrics"
	"github.com/bird0711/GoTaskQueue/internal/task"
)

type TaskStore interface {
	Create(context.Context, task.CreateTaskParams) (*task.Task, error)
	Get(context.Context, string) (*task.Task, error)
	MetricsSnapshot(context.Context, time.Time) (metrics.Snapshot, error)
	CountByStatus(context.Context) (map[task.Status]int64, error)
	RecentByStatus(context.Context, task.Status, int) ([]*task.Task, error)
	Recent(context.Context, int) ([]*task.Task, error)
}

type createTaskRequest struct {
	TaskType       string          `json:"task_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key"`
	TraceID        *string         `json:"trace_id"`
	RunAt          *time.Time      `json:"run_at"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	MaxRetries     int             `json:"max_retries"`
}

type taskResponse struct {
	ID             string          `json:"id"`
	TaskType       string          `json:"task_type"`
	Payload        json.RawMessage `json:"payload"`
	Status         task.Status     `json:"status"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	TraceID        *string         `json:"trace_id,omitempty"`
	RunAt          time.Time       `json:"run_at"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	MaxRetries     int             `json:"max_retries"`
	RetryCount     int             `json:"retry_count"`
	NextRetryAt    *time.Time      `json:"next_retry_at,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	WorkerID       *string         `json:"worker_id,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	params, err := validateCreateTaskRequest(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	created, err := s.tasks.Create(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database write failed")
		return
	}

	writeJSON(w, http.StatusCreated, toTaskResponse(created))
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "task id is required")
		return
	}

	found, err := s.tasks.Get(r.Context(), id)
	if errors.Is(err, task.ErrNotFound) {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database read failed")
		return
	}

	writeJSON(w, http.StatusOK, toTaskResponse(found))
}

func validateCreateTaskRequest(req createTaskRequest) (task.CreateTaskParams, error) {
	taskType := strings.TrimSpace(req.TaskType)
	if taskType == "" {
		return task.CreateTaskParams{}, errors.New("task_type is required")
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return task.CreateTaskParams{}, errors.New("payload must be valid json")
	}

	if req.IdempotencyKey != nil {
		key := strings.TrimSpace(*req.IdempotencyKey)
		if key == "" {
			return task.CreateTaskParams{}, errors.New("idempotency_key cannot be blank")
		}
		req.IdempotencyKey = &key
	}

	if req.TraceID != nil {
		traceID := strings.TrimSpace(*req.TraceID)
		if traceID == "" {
			return task.CreateTaskParams{}, errors.New("trace_id cannot be blank")
		}
		req.TraceID = &traceID
	}

	runAt := time.Now().UTC()
	if req.RunAt != nil {
		runAt = req.RunAt.UTC()
	}

	timeoutSeconds := req.TimeoutSeconds
	if timeoutSeconds == 0 {
		timeoutSeconds = 300
	}
	if timeoutSeconds < 0 {
		return task.CreateTaskParams{}, errors.New("timeout_seconds must be positive")
	}

	maxRetries := req.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}
	if maxRetries < 0 {
		return task.CreateTaskParams{}, errors.New("max_retries cannot be negative")
	}

	return task.CreateTaskParams{
		Type:           taskType,
		Payload:        payload,
		IdempotencyKey: req.IdempotencyKey,
		TraceID:        req.TraceID,
		RunAt:          runAt,
		TimeoutSeconds: timeoutSeconds,
		MaxRetries:     maxRetries,
	}, nil
}

func toTaskResponse(task *task.Task) taskResponse {
	return taskResponse{
		ID:             task.ID,
		TaskType:       task.Type,
		Payload:        task.Payload,
		Status:         task.Status,
		IdempotencyKey: task.IdempotencyKey,
		TraceID:        task.TraceID,
		RunAt:          task.RunAt,
		TimeoutSeconds: task.TimeoutSeconds,
		MaxRetries:     task.MaxRetries,
		RetryCount:     task.RetryCount,
		NextRetryAt:    task.NextRetryAt,
		LastError:      task.LastError,
		WorkerID:       task.WorkerID,
		StartedAt:      task.StartedAt,
		FinishedAt:     task.FinishedAt,
		CreatedAt:      task.CreatedAt,
		UpdatedAt:      task.UpdatedAt,
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
