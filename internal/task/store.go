package task

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bird0711/GoTaskQueue/internal/metrics"
)

var ErrNotFound = errors.New("task not found")
var ErrInvalidTransition = errors.New("invalid task status transition")

type CreateTaskParams struct {
	Type           string
	Payload        json.RawMessage
	IdempotencyKey *string
	RunAt          time.Time
	TimeoutSeconds int
	MaxRetries     int
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) Create(ctx context.Context, params CreateTaskParams) (*Task, error) {
	status := StatusPending
	if params.RunAt.After(time.Now().UTC()) {
		status = StatusScheduled
	}

	task := &Task{
		ID:             newID(),
		Type:           params.Type,
		Payload:        params.Payload,
		Status:         status,
		IdempotencyKey: params.IdempotencyKey,
		RunAt:          params.RunAt,
		TimeoutSeconds: params.TimeoutSeconds,
		MaxRetries:     params.MaxRetries,
	}

	row := s.db.QueryRow(ctx, `
		INSERT INTO tasks (
			id,
			task_type,
			payload,
			status,
			idempotency_key,
			run_at,
			timeout_seconds,
			max_retries
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING
			id,
			task_type,
			payload,
			status,
			idempotency_key,
			run_at,
			timeout_seconds,
			max_retries,
			retry_count,
			next_retry_at,
			last_error,
			worker_id,
			started_at,
			finished_at,
			created_at,
			updated_at
	`, task.ID, task.Type, task.Payload, task.Status, task.IdempotencyKey, task.RunAt, task.TimeoutSeconds, task.MaxRetries)

	return scanTask(row)
}

func (s *Store) Get(ctx context.Context, id string) (*Task, error) {
	row := s.db.QueryRow(ctx, `
		SELECT
			id,
			task_type,
			payload,
			status,
			idempotency_key,
			run_at,
			timeout_seconds,
			max_retries,
			retry_count,
			next_retry_at,
			last_error,
			worker_id,
			started_at,
			finished_at,
			created_at,
			updated_at
		FROM tasks
		WHERE id = $1
	`, id)

	task, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return task, err
}

func (s *Store) DueDispatchable(ctx context.Context, now time.Time, limit int) ([]*Task, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id,
			task_type,
			payload,
			status,
			idempotency_key,
			run_at,
			timeout_seconds,
			max_retries,
			retry_count,
			next_retry_at,
			last_error,
			worker_id,
			started_at,
			finished_at,
			created_at,
			updated_at
		FROM tasks
		WHERE (status = 'scheduled' AND run_at <= $1)
			OR (status = 'retrying' AND next_retry_at IS NOT NULL AND next_retry_at <= $1)
		ORDER BY
			CASE
				WHEN status = 'retrying' THEN next_retry_at
				ELSE run_at
			END ASC,
			created_at ASC
		LIMIT $2
	`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}

func (s *Store) DueScheduled(ctx context.Context, now time.Time, limit int) ([]*Task, error) {
	return s.DueDispatchable(ctx, now, limit)
}

func (s *Store) MetricsSnapshot(ctx context.Context, now time.Time) (metrics.Snapshot, error) {
	row := s.db.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (
				WHERE status = 'pending'
					OR (status = 'scheduled' AND run_at <= $1)
					OR (status = 'retrying' AND next_retry_at IS NOT NULL AND next_retry_at <= $1)
			) AS queue_backlog,
			COUNT(*) FILTER (WHERE status = 'running') AS running_tasks
		FROM tasks
	`, now)

	var snapshot metrics.Snapshot
	if err := row.Scan(&snapshot.QueueBacklog, &snapshot.RunningTasks); err != nil {
		return metrics.Snapshot{}, err
	}
	return snapshot, nil
}

func (s *Store) CountByStatus(ctx context.Context) (map[Status]int64, error) {
	rows, err := s.db.Query(ctx, `
		SELECT status, COUNT(*)
		FROM tasks
		GROUP BY status
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[Status]int64)
	for rows.Next() {
		var status Status
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return counts, nil
}

func (s *Store) RecentByStatus(ctx context.Context, status Status, limit int) ([]*Task, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id,
			task_type,
			payload,
			status,
			idempotency_key,
			run_at,
			timeout_seconds,
			max_retries,
			retry_count,
			next_retry_at,
			last_error,
			worker_id,
			started_at,
			finished_at,
			created_at,
			updated_at
		FROM tasks
		WHERE status = $1
		ORDER BY updated_at DESC, created_at DESC
		LIMIT $2
	`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}

func (s *Store) Transition(ctx context.Context, id string, from Status, to Status, workerID *string) (*Task, error) {
	if !CanTransition(from, to) {
		return nil, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks
		SET
			status = $3,
			worker_id = CASE WHEN $3 = 'running' THEN $4 ELSE worker_id END,
			started_at = CASE WHEN $3 = 'running' THEN NOW() ELSE started_at END,
			next_retry_at = CASE WHEN $3 = 'pending' THEN NULL ELSE next_retry_at END,
			finished_at = CASE WHEN $3 IN ('success', 'dead') THEN NOW() ELSE finished_at END,
			updated_at = NOW()
		WHERE id = $1
			AND status = $2
		RETURNING
			id,
			task_type,
			payload,
			status,
			idempotency_key,
			run_at,
			timeout_seconds,
			max_retries,
			retry_count,
			next_retry_at,
			last_error,
			worker_id,
			started_at,
			finished_at,
			created_at,
			updated_at
	`, id, from, to, workerID)

	task, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return task, err
}

func (s *Store) MarkFailed(ctx context.Context, id string, failure string) (*Task, error) {
	row := s.db.QueryRow(ctx, `
		UPDATE tasks
		SET
			status = 'failed',
			last_error = $2,
			updated_at = NOW()
		WHERE id = $1
			AND status = 'running'
		RETURNING
			id,
			task_type,
			payload,
			status,
			idempotency_key,
			run_at,
			timeout_seconds,
			max_retries,
			retry_count,
			next_retry_at,
			last_error,
			worker_id,
			started_at,
			finished_at,
			created_at,
			updated_at
	`, id, failure)

	task, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return task, err
}

func (s *Store) CompleteFailure(ctx context.Context, id string, decision FailureDecision, failure string) (*Task, error) {
	if decision.Status != StatusRetrying && decision.Status != StatusDead {
		return nil, fmt.Errorf("%w: failed -> %s", ErrInvalidTransition, decision.Status)
	}

	row := s.db.QueryRow(ctx, `
		UPDATE tasks
		SET
			status = $2,
			retry_count = $3,
			next_retry_at = $4,
			last_error = $5,
			finished_at = CASE WHEN $2 = 'dead' THEN NOW() ELSE finished_at END,
			updated_at = NOW()
		WHERE id = $1
			AND status = 'failed'
		RETURNING
			id,
			task_type,
			payload,
			status,
			idempotency_key,
			run_at,
			timeout_seconds,
			max_retries,
			retry_count,
			next_retry_at,
			last_error,
			worker_id,
			started_at,
			finished_at,
			created_at,
			updated_at
	`, id, decision.Status, decision.RetryCount, decision.NextRetryAt, failure)

	task, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return task, err
}

type taskRow interface {
	Scan(dest ...any) error
}

func scanTask(row taskRow) (*Task, error) {
	var task Task
	if err := row.Scan(
		&task.ID,
		&task.Type,
		&task.Payload,
		&task.Status,
		&task.IdempotencyKey,
		&task.RunAt,
		&task.TimeoutSeconds,
		&task.MaxRetries,
		&task.RetryCount,
		&task.NextRetryAt,
		&task.LastError,
		&task.WorkerID,
		&task.StartedAt,
		&task.FinishedAt,
		&task.CreatedAt,
		&task.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &task, nil
}

func newID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(fmt.Sprintf("generate task id: %v", err))
	}

	return "task_" + hex.EncodeToString(bytes[:])
}
