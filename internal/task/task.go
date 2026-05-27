package task

import (
	"encoding/json"
	"time"
)

type Task struct {
	ID             string
	Type           string
	Payload        json.RawMessage
	Status         Status
	NewlyCreated   bool
	IdempotencyKey *string
	TraceID        *string
	RunAt          time.Time
	TimeoutSeconds int
	MaxRetries     int
	RetryCount     int
	NextRetryAt    *time.Time
	LastError      *string
	WorkerID       *string
	StartedAt      *time.Time
	FinishedAt     *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
