package httpserver

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/task"
)

func TestValidateCreateTaskRequestDefaultsToImmediateTask(t *testing.T) {
	params, err := validateCreateTaskRequest(createTaskRequest{
		TaskType: "email.send",
		Payload:  json.RawMessage(`{"to":"user@example.com"}`),
	})
	if err != nil {
		t.Fatalf("validate request: %v", err)
	}

	if params.Type != "email.send" {
		t.Fatalf("expected task type email.send, got %q", params.Type)
	}
	if params.TimeoutSeconds != 300 {
		t.Fatalf("expected default timeout 300, got %d", params.TimeoutSeconds)
	}
	if params.MaxRetries != 3 {
		t.Fatalf("expected default max retries 3, got %d", params.MaxRetries)
	}
	if params.RunAt.After(time.Now().UTC().Add(1 * time.Second)) {
		t.Fatalf("expected immediate run_at, got %s", params.RunAt)
	}
}

func TestValidateCreateTaskRequestRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		req  createTaskRequest
	}{
		{
			name: "blank task type",
			req:  createTaskRequest{TaskType: ""},
		},
		{
			name: "invalid payload",
			req: createTaskRequest{
				TaskType: "email.send",
				Payload:  json.RawMessage(`{`),
			},
		},
		{
			name: "blank idempotency key",
			req: createTaskRequest{
				TaskType:       "email.send",
				IdempotencyKey: ptr(" "),
			},
		},
		{
			name: "negative timeout",
			req: createTaskRequest{
				TaskType:       "email.send",
				TimeoutSeconds: -1,
			},
		},
		{
			name: "negative max retries",
			req: createTaskRequest{
				TaskType:   "email.send",
				MaxRetries: -1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := validateCreateTaskRequest(tt.req); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestToTaskResponse(t *testing.T) {
	now := time.Now().UTC()
	model := &task.Task{
		ID:             "task_123",
		Type:           "email.send",
		Payload:        json.RawMessage(`{"ok":true}`),
		Status:         task.StatusPending,
		RunAt:          now,
		TimeoutSeconds: 30,
		MaxRetries:     2,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	response := toTaskResponse(model)
	if response.ID != model.ID {
		t.Fatalf("expected id %q, got %q", model.ID, response.ID)
	}
	if response.Status != task.StatusPending {
		t.Fatalf("expected pending status, got %q", response.Status)
	}
}

func ptr(value string) *string {
	return &value
}
