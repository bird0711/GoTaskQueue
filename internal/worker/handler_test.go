package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/bird0711/GoTaskQueue/internal/task"
)

func TestHandlerRegistryHandleRoutesByTaskType(t *testing.T) {
	registry := NewHandlerRegistry()

	called := false
	if err := registry.Register("demo.echo", HandlerFunc(func(_ context.Context, taskModel *task.Task) error {
		called = true
		if taskModel.Type != "demo.echo" {
			t.Fatalf("expected task type demo.echo, got %q", taskModel.Type)
		}
		return nil
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	err := registry.Handle(context.Background(), &task.Task{Type: "demo.echo"})
	if err != nil {
		t.Fatalf("handle task: %v", err)
	}
	if !called {
		t.Fatal("expected registered handler to be called")
	}
}

func TestHandlerRegistryHandleReturnsUnknownTaskTypeError(t *testing.T) {
	registry := NewHandlerRegistry()

	err := registry.Handle(context.Background(), &task.Task{Type: "missing.type"})
	var unknownErr UnknownTaskTypeError
	if !errors.As(err, &unknownErr) {
		t.Fatalf("expected unknown task type error, got %v", err)
	}
	if unknownErr.TaskType != "missing.type" {
		t.Fatalf("expected task type missing.type, got %q", unknownErr.TaskType)
	}
	if !IsNonRetryable(err) {
		t.Fatalf("expected unknown task type to be non-retryable, got %T", err)
	}
}

func TestHandlerRegistryRegisterRejectsDuplicateTaskType(t *testing.T) {
	registry := NewHandlerRegistry()

	if err := registry.Register("demo.echo", HandlerFunc(func(context.Context, *task.Task) error { return nil })); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	err := registry.Register("demo.echo", HandlerFunc(func(context.Context, *task.Task) error { return nil }))
	if err == nil {
		t.Fatal("expected duplicate registration error")
	}
}
