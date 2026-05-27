package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/bird0711/GoTaskQueue/internal/task"
)

type Handler interface {
	Handle(context.Context, *task.Task) error
}

type HandlerFunc func(context.Context, *task.Task) error

func (f HandlerFunc) Handle(ctx context.Context, task *task.Task) error {
	return f(ctx, task)
}

type UnknownTaskTypeError struct {
	TaskType string
}

func (e UnknownTaskTypeError) Error() string {
	return fmt.Sprintf("no handler registered for task_type %q", e.TaskType)
}

type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]Handler),
	}
}

func (r *HandlerRegistry) Register(taskType string, handler Handler) error {
	normalizedTaskType := strings.TrimSpace(taskType)
	if normalizedTaskType == "" {
		return errors.New("task_type is required")
	}
	if handler == nil {
		return fmt.Errorf("handler for task_type %q is nil", normalizedTaskType)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[normalizedTaskType]; exists {
		return fmt.Errorf("handler already registered for task_type %q", normalizedTaskType)
	}

	r.handlers[normalizedTaskType] = handler
	return nil
}

func (r *HandlerRegistry) Handle(ctx context.Context, taskModel *task.Task) error {
	if taskModel == nil {
		return errors.New("task is nil")
	}

	taskType := strings.TrimSpace(taskModel.Type)

	r.mu.RLock()
	handler, ok := r.handlers[taskType]
	r.mu.RUnlock()
	if !ok {
		return UnknownTaskTypeError{TaskType: taskModel.Type}
	}

	return handler.Handle(ctx, taskModel)
}
