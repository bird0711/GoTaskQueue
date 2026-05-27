package worker

import (
	"context"
	"log/slog"

	"github.com/bird0711/GoTaskQueue/internal/task"
)

type DemoEchoHandler struct {
	Logger *slog.Logger
}

func (h DemoEchoHandler) Handle(ctx context.Context, taskModel *task.Task) error {
	if h.Logger != nil {
		h.Logger.InfoContext(ctx, "demo echo task handled", "task_id", taskModel.ID, "task_type", taskModel.Type, "payload", string(taskModel.Payload))
	}
	return nil
}
