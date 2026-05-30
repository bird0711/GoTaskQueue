package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/bird0711/GoTaskQueue/internal/config"
	"github.com/bird0711/GoTaskQueue/internal/dependencies"
	"github.com/bird0711/GoTaskQueue/internal/httpserver"
	"github.com/bird0711/GoTaskQueue/internal/metrics"
	"github.com/bird0711/GoTaskQueue/internal/queue"
	"github.com/bird0711/GoTaskQueue/internal/scheduler"
	"github.com/bird0711/GoTaskQueue/internal/task"
	"github.com/bird0711/GoTaskQueue/internal/worker"
)

const shutdownTimeout = 10 * time.Second

// App wires top-level components without attaching infrastructure yet.
type App struct {
	config config.Config
	logger *slog.Logger
	server *httpserver.Server
}

func New(cfg config.Config, logger *slog.Logger) *App {
	return &App{
		config: cfg,
		logger: logger,
	}
}

func (a *App) Run(ctx context.Context) error {
	deps, err := dependencies.Connect(ctx, a.config, a.logger)
	if err != nil {
		return err
	}
	defer deps.Close(a.logger)

	taskStore := task.NewStore(deps.Postgres)
	metricsRegistry := metrics.NewRegistry().WithSnapshotProvider(taskStore)
	streamPublisher := queue.NewRedisStreamPublisher(deps.Redis, a.config.Redis.StreamName)
	scheduledQueue := queue.NewRedisScheduledQueue(deps.Redis, a.config.Redis.ScheduledSetKey)
	deadPublisher := queue.NewRedisDeadStreamPublisher(deps.Redis, a.config.Redis.DeadStreamName)
	taskService := task.NewService(
		taskStore,
		streamPublisher,
		scheduledQueue,
	).WithMetrics(metricsRegistry)
	a.server = httpserver.New(httpserver.Config{
		Addr: a.config.HTTP.Addr,
	}, taskService, metricsRegistry)
	streamConsumer := queue.NewRedisStreamConsumer(
		deps.Redis,
		a.config.Redis.StreamName,
		a.config.Redis.ConsumerGroup,
		a.config.Redis.ConsumerName,
		5*time.Second,
	)
	handlerRegistry := worker.NewHandlerRegistry()
	if err := handlerRegistry.Register("demo.echo", worker.DemoEchoHandler{Logger: a.logger}); err != nil {
		return err
	}
	if err := handlerRegistry.Register("webhook.deliver", worker.WebhookDeliverHandler{}); err != nil {
		return err
	}
	workerRunner := worker.New(streamConsumer, taskStore, a.logger, a.config.Redis.ConsumerName).
		WithHandlerRegistry(handlerRegistry).
		WithConcurrency(a.config.Worker.Concurrency).
		WithBatchSize(a.config.Worker.BatchSize).
		WithMetrics(metricsRegistry).
		WithDeadPublisher(deadPublisher)
	schedulerRunner := scheduler.New(
		taskStore,
		streamPublisher,
		scheduledQueue,
		a.logger,
		time.Duration(a.config.Scheduler.IntervalSeconds)*time.Second,
		a.config.Scheduler.BatchSize,
	).WithDeadPublisher(deadPublisher)

	runCtx, cancelRunners := context.WithCancel(ctx)
	defer cancelRunners()

	serverErr := make(chan error, 1)
	go func() {
		a.logger.Info("http server starting", "addr", a.config.HTTP.Addr)
		serverErr <- a.server.Run()
	}()
	workerErr := make(chan error, 1)
	go func() {
		workerErr <- workerRunner.Run(runCtx)
	}()
	schedulerErr := make(chan error, 1)
	go func() {
		schedulerErr <- schedulerRunner.Run(runCtx)
	}()

	var earlyErr error
	serverDone, workerDone, schedulerDone := false, false, false

	select {
	case <-ctx.Done():
		a.logger.Info("shutdown requested")
	case err := <-serverErr:
		serverDone = true
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			earlyErr = err
		}
		a.logger.Info("server runner exited", "error", err)
	case err := <-workerErr:
		workerDone = true
		if err != nil {
			earlyErr = err
		}
		a.logger.Info("worker runner exited", "error", err)
	case err := <-schedulerErr:
		schedulerDone = true
		if err != nil {
			earlyErr = err
		}
		a.logger.Info("scheduler runner exited", "error", err)
	}

	cancelRunners()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()

	if err := a.server.Shutdown(shutdownCtx); err != nil {
		a.logger.Warn("server shutdown returned error", "error", err)
	}

	if !serverDone {
		waitForRunner(shutdownCtx, "server", serverErr, a.logger)
	}
	if !workerDone {
		waitForRunner(shutdownCtx, "worker", workerErr, a.logger)
	}
	if !schedulerDone {
		waitForRunner(shutdownCtx, "scheduler", schedulerErr, a.logger)
	}

	a.logger.Info("application shutdown complete")
	return earlyErr
}

func waitForRunner(ctx context.Context, name string, errCh <-chan error, logger *slog.Logger) {
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("runner exited with error during shutdown", "runner", name, "error", err)
			return
		}
		logger.Info("runner exited", "runner", name)
	case <-ctx.Done():
		logger.Warn("timeout waiting for runner to exit", "runner", name)
	}
}
