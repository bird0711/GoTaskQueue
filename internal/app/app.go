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
	taskService := task.NewService(
		taskStore,
		streamPublisher,
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
	workerRunner := worker.New(streamConsumer, taskStore, a.logger, a.config.Redis.ConsumerName).WithMetrics(metricsRegistry)
	schedulerRunner := scheduler.New(
		taskStore,
		streamPublisher,
		a.logger,
		time.Duration(a.config.Scheduler.IntervalSeconds)*time.Second,
		a.config.Scheduler.BatchSize,
	)

	serverErr := make(chan error, 1)
	go func() {
		a.logger.Info("http server starting", "addr", a.config.HTTP.Addr)
		serverErr <- a.server.Run()
	}()
	workerErr := make(chan error, 1)
	go func() {
		workerErr <- workerRunner.Run(ctx)
	}()
	schedulerErr := make(chan error, 1)
	go func() {
		schedulerErr <- schedulerRunner.Run(ctx)
	}()

	select {
	case <-ctx.Done():
		a.logger.Info("shutdown requested")
	case err := <-serverErr:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case err := <-workerErr:
		if err != nil {
			return err
		}
	case err := <-schedulerErr:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := a.server.Shutdown(shutdownCtx); err != nil {
		return err
	}

	err = <-serverErr
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	a.logger.Info("application shutdown complete")
	return nil
}
