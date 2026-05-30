package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bird0711/GoTaskQueue/internal/app"
	"github.com/bird0711/GoTaskQueue/internal/config"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()
	logger.Info("configuration loaded", "http_addr", cfg.HTTP.Addr, "redis_addr", cfg.Redis.Addr, "redis_stream_name", cfg.Redis.StreamName, "redis_scheduled_set_key", cfg.Redis.ScheduledSetKey, "redis_consumer_group", cfg.Redis.ConsumerGroup, "redis_consumer_name", cfg.Redis.ConsumerName, "postgres_host", cfg.Postgres.Host, "postgres_port", cfg.Postgres.Port, "postgres_database", cfg.Postgres.Database, "prometheus_url", cfg.Prometheus.URL, "scheduler_interval_seconds", cfg.Scheduler.IntervalSeconds, "scheduler_batch_size", cfg.Scheduler.BatchSize, "worker_concurrency", cfg.Worker.Concurrency, "worker_batch_size", cfg.Worker.BatchSize)

	application := app.New(cfg, logger)
	if err := application.Run(ctx); err != nil {
		logger.Error("application stopped with error", "error", err)
		os.Exit(1)
	}
}
