//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/bird0711/GoTaskQueue/internal/config"
	"github.com/bird0711/GoTaskQueue/internal/dependencies"
	"github.com/bird0711/GoTaskQueue/internal/httpserver"
	"github.com/bird0711/GoTaskQueue/internal/metrics"
	"github.com/bird0711/GoTaskQueue/internal/queue"
	"github.com/bird0711/GoTaskQueue/internal/scheduler"
	"github.com/bird0711/GoTaskQueue/internal/task"
	"github.com/bird0711/GoTaskQueue/internal/worker"
)

type integrationEnv struct {
	baseURL string
	client  *http.Client
	redis   *redis.Client
	pg      *pgxpool.Pool
	store   *task.Store

	cancel     context.CancelFunc
	server     *httpserver.Server
	serverDone chan error
	workerDone chan error
	schedDone  chan error

	streamName string
	scheduled  string
	deadStream string
}

type createTaskResponse struct {
	ID        string      `json:"id"`
	TaskType  string      `json:"task_type"`
	Status    task.Status `json:"status"`
	TraceID   *string     `json:"trace_id"`
	LastError *string     `json:"last_error"`
}

func TestImmediateTaskFlow(t *testing.T) {
	env := newIntegrationEnv(t)
	defer env.close(t)

	created := env.createTask(t, map[string]any{
		"task_type": "demo.echo",
		"payload": map[string]any{
			"message": "immediate",
		},
	})

	if created.TaskType != "demo.echo" {
		t.Fatalf("expected task_type demo.echo, got %q", created.TaskType)
	}
	if created.TraceID == nil || !strings.HasPrefix(*created.TraceID, "trace_") {
		t.Fatalf("expected generated trace id, got %#v", created.TraceID)
	}

	found := env.waitForTaskStatus(t, created.ID, 10*time.Second, task.StatusSuccess)
	if found.Status != task.StatusSuccess {
		t.Fatalf("expected success status, got %q", found.Status)
	}
	if found.TraceID == nil || *found.TraceID != *created.TraceID {
		t.Fatalf("expected trace id to remain %v, got %#v", created.TraceID, found.TraceID)
	}

	stored, err := env.store.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("get stored task: %v", err)
	}
	if stored.Status != task.StatusSuccess {
		t.Fatalf("expected stored task success, got %q", stored.Status)
	}

	streamLen, err := env.redis.XLen(context.Background(), env.streamName).Result()
	if err != nil {
		t.Fatalf("stream length: %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("expected one stream message, got %d", streamLen)
	}
}

func TestDelayedTaskFlow(t *testing.T) {
	env := newIntegrationEnv(t)
	defer env.close(t)

	runAt := time.Now().UTC().Add(2 * time.Second)
	created := env.createTask(t, map[string]any{
		"task_type": "demo.echo",
		"payload": map[string]any{
			"message": "scheduled",
		},
		"run_at": runAt.Format(time.RFC3339Nano),
	})

	if created.Status != task.StatusScheduled {
		t.Fatalf("expected scheduled status, got %q", created.Status)
	}

	scheduledIDs, err := env.redis.ZRange(context.Background(), env.scheduled, 0, -1).Result()
	if err != nil {
		t.Fatalf("scheduled zset contents: %v", err)
	}
	if len(scheduledIDs) != 1 || scheduledIDs[0] != created.ID {
		t.Fatalf("expected scheduled zset to contain %q, got %#v", created.ID, scheduledIDs)
	}

	found := env.waitForTaskStatus(t, created.ID, 12*time.Second, task.StatusSuccess)
	if found.Status != task.StatusSuccess {
		t.Fatalf("expected success status, got %q", found.Status)
	}

	remaining, err := env.redis.ZRange(context.Background(), env.scheduled, 0, -1).Result()
	if err != nil {
		t.Fatalf("scheduled zset contents after execution: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected scheduled zset to be empty, got %#v", remaining)
	}
}

func TestIdempotentSubmission(t *testing.T) {
	env := newIntegrationEnv(t)
	defer env.close(t)

	requestBody := map[string]any{
		"task_type":       "demo.echo",
		"idempotency_key": "integration-idempotent-1",
		"payload": map[string]any{
			"message": "same request",
		},
	}

	first := env.createTask(t, requestBody)
	second := env.createTask(t, requestBody)
	if first.ID != second.ID {
		t.Fatalf("expected same task id, got %q and %q", first.ID, second.ID)
	}

	_ = env.waitForTaskStatus(t, first.ID, 10*time.Second, task.StatusSuccess)

	var count int
	err := env.pg.QueryRow(context.Background(), `
		SELECT COUNT(*)
		FROM tasks
		WHERE idempotency_key = $1
	`, "integration-idempotent-1").Scan(&count)
	if err != nil {
		t.Fatalf("count idempotent tasks: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one task row for idempotency key, got %d", count)
	}

	streamLen, err := env.redis.XLen(context.Background(), env.streamName).Result()
	if err != nil {
		t.Fatalf("stream length: %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("expected one stream message for idempotent submission, got %d", streamLen)
	}
}

func TestUnknownTaskTypeFailsToDead(t *testing.T) {
	env := newIntegrationEnv(t)
	defer env.close(t)

	created := env.createTask(t, map[string]any{
		"task_type":   "unknown.type",
		"max_retries": 0,
		"payload": map[string]any{
			"message": "should fail",
		},
	})

	found := env.waitForTaskStatus(t, created.ID, 10*time.Second, task.StatusDead)
	if found.Status != task.StatusDead {
		t.Fatalf("expected dead status, got %q", found.Status)
	}
	if found.LastError == nil || !strings.Contains(*found.LastError, `no handler registered for task_type "unknown.type"`) {
		t.Fatalf("expected unknown task type error, got %#v", found.LastError)
	}

	deadLen, err := env.redis.XLen(context.Background(), env.deadStream).Result()
	if err != nil {
		t.Fatalf("dead stream length: %v", err)
	}
	if deadLen < 1 {
		t.Fatalf("expected at least one dead stream message, got %d", deadLen)
	}

	deadEntries, err := env.redis.XRange(context.Background(), env.deadStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("read dead stream: %v", err)
	}
	if len(deadEntries) == 0 {
		t.Fatal("expected dead stream entries")
	}
	last := deadEntries[len(deadEntries)-1]
	if got, _ := last.Values["task_id"].(string); got != created.ID {
		t.Fatalf("expected dead message task_id %q, got %q", created.ID, got)
	}
	if created.TraceID != nil {
		if got, _ := last.Values["trace_id"].(string); got != *created.TraceID {
			t.Fatalf("expected dead message trace_id %q, got %q", *created.TraceID, got)
		}
	}
	if got, _ := last.Values["last_error"].(string); !strings.Contains(got, `no handler registered for task_type "unknown.type"`) {
		t.Fatalf("expected dead message last_error to mention unknown task type, got %q", got)
	}
}

func newIntegrationEnv(t *testing.T) *integrationEnv {
	t.Helper()

	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	httpAddr := randomHTTPAddr(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		HTTP: config.HTTPConfig{
			Addr: httpAddr,
		},
		Redis: config.RedisConfig{
			Addr:            "localhost:6380",
			StreamName:      "it:tasks:stream:" + suffix,
			ScheduledSetKey: "it:tasks:scheduled:" + suffix,
			DeadStreamName:  "it:tasks:dead:" + suffix,
			ConsumerGroup:   "it-workers-" + suffix,
			ConsumerName:    "it-worker-" + suffix,
		},
		Postgres: config.PostgresConfig{
			Host:     "localhost",
			Port:     "5432",
			Database: "gotaskqueue",
			User:     "gotaskqueue",
			Password: "gotaskqueue",
			SSLMode:  "disable",
		},
		Scheduler: config.SchedulerConfig{
			IntervalSeconds: 1,
			BatchSize:       10,
		},
	}

	ctx := context.Background()
	deps, err := dependencies.Connect(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("connect dependencies: %v", err)
	}

	if err := resetIntegrationState(ctx, deps.Postgres, deps.Redis, cfg.Redis.StreamName, cfg.Redis.ScheduledSetKey, cfg.Redis.DeadStreamName); err != nil {
		deps.Close(logger)
		t.Fatalf("reset integration state: %v", err)
	}

	taskStore := task.NewStore(deps.Postgres)
	metricsRegistry := metrics.NewRegistry().WithSnapshotProvider(taskStore)
	streamPublisher := queue.NewRedisStreamPublisher(deps.Redis, cfg.Redis.StreamName)
	scheduledQueue := queue.NewRedisScheduledQueue(deps.Redis, cfg.Redis.ScheduledSetKey)
	deadPublisher := queue.NewRedisDeadStreamPublisher(deps.Redis, cfg.Redis.DeadStreamName)
	taskService := task.NewService(taskStore, streamPublisher, scheduledQueue).WithMetrics(metricsRegistry)
	server := httpserver.New(httpserver.Config{Addr: httpAddr}, taskService, metricsRegistry)
	streamConsumer := queue.NewRedisStreamConsumer(
		deps.Redis,
		cfg.Redis.StreamName,
		cfg.Redis.ConsumerGroup,
		cfg.Redis.ConsumerName,
		200*time.Millisecond,
	)
	handlerRegistry := worker.NewHandlerRegistry()
	if err := handlerRegistry.Register("demo.echo", worker.DemoEchoHandler{Logger: logger}); err != nil {
		deps.Close(logger)
		t.Fatalf("register demo.echo handler: %v", err)
	}
	workerRunner := worker.New(streamConsumer, taskStore, logger, cfg.Redis.ConsumerName).
		WithHandlerRegistry(handlerRegistry).
		WithPendingRecovery(2*time.Second, 10, 500*time.Millisecond).
		WithMetrics(metricsRegistry).
		WithDeadPublisher(deadPublisher)
	schedulerRunner := scheduler.New(
		taskStore,
		streamPublisher,
		scheduledQueue,
		logger,
		200*time.Millisecond,
		10,
	).WithDeadPublisher(deadPublisher)

	runCtx, cancel := context.WithCancel(context.Background())
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		cancel()
		deps.Close(logger)
		t.Fatalf("listen on %s: %v", httpAddr, err)
	}

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.RunListener(listener)
	}()
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- workerRunner.Run(runCtx)
	}()
	schedDone := make(chan error, 1)
	go func() {
		schedDone <- schedulerRunner.Run(runCtx)
	}()

	waitForHealthyServer(t, "http://"+httpAddr+"/healthz")

	t.Cleanup(func() {
		deps.Close(logger)
	})

	return &integrationEnv{
		baseURL:    "http://" + httpAddr,
		client:     &http.Client{Timeout: 2 * time.Second},
		redis:      deps.Redis,
		pg:         deps.Postgres,
		store:      taskStore,
		cancel:     cancel,
		server:     server,
		serverDone: serverDone,
		workerDone: workerDone,
		schedDone:  schedDone,
		streamName: cfg.Redis.StreamName,
		scheduled:  cfg.Redis.ScheduledSetKey,
		deadStream: cfg.Redis.DeadStreamName,
	}
}

func (e *integrationEnv) close(t *testing.T) {
	t.Helper()

	e.cancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("shutdown server: %v", err)
	}

	for _, done := range []chan error{e.serverDone, e.workerDone, e.schedDone} {
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Fatalf("background runner failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for background runner to stop")
		}
	}
}

func (e *integrationEnv) createTask(t *testing.T, body map[string]any) createTaskResponse {
	t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, e.baseURL+"/tasks", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatalf("create task request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 201, got %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var created createTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create task response: %v", err)
	}
	if created.ID == "" {
		t.Fatal("expected created task id")
	}
	return created
}

func (e *integrationEnv) getTask(t *testing.T, id string) createTaskResponse {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, e.baseURL+"/tasks/"+id, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatalf("get task request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 200, got %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var found createTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&found); err != nil {
		t.Fatalf("decode get task response: %v", err)
	}
	return found
}

func (e *integrationEnv) waitForTaskStatus(t *testing.T, id string, timeout time.Duration, statuses ...task.Status) createTaskResponse {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		found := e.getTask(t, id)
		for _, status := range statuses {
			if found.Status == status {
				return found
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	last := e.getTask(t, id)
	t.Fatalf("task %s did not reach any of %#v within %s, last status=%q", id, statuses, timeout, last.Status)
	return createTaskResponse{}
}

func randomHTTPAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate random port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close temporary listener: %v", err)
	}
	return addr
}

func waitForHealthyServer(t *testing.T, healthURL string) {
	t.Helper()

	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(healthURL)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server did not become healthy: %s", healthURL)
}

func resetIntegrationState(ctx context.Context, pg *pgxpool.Pool, redisClient *redis.Client, streamName, scheduledKey, deadStream string) error {
	if _, err := pg.Exec(ctx, "TRUNCATE TABLE tasks"); err != nil {
		return fmt.Errorf("truncate tasks: %w", err)
	}
	if err := redisClient.Del(ctx, streamName, scheduledKey, deadStream).Err(); err != nil {
		return fmt.Errorf("clear redis integration keys: %w", err)
	}
	return nil
}
