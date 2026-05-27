# MVP Manual Acceptance

This guide verifies the current MVP with local dependencies, the Go app, dashboard, metrics, and Prometheus.

## Prerequisites

- Docker with Compose support.
- Go matching `go.mod`.
- `golangci-lint` available on `PATH`.
- Run commands from the repository root.

## Local Setup

1. Start dependencies:

   ```sh
   make up
   ```

2. Apply the initial database migration:

   ```sh
   make migrate-up
   ```

   `make migrate-up` creates `schema_migrations`, runs pending `migrations/*.up.sql` files in filename order, and skips versions already recorded.

3. Run local checks:

   ```sh
   make check
   ```

4. Run integration tests against local Docker Compose dependencies:

   ```sh
   make integration-test
   ```

   Expected result:

   - Integration tests pass after local dependencies are started and migrations are applied.
   - The suite verifies immediate task flow, delayed task flow, idempotent submission, and unknown `task_type` failure flow end-to-end across API, Postgres, Redis Stream, Redis ZSET, Worker, and Scheduler.

5. Start the Go application:

   ```sh
   make run
   ```

The app listens on `HTTP_ADDR`, which defaults to `:8080`.

## Submit an Immediate Task

Create a task that should be queued immediately:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"hello from acceptance"}}'
```

Expected result:

- The response is `201 Created`.
- The response contains a task `id` and a `trace_id` (auto-generated when not provided, prefixed with `trace_`).
- The task should move through `pending`, `running`, and then `success` as the worker consumes it.
- The application log shows the `demo.echo` handler processed the payload, and lifecycle log lines (`task running`, `task succeeded`, `task message acked`) include the same `trace_id`.

Fetch the task by ID:

```sh
curl -sS http://localhost:8080/tasks/TASK_ID
```

Expected result:

- The task eventually has `"status":"success"`.

## Verify Idempotent Submission

Create a task with an `idempotency_key`:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"idempotent"},"idempotency_key":"acceptance-demo-1"}'
```

Submit the same request again with the same `idempotency_key`:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"idempotent"},"idempotency_key":"acceptance-demo-1"}'
```

Expected result:

- Both responses contain the same task `id`.
- The second submission returns the existing task instead of creating a new row.
- The second submission does not publish another Redis Stream message.

## Submit a Delayed Task

Create a task with a future `run_at` timestamp:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"scheduled"},"run_at":"2099-01-01T00:00:00Z"}'
```

Expected result:

- The response is `201 Created`.
- The task status is `scheduled`.
- The task is not immediately executed.
- The task ID is added to the Redis scheduled ZSET.

For a quick scheduler check, submit a task with `run_at` a few seconds in the future, then fetch it after it is due.

Expected result:

- The scheduler dispatches it from the Redis scheduled ZSET after `run_at`.
- The task eventually reaches `success`.

## Verify Unknown Task Type Failure

Create a task for an unregistered `task_type`:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"unknown.type","payload":{"message":"should fail"}}'
```

Expected result:

- The response is `201 Created`.
- The task first becomes `pending`, then `running`, then enters the existing failure chain.
- Fetching the task later shows `status` eventually becomes `retrying` or `dead`.
- `last_error` includes `no handler registered for task_type "unknown.type"`.

## Verify Dead Stream

After a task reaches `dead` (for example, by setting `max_retries: 0` on an unknown `task_type`), inspect the dead Redis Stream:

```sh
docker compose exec redis redis-cli XLEN tasks:dead
docker compose exec redis redis-cli XREVRANGE tasks:dead + - COUNT 5
```

Expected result:

- `XLEN tasks:dead` is at least 1 after a task enters `dead`.
- The most recent entry contains `task_id` matching the dead task, `task_type` matching the original task type, `trace_id` matching the API response trace_id, `last_error` containing the failure message, and `retry_count` matching the task's final retry count.
- The entry does not contain a `payload` field (consumers can fetch payload via `GET /tasks/{id}` if needed).
- If Redis is briefly unavailable when the worker tries to publish, the application log shows a `publish dead task to dead stream` warning and the task still reaches `dead` in Postgres and the main stream message is still acked.

The dead stream name can be overridden via `REDIS_DEAD_STREAM_NAME` (default `tasks:dead`).

## Dashboard

Open:

```text
http://localhost:8080/dashboard
```

Expected result:

- The page shows total tasks.
- The page shows counts for each task status.
- The page shows queue backlog and running task count.
- The page shows recent failed tasks and recent dead tasks when present.
- The page is read-only.

The same dashboard is also available at:

```text
http://localhost:8080/
```

## Metrics

Open or fetch:

```sh
curl -sS http://localhost:8080/metrics
```

Expected result:

- The response is Prometheus text format.
- Metric names use the `gotaskqueue_*` prefix.
- The response includes task lifecycle counters, duration summaries, queue backlog, and running task gauges.

Useful metric names:

- `gotaskqueue_tasks_submitted_total`
- `gotaskqueue_tasks_started_total`
- `gotaskqueue_tasks_succeeded_total`
- `gotaskqueue_tasks_failed_total`
- `gotaskqueue_tasks_retried_total`
- `gotaskqueue_tasks_dead_total`
- `gotaskqueue_task_execution_duration_seconds`
- `gotaskqueue_task_wait_duration_seconds`
- `gotaskqueue_queue_backlog`
- `gotaskqueue_worker_running_tasks`

## Running Timeout Recovery

Running timeout recovery is exercised by the scheduler against Postgres tasks that remain `running` beyond `started_at + timeout_seconds`.

To verify manually, use `psql` in the Postgres container to create or update a test task into an expired `running` state, then wait for one scheduler tick:

```sh
make migrate-up
```

Expected result:

- A task with `status = 'running'` and expired `started_at + timeout_seconds` is marked failed by recovery.
- The task then moves to `retrying` or `dead` based on `retry_count` and `max_retries`.
- `last_error` includes `running timeout recovery`.
- A task that has not exceeded its timeout remains `running`.

## Prometheus

Open:

```text
http://localhost:9090
```

Expected result:

- Prometheus is running.
- The Go app target is scrapeable after the app is started.
- Querying `gotaskqueue_tasks_submitted_total` returns a value after submitting tasks.
- Querying `gotaskqueue_queue_backlog` and `gotaskqueue_worker_running_tasks` returns current gauges.

## Shutdown

Stop local dependency services:

```sh
make down
```
