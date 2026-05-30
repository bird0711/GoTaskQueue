# GoTaskQueue

GoTaskQueue is a Go asynchronous task queue middleware and backend infrastructure component. It accepts tasks over HTTP, persists task metadata in Postgres, delivers work through Redis Streams, schedules delayed work with Redis ZSET, executes handlers in workers, and exposes operational visibility through metrics and a read-only dashboard.

The project is intentionally not a full workflow platform. The goal is to demonstrate the core design problems behind async task systems: durable state, at-least-once delivery, retries, idempotent submission, delayed scheduling, worker recovery, dead-letter handling, trace correlation, metrics, and graceful shutdown.

## What It Solves

Synchronous APIs should not block on slow or unreliable work such as sending messages, generating reports, calling third-party APIs, or running background maintenance. GoTaskQueue separates submission from execution:

- API callers get a durable task record immediately.
- Redis Streams decouple producers from workers.
- Postgres remains the source of truth for task state.
- Redis ZSET triggers delayed execution without making Redis the durable task store.
- Worker and scheduler recovery paths reduce the impact of crashes and timeouts.
- Metrics, trace IDs, dashboard pages, and a dead stream make failures observable.

## Architecture

```text
              HTTP API
                 |
                 v
        +------------------+
        |   Task Service   |
        +------------------+
          |              |
          v              v
   +-------------+   +----------------+
   |  Postgres   |   | Redis Scheduled|
   | tasks table |   | ZSET           |
   +-------------+   +----------------+
          |              |
          |              v
          |        +-----------+
          |        | Scheduler |
          |        +-----------+
          |              |
          v              v
        +------------------+
        | Redis Stream     |
        | tasks:stream     |
        +------------------+
                 |
                 v
        +------------------+
        | Worker Pool      |
        | handler registry |
        +------------------+
          |              |
          v              v
   +-------------+   +----------------+
   |  Postgres   |   | Dead Stream    |
   | final state |   | tasks:dead     |
   +-------------+   +----------------+
```

Task state flow:

```text
scheduled -> pending -> running -> success
scheduled -> pending -> running -> failed -> retrying -> pending -> running
scheduled -> pending -> running -> failed -> dead
pending -> running -> failed -> dead
running -> failed -> retrying
running -> failed -> dead
```

`success` and `dead` are terminal states. Delivery is at-least-once, not exactly-once; task handlers should still be written with external idempotency in mind.

## Core Features

- Asynchronous tasks: `POST /tasks` stores a task and publishes immediate work to Redis Streams.
- Delayed tasks: future `run_at` values are written to Redis ZSET and dispatched by the scheduler when due.
- Handler registry: workers dispatch by `task_type`; the app registers `demo.echo` and `webhook.deliver` examples.
- Handler validation: handlers parse their own JSON payloads and return readable errors; retryable errors follow normal retry/dead handling, while non-retryable errors such as invalid payloads, unknown task types, and webhook 4xx responses go directly to `dead`.
- Retries: failures move through `failed -> retrying` with exponential second-level backoff.
- Dead letters: exhausted tasks enter `dead` and are also written to `tasks:dead` with `task_id`, `task_type`, `trace_id`, `last_error`, and `retry_count`.
- Idempotent submission: repeated `idempotency_key` requests return the existing task and do not publish duplicate work.
- Recovery: Redis pending messages can be claimed; expired Postgres `running` tasks are recovered by the scheduler.
- Timeouts: workers execute with per-task `timeout_seconds`.
- Trace correlation: `trace_id` is accepted or generated at creation, stored in Postgres, sent through Redis Stream messages, and logged by worker lifecycle events.
- Metrics: `/metrics` exposes `gotaskqueue_*` counters, summaries, and gauges.
- Dashboard: `/dashboard` shows status counts, queue backlog, recent tasks, failed/dead tasks, and read-only task detail pages.
- Graceful shutdown: the app waits for server, worker, and scheduler goroutines to exit before closing dependencies.

## Local Setup

Prerequisites:

- Go matching `go.mod`
- Docker with Compose support
- `golangci-lint` on `PATH`

Start local dependencies:

```sh
make up
```

Apply database migrations:

```sh
make migrate-up
```

Run checks:

```sh
make check
```

Start the application:

```sh
make run
```

Default local endpoints:

- API and dashboard: `http://localhost:8080`
- Dashboard: `http://localhost:8080/dashboard`
- Metrics: `http://localhost:8080/metrics`
- Prometheus: `http://localhost:9090`
- Redis: `localhost:6380`
- Postgres: `localhost:5432`

Stop local dependencies:

```sh
make down
```

## v1.0 Demo Runbook

Start dependencies and apply migrations:

```sh
make up
make migrate-up
```

Start the application in another terminal:

```sh
make run
```

Submit an immediate task:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"hello now"}}'
```

Submit a delayed task:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"hello later"},"run_at":"2099-01-01T00:00:00Z"}'
```

Submit a `webhook.deliver` task. For a local demo, point `url` at any temporary HTTP endpoint you control:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"webhook.deliver","payload":{"url":"https://example.com/webhook","method":"POST","headers":{"Content-Type":"application/json"},"body":{"message":"hello webhook"}}}'
```

Create failure cases and observe retry/dead behavior:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"webhook.deliver","max_retries":0,"payload":{"url":"https://example.com/missing","method":"POST","headers":{},"body":{"message":"bad request"}}}'

curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"unknown.type","max_retries":0,"payload":{"message":"should become dead"}}'
```

For retry behavior, point `webhook.deliver` at a temporary endpoint that returns 5xx or times out and leave `max_retries` above zero. For direct dead behavior, invalid payloads, webhook 4xx responses, and unknown task types are deterministic.

Fetch a task, replacing `TASK_ID` with an ID returned by `POST /tasks`:

```sh
curl -sS http://localhost:8080/tasks/TASK_ID
```

Open operational views:

```text
http://localhost:8080/dashboard
http://localhost:8080/dashboard/tasks/TASK_ID
http://localhost:8080/metrics
http://localhost:9090
```

The dashboard is a read-only operations page for inspecting task state. It is not intended to be a full product website or management console.

## Configuration

Configuration is environment-variable based. Useful defaults are shown in `.env.example`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Go HTTP server listen address |
| `REDIS_ADDR` | `localhost:6380` | Redis client address |
| `REDIS_STREAM_NAME` | `tasks:stream` | Main Redis Stream |
| `REDIS_SCHEDULED_SET_KEY` | `tasks:scheduled` | Delayed task ZSET |
| `REDIS_DEAD_STREAM_NAME` | `tasks:dead` | Dead-letter Redis Stream |
| `REDIS_CONSUMER_GROUP` | `gotaskqueue-workers` | Redis Stream consumer group |
| `REDIS_CONSUMER_NAME` | `gotaskqueue-worker-1` | Worker consumer name |
| `POSTGRES_HOST` | `localhost` | Postgres host |
| `POSTGRES_PORT` | `5432` | Postgres port |
| `POSTGRES_DB` | `gotaskqueue` | Postgres database |
| `POSTGRES_USER` | `gotaskqueue` | Postgres user |
| `POSTGRES_PASSWORD` | `gotaskqueue` | Postgres password |
| `SCHEDULER_INTERVAL_SECONDS` | `2` | Scheduler tick interval |
| `SCHEDULER_BATCH_SIZE` | `100` | Scheduler batch size |
| `WORKER_CONCURRENCY` | `4` | Max in-process concurrent task executions |
| `WORKER_BATCH_SIZE` | `10` | Max Redis Stream messages read per worker poll |

## API Examples

Create an immediate task:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"hello"}}'
```

Create a task with an external trace ID:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","trace_id":"trace_from_client_123","payload":{"message":"trace me"}}'
```

Create a delayed task:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"later"},"run_at":"2099-01-01T00:00:00Z"}'
```

Create a webhook delivery task:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"webhook.deliver","payload":{"url":"https://example.com/webhook","method":"POST","headers":{"Content-Type":"application/json"},"body":{"message":"hello"}}}'
```

Create an idempotent task:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","idempotency_key":"request-123","payload":{"message":"only once"}}'
```

Force a task into the failure/dead path by using an unknown task type and no retries:

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"unknown.type","max_retries":0,"payload":{"message":"should fail"}}'
```

Fetch task state:

```sh
curl -sS http://localhost:8080/tasks/TASK_ID
```

Task responses include fields such as `id`, `task_type`, `payload`, `status`, `trace_id`, `retry_count`, `last_error`, `worker_id`, `started_at`, `finished_at`, `created_at`, and `updated_at`.

## Dashboard And Prometheus

Open the dashboard:

```text
http://localhost:8080/dashboard
```

The dashboard is read-only. It shows total tasks, status counts, queue backlog, running task count, recent tasks, recent failed tasks, recent dead tasks, and task detail pages at:

```text
http://localhost:8080/dashboard/tasks/TASK_ID
```

Fetch Prometheus metrics:

```sh
curl -sS http://localhost:8080/metrics
```

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

Open Prometheus:

```text
http://localhost:9090
```

## Dead Stream

When a task reaches `dead`, the worker or scheduler attempts to publish a compact dead-letter message to Redis Stream `tasks:dead`.

Inspect it locally:

```sh
docker compose exec redis redis-cli XLEN tasks:dead
docker compose exec redis redis-cli XREVRANGE tasks:dead + - COUNT 5
```

Dead stream messages intentionally omit full payload. Consumers can use `task_id` to fetch the full task from Postgres through `GET /tasks/{id}`.

## Tests

Run unit, vet, and lint checks:

```sh
make check
```

Run integration tests against local Docker Compose dependencies:

```sh
make integration-test
```

`make integration-test` applies migrations and runs tests tagged with `integration`. The suite verifies API, Postgres, Redis Stream, Redis ZSET, Scheduler, Worker, idempotent submission, unknown `task_type`, and dead stream behavior.

CI runs `make check` by default. Integration tests are intentionally separate because they require local Redis and Postgres services.

## Project Structure

```text
cmd/gotaskqueue/        application entrypoint
internal/app/           top-level component wiring and graceful shutdown
internal/config/        environment configuration
internal/httpserver/    API, metrics endpoint, dashboard
internal/task/          task model, store, state machine, retry logic
internal/queue/         Redis Stream, ZSET, dead stream adapters
internal/scheduler/     delayed task dispatch and running-timeout recovery
internal/worker/        Redis consumer, handler registry, worker pool
migrations/             Postgres schema migrations
configs/prometheus/     Prometheus scrape configuration
docs/                   design notes, status, manual acceptance
```

## Design Notes For Interviews

Key tradeoffs to discuss:

- The system uses at-least-once delivery. This keeps the design realistic and avoids pretending Redis Streams plus Postgres can provide exactly-once side effects.
- Postgres is the source of truth. Redis is used for delivery, scheduling triggers, and dead-letter notifications.
- Redis Stream pending recovery handles messages delivered but not acked before a worker crash; Postgres running-timeout recovery handles tasks that were marked running but stopped making progress.
- Conditional state updates protect worker claims and scheduler dispatch from duplicate execution paths.
- `idempotency_key` prevents duplicate task creation at submission time, but handler side effects still need business-level idempotency.
- Dead letters make exhausted or non-retryable failures inspectable without storing full payloads in Redis.
- Graceful shutdown cancels runners, waits for in-flight worker tasks, and only then closes Redis/Postgres dependencies.
- `trace_id` is lightweight correlation, not a full distributed tracing implementation.
- Dead stream publication is best-effort and does not block the Postgres terminal state.

## Future Work

- `XAUTOCLAIM`: replace the current pending recovery scan/claim path with Redis `XAUTOCLAIM` for simpler pending ownership transfer.
- Prometheus official client: replace the lightweight custom text exposition with `prometheus/client_golang` histograms, labels, and registry support.
- Dashboard pagination: add pagination if recent task and detail views become expensive on large task tables.
- Task type concurrency limits: constrain noisy task types independently from global `WORKER_CONCURRENCY`.
- Rate limiting: add per-task-type or global rate controls for third-party integrations.
- Handler examples: add additional realistic handlers such as report generation.
- Operational tooling: add CLI helpers for inspecting queues, pending messages, and dead tasks.
