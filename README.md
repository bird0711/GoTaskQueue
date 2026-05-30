# GoTaskQueue

GoTaskQueue 是一个基于 Go、Redis Streams 和 Postgres 的异步任务队列系统。它不是完整网站，也不是工作流平台，而是一个后端基础设施组件：业务系统通过 HTTP API 提交任务，GoTaskQueue 负责持久化、投递、调度、执行、重试、死信和观测。

这个项目适合作为 Go 后端作品展示，重点体现：

- Redis Streams 的 at-least-once 投递。
- Postgres 作为任务状态事实来源。
- 延迟任务调度。
- Worker 并发和批量消费。
- 幂等提交。
- 失败重试、超时、死信。
- trace_id 链路关联。
- Prometheus 指标和只读 dashboard。
- Docker Compose 本地依赖和自动化测试。

## 功能特性

- 任务提交：`POST /tasks` 创建异步任务。
- 任务查询：`GET /tasks/{id}` 查询任务状态。
- 延迟任务：未来 `run_at` 会写入 Redis ZSET，由 Scheduler 到期投递。
- Redis Stream 队列：立即任务和到期任务通过 Redis Streams 投递给 Worker。
- Postgres 持久化：任务元数据、状态、错误、重试次数等存储在 Postgres。
- 幂等提交：`idempotency_key` 防止重复创建任务。
- Worker 并发：`WORKER_CONCURRENCY` 控制同时执行任务数量。
- 批量消费：`WORKER_BATCH_SIZE` 控制每次最多读取多少条 Redis Stream 消息。
- 重试和死信：失败任务按退避策略重试，耗尽后进入 `dead` 并写入 `tasks:dead`。
- 真实 handler 示例：内置 `webhook.deliver`，可发送 HTTP webhook。
- 可观测性：提供 `/metrics`、Prometheus 和只读 dashboard。
- 优雅关闭：进程退出时等待 server、worker、scheduler 正常收尾。

## 架构概览

```text
HTTP API
   |
   v
Task Service
   |
   +--> Postgres tasks table
   |
   +--> Redis Stream tasks:stream
   |
   +--> Redis ZSET tasks:scheduled

Scheduler
   |
   v
Redis Stream tasks:stream
   |
   v
Worker
   |
   +--> Handler registry
   +--> Postgres 状态更新
   +--> Redis Stream ack
   +--> Redis Stream tasks:dead

Metrics / Dashboard
   |
   +--> /metrics
   +--> /dashboard
   +--> /dashboard/tasks/{id}
```

任务状态流转：

```text
scheduled -> pending -> running -> success
scheduled -> pending -> running -> failed -> retrying -> pending -> running
scheduled -> pending -> running -> failed -> dead
pending -> running -> failed -> dead
running -> failed -> retrying
running -> failed -> dead
```

系统采用 at-least-once delivery，不承诺 exactly-once。外部副作用任务仍需要业务侧保证幂等。

## 快速启动

### 1. 准备环境

需要安装：

- Go，版本以 `go.mod` 为准。
- Docker 和 Docker Compose。
- `golangci-lint`。
- `make`。

### 2. 启动本地依赖

```sh
make up
```

这会启动：

- Redis：`localhost:6380`
- Postgres：`localhost:5432`
- Prometheus：`localhost:9090`

### 3. 执行数据库迁移

```sh
make migrate-up
```

迁移脚本支持重复执行，已执行过的 migration 会跳过。

### 4. 启动应用

```sh
make run
```

应用默认监听：

```text
http://localhost:8080
```

### 5. 提交一个立即任务

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"hello"}}'
```

返回结果中会包含任务 ID，例如：

```json
{
  "id": "task_xxx",
  "task_type": "demo.echo",
  "status": "pending"
}
```

### 6. 查询任务状态

```sh
curl -sS http://localhost:8080/tasks/TASK_ID
```

把 `TASK_ID` 替换成上一步返回的任务 ID。

### 7. 提交一个延迟任务

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"later"},"run_at":"2099-01-01T00:00:00Z"}'
```

### 8. 提交一个 webhook 任务

`webhook.deliver` 是项目内置的真实业务 handler 示例。

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"webhook.deliver","payload":{"url":"https://example.com/webhook","method":"POST","headers":{"Content-Type":"application/json"},"body":{"message":"hello webhook"}}}'
```

行为说明：

- 2xx：任务成功。
- 4xx：不可重试错误，任务进入 `dead`。
- 5xx、网络错误、超时：可重试错误，进入 retry / dead 链路。

### 9. 制造一个失败任务

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"unknown.type","max_retries":0,"payload":{"message":"fail"}}'
```

这个任务会进入 `dead`，可用于演示失败和死信链路。

## Dashboard

打开：

```text
http://localhost:8080/dashboard
```

dashboard 是只读运维页面，展示：

- 任务总数。
- 各状态任务数量。
- 队列积压。
- running 数量。
- 最近任务。
- 最近失败任务。
- 最近 dead 任务。

任务详情页：

```text
http://localhost:8080/dashboard/tasks/TASK_ID
```

详情页展示任务 payload、状态、错误、重试次数、运行时间、trace_id 等信息。

## Metrics 和 Prometheus

应用暴露 Prometheus 指标：

```text
http://localhost:8080/metrics
```

Prometheus 页面：

```text
http://localhost:9090
```

常用指标：

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

## 配置项

常用环境变量见 `.env.example`：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | HTTP 服务监听地址 |
| `REDIS_ADDR` | `localhost:6380` | Redis 地址 |
| `REDIS_STREAM_NAME` | `tasks:stream` | 主任务 Stream |
| `REDIS_SCHEDULED_SET_KEY` | `tasks:scheduled` | 延迟任务 ZSET |
| `REDIS_DEAD_STREAM_NAME` | `tasks:dead` | 死信 Stream |
| `REDIS_CONSUMER_GROUP` | `gotaskqueue-workers` | Redis consumer group |
| `REDIS_CONSUMER_NAME` | `gotaskqueue-worker-1` | Redis consumer 名称 |
| `POSTGRES_HOST` | `localhost` | Postgres host |
| `POSTGRES_PORT` | `5432` | Postgres port |
| `POSTGRES_DB` | `gotaskqueue` | Postgres 数据库 |
| `POSTGRES_USER` | `gotaskqueue` | Postgres 用户 |
| `POSTGRES_PASSWORD` | `gotaskqueue` | Postgres 密码 |
| `SCHEDULER_INTERVAL_SECONDS` | `2` | Scheduler 扫描间隔 |
| `SCHEDULER_BATCH_SIZE` | `100` | Scheduler 每批扫描数量 |
| `WORKER_CONCURRENCY` | `4` | Worker 最大并发执行数 |
| `WORKER_BATCH_SIZE` | `10` | Worker 每次最多读取消息数 |

## 测试

运行单元测试、vet 和 lint：

```sh
make check
```

运行集成测试：

```sh
make integration-test
```

集成测试依赖本地 Docker Compose 中的 Redis 和 Postgres。

## 常用命令

```sh
make up              # 启动 Redis / Postgres / Prometheus
make migrate-up      # 执行数据库迁移
make run             # 启动 Go 应用
make check           # 单元测试 + vet + lint
make integration-test # 集成测试
make down            # 停止本地依赖
```

## 项目结构

```text
cmd/gotaskqueue/        应用入口
internal/app/           应用组装和 graceful shutdown
internal/config/        环境变量配置
internal/httpserver/    HTTP API、metrics、dashboard
internal/task/          任务模型、状态机、存储和 service
internal/queue/         Redis Stream、ZSET、dead stream 适配
internal/scheduler/     延迟任务调度和 running timeout recovery
internal/worker/        Redis consumer、handler registry、worker pool
migrations/             Postgres schema migrations
configs/prometheus/     Prometheus 配置
scripts/                本地辅助脚本
```

## 面试讲解重点

- 为什么使用 Redis Streams，而不是只用数据库轮询。
- 为什么 Postgres 是任务状态的 source of truth。
- at-least-once delivery 和 exactly-once 的取舍。
- Redis pending recovery 和 Postgres running timeout recovery 分别解决什么问题。
- `idempotency_key` 如何降低重复提交影响。
- retry、dead stream、不可重试错误如何设计。
- `WORKER_CONCURRENCY` 和 `WORKER_BATCH_SIZE` 如何配合。
- graceful shutdown 如何避免任务处理中断。

## 本地数据和上传说明

Docker 本地数据目录 `.docker-data/` 已写入 `.gitignore`，不会上传到 Git。

`.env`、日志、构建产物、编辑器配置、AI 控制文档和开发过程文档也已忽略。公开仓库只保留运行项目所需的源码、配置、迁移、测试和 README。
