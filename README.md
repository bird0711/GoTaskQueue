# GoTaskQueue

GoTaskQueue 是一个基于 Go、Redis Streams 和 Postgres 的异步任务队列系统，用于演示任务提交、延迟调度、Worker 执行、失败重试、死信和可观测性等后端核心能力。

## 快速启动

### 1. 环境要求

本地需要安装：

- Go，版本以 `go.mod` 为准
- Docker
- Docker Compose
- make
- golangci-lint

### 2. 启动依赖

```sh
make up
```

首次执行时，Docker Compose 会自动下载并启动项目依赖的服务：

- Redis：`localhost:6380`
- Postgres：`localhost:5432`
- Prometheus：`localhost:9090`

### 3. 执行 migration

```sh
make migrate-up
```

该命令会初始化数据库表结构。重复执行时，已经执行过的 migration 会被跳过。

### 4. 启动应用

```sh
make run
```

应用默认监听：

```text
http://localhost:8080
```

### 5. 提交测试任务

另开一个终端，提交一个立即执行任务：

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"hello"}}'
```

返回结果中会包含任务 ID。可以继续查询任务状态：

```sh
curl -sS http://localhost:8080/tasks/TASK_ID
```

### 6. 打开 dashboard

```text
http://localhost:8080/dashboard
```

也可以查看：

```text
http://localhost:8080/metrics
http://localhost:9090
```

### 最短跑通流程

```sh
make up
make migrate-up
make run
```

然后提交一个 `demo.echo` 任务并打开 `/dashboard`。

## 项目功能

- HTTP API：提交任务、查询任务状态。
- Redis Streams：负责任务投递和 Worker 消费。
- Redis ZSET：负责延迟任务调度。
- Postgres：持久化任务状态、payload、错误和重试信息。
- Worker：支持并发执行和批量消费。
- Scheduler：扫描到期延迟任务并投递到 Redis Stream。
- 重试和死信：失败任务可重试，耗尽后进入 `dead` 并写入 `tasks:dead`。
- 幂等提交：通过 `idempotency_key` 避免重复创建任务。
- trace_id：贯通 API、数据库、Redis 消息和日志。
- handler 示例：内置 `demo.echo` 和 `webhook.deliver`。
- 可观测性：提供 `/metrics`、Prometheus 和只读 dashboard。
- graceful shutdown：退出时等待 server、worker、scheduler 收尾。

## 架构说明

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

Dashboard / Metrics
   |
   +--> /dashboard
   +--> /dashboard/tasks/{id}
   +--> /metrics
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

## API 示例

提交立即任务：

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"hello"}}'
```

提交延迟任务：

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"demo.echo","payload":{"message":"later"},"run_at":"2099-01-01T00:00:00Z"}'
```

提交 webhook 任务：

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"webhook.deliver","payload":{"url":"https://example.com/webhook","method":"POST","headers":{"Content-Type":"application/json"},"body":{"message":"hello"}}}'
```

查询任务：

```sh
curl -sS http://localhost:8080/tasks/TASK_ID
```

制造失败任务：

```sh
curl -sS -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"unknown.type","max_retries":0,"payload":{"message":"fail"}}'
```

## 配置说明

配置通过环境变量读取。可以参考 `.env.example`。

常用配置：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | HTTP 服务监听地址 |
| `REDIS_ADDR` | `localhost:6380` | Redis 地址 |
| `POSTGRES_HOST` | `localhost` | Postgres host |
| `POSTGRES_PORT` | `5432` | Postgres port |
| `POSTGRES_DB` | `gotaskqueue` | Postgres 数据库 |
| `POSTGRES_USER` | `gotaskqueue` | Postgres 用户 |
| `POSTGRES_PASSWORD` | `gotaskqueue` | Postgres 密码 |
| `WORKER_CONCURRENCY` | `4` | Worker 最大并发执行数 |
| `WORKER_BATCH_SIZE` | `10` | Worker 每次最多读取消息数 |
| `SCHEDULER_INTERVAL_SECONDS` | `2` | Scheduler 扫描间隔 |
| `SCHEDULER_BATCH_SIZE` | `100` | Scheduler 每批扫描数量 |

完整配置见 `.env.example`。

## 测试

运行单元测试、vet 和 lint：

```sh
make check
```

运行集成测试：

```sh
make integration-test
```

集成测试依赖 Docker Compose 中的 Redis 和 Postgres。

## 常用命令

```sh
make up               # 启动 Redis / Postgres / Prometheus
make migrate-up       # 执行数据库迁移
make run              # 启动 Go 应用
make check            # 单元测试 + vet + lint
make integration-test # 集成测试
make down             # 停止本地依赖
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

## 面试讲解点

- 为什么使用 Redis Streams，而不是只用数据库轮询。
- 为什么 Postgres 是任务状态的 source of truth。
- at-least-once delivery 和 exactly-once 的取舍。
- Redis pending recovery 和 Postgres running timeout recovery 分别解决什么问题。
- `idempotency_key` 如何降低重复提交影响。
- retry、dead stream、不可重试错误如何设计。
- `WORKER_CONCURRENCY` 和 `WORKER_BATCH_SIZE` 如何配合。
- graceful shutdown 如何避免任务处理中断。

## 上传说明

`.docker-data/`、`.env`、日志、构建产物、编辑器配置、AI 控制文档和开发过程文档都已写入 `.gitignore`。公开仓库只保留运行项目所需的源码、配置、迁移、测试和 README。
