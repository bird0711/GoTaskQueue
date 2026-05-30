# MVP 方案

## 1. 项目定位

本项目是一个 Go 分布式任务调度与异步队列系统。

MVP 的目标不是做一个完整的工作流平台，而是实现一个能展示 Go 后端工程能力的异步任务系统：

- API 接收任务提交。
- 任务被异步投递到队列。
- worker 并发消费任务。
- 系统维护任务状态机。
- 支持延迟任务、失败重试、死信、幂等、超时和可观测性。

项目需要能清楚解释以下问题：

- 为什么需要队列。
- 如何降低任务重复执行的影响。
- worker 崩溃后任务如何恢复。
- 如何设计重试和退避。
- 如何做并发控制和限流。
- 如何观察任务积压。
- Redis Streams 的 ack 和 pending 机制如何工作。
- 如何做 graceful shutdown。

## 2. MVP 范围

MVP 必须包含：

- 任务提交 API。
- 任务状态查询 API。
- 任务状态机：`scheduled`、`pending`、`running`、`success`、`failed`、`retrying`、`dead`。
- Redis Streams 队列消费。
- Redis ZSET 延迟任务调度。
- Postgres 任务元数据持久化。
- worker 并发消费。
- 任务执行超时。
- 重试和指数退避。
- 死信任务处理。
- 幂等任务提交。
- Prometheus metrics。
- 简单 dashboard。
- graceful shutdown。

MVP 暂不包含：

- RabbitMQ / NATS / Kafka 多后端适配。
- 分布式工作流 DAG。
- 多租户。
- 复杂权限系统。
- 复杂 Web 管理后台。
- 任务优先级。
- 生产级分片调度。
- Exactly-once 语义。

## 3. 技术选型

MVP 使用：

- Go：主开发语言。
- Redis Streams：主任务队列。
- Redis ZSET：延迟任务调度集合。
- Postgres：任务元数据、状态、幂等记录。
- Prometheus：指标采集。
- Docker Compose：本地依赖编排。
- Go HTTP Server：API、metrics、简单 dashboard。

本地开发阶段：

- Go 应用在本地直接运行。
- Redis、Postgres、Prometheus 使用 Docker Compose 启动。
- 暂不强制将 Go 应用容器化。

## 4. 核心组件

### API

API 负责：

- 接收任务提交。
- 校验请求参数。
- 生成或接收 `idempotency_key`。
- 写入 Postgres。
- 对立即执行任务投递到 Redis Stream。
- 对延迟任务写入 Redis ZSET。
- 查询任务状态。
- 暴露健康检查接口。
- 暴露 dashboard 页面。

### Scheduler

Scheduler 负责：

- 扫描 Redis ZSET 中到期的延迟任务。
- 将到期任务投递到 Redis Stream。
- 将任务状态从 `scheduled` 更新为 `pending`。
- 避免重复调度同一个任务。

### Worker

Worker 负责：

- 使用 Redis Streams consumer group 消费任务。
- 将任务状态从 `pending` 或 `retrying` 改为 `running`。
- 使用 goroutine worker pool 并发执行任务。
- 使用 `context.WithTimeout` 控制任务超时。
- 成功后更新状态为 `success` 并 ack 消息。
- 失败后进入重试或死信。
- 处理 pending 消息恢复。
- 支持 graceful shutdown。

### Storage

Storage 负责：

- Postgres 任务表读写。
- 任务状态流转。
- 幂等键唯一约束。
- 任务执行记录。
- 失败原因和重试次数记录。

### Metrics

Metrics 负责：

- 任务提交数量。
- 任务开始数量。
- 成功任务数量。
- 失败任务数量。
- 重试任务数量。
- 死信任务数量。
- 队列积压数量。
- 正在运行任务数量。
- 任务等待耗时。
- 任务执行耗时。

## 5. 任务状态机

状态定义：

- `scheduled`：延迟任务，尚未到执行时间。
- `pending`：任务已准备好执行，等待 worker 消费。
- `running`：任务已被 worker 领取并正在执行。
- `success`：任务执行成功。
- `failed`：任务本次执行失败。
- `retrying`：任务等待下一次重试。
- `dead`：任务达到最大重试次数，不再自动执行。

主要流转：

```text
scheduled -> pending -> running -> success
scheduled -> pending -> running -> failed -> retrying -> pending -> running
scheduled -> pending -> running -> failed -> dead
pending -> running -> failed -> dead
running -> failed -> retrying
running -> failed -> dead
```

约束：

- 只有 `scheduled` 可以由 scheduler 转为 `pending`。
- 只有 `pending` 或 `retrying` 可以被 worker 转为 `running`。
- `success` 和 `dead` 是终态。
- 状态流转必须通过统一服务逻辑完成。
- 不允许在多个模块中随意更新任务状态。

## 6. 数据模型草案

任务表 `tasks` 建议字段：

- `id`：任务 ID。
- `task_type`：任务类型。
- `payload`：任务参数，JSON。
- `status`：任务状态。
- `idempotency_key`：幂等键，可为空但建议支持唯一约束。
- `run_at`：计划执行时间。
- `timeout_seconds`：任务超时时间。
- `max_retries`：最大重试次数。
- `retry_count`：当前重试次数。
- `next_retry_at`：下次重试时间。
- `last_error`：最后一次错误。
- `worker_id`：当前执行 worker。
- `started_at`：开始执行时间。
- `finished_at`：完成时间。
- `created_at`：创建时间。
- `updated_at`：更新时间。

建议约束：

- `id` 主键。
- `idempotency_key` 唯一索引。
- `status` 建立索引。
- `run_at` 建立索引。
- `created_at` 建立索引。

## 7. Redis 设计

Redis Streams：

```text
stream: tasks:stream
group:  gotaskqueue-workers
```

消息内容：

```text
task_id
task_type
trace_id
```

延迟任务 ZSET：

```text
key:   tasks:scheduled
score: run_at unix timestamp
value: task_id
```

死信可选结构：

```text
stream: tasks:dead
```

说明：

- Redis Stream 负责任务投递和消费。
- Postgres 负责长期任务状态。
- Redis ZSET 只负责延迟任务触发，不作为任务事实来源。

## 8. 投递与消费语义

MVP 采用 at-least-once delivery。

系统不承诺 exactly-once。原因是：

- worker 可能在执行成功后、ack 前崩溃。
- Redis pending 消息可能被重新 claim。
- 网络抖动可能导致状态更新和 ack 之间出现不一致。

降低重复执行影响的方式：

- 提交阶段使用 `idempotency_key`。
- worker 执行前使用数据库条件更新抢占任务。
- 只有合法状态可以进入 `running`。
- 已经是 `success` 或 `dead` 的任务被重复消费时直接跳过并 ack。
- 对外部副作用操作要求业务侧提供幂等能力。

## 9. 重试与退避

失败后根据 `retry_count` 和 `max_retries` 决定是否重试。

建议退避策略：

```text
delay = base_delay * 2^retry_count + jitter
```

示例：

- 第 1 次重试：5 秒。
- 第 2 次重试：30 秒。
- 第 3 次重试：2 分钟。
- 第 4 次重试：10 分钟。

达到最大重试次数后：

- 状态更新为 `dead`。
- 记录 `last_error`。
- 增加 dead metrics。
- 可选择写入 dead stream 供后续人工处理。

## 10. worker 崩溃恢复

Redis 层：

- worker 使用 consumer group 消费。
- 消息处理成功后必须 ack。
- 定期检查 pending entries。
- 超过 idle 时间的消息通过 `XAUTOCLAIM` 或 `XCLAIM` 转交给其他 worker。

Postgres 层：

- `running` 任务记录 `started_at` 和 `worker_id`。
- 超过任务超时时间仍未完成的任务可以被恢复流程标记为失败或重试。
- 状态更新必须使用条件更新，避免多个 worker 同时抢占。

恢复原则：

- Redis pending recovery 解决消息未 ack。
- Postgres running timeout recovery 解决任务状态卡住。
- 重复执行风险通过幂等和状态检查控制。

## 11. 并发控制与限流

MVP 先实现 worker 进程内并发控制：

- worker 启动时配置 `concurrency`。
- 每个 worker 进程最多同时执行 `concurrency` 个任务。
- 使用 goroutine pool 或 semaphore 控制并发。

后续可扩展：

- 按 `task_type` 限制并发。
- 按 `task_type` 限制速率。
- API 提交限流。

## 12. 任务超时

每个任务执行时必须创建 context：

```text
context.WithTimeout(parent, task.timeout)
```

超时后：

- 任务执行函数应尽快返回。
- worker 将任务标记为失败。
- 根据重试策略进入 `retrying` 或 `dead`。

注意：

- context 只能通知 Go 代码停止。
- 如果任务调用外部服务，外部服务也需要支持超时或取消。

## 13. graceful shutdown

worker 收到退出信号后：

- 停止拉取新消息。
- 等待正在执行的任务完成，直到达到 shutdown timeout。
- 对已完成任务更新状态并 ack。
- 对未完成任务不 ack，让 Redis pending recovery 接管。
- 关闭 Redis、Postgres 连接。

API 收到退出信号后：

- 停止接收新请求。
- 等待正在处理的请求完成。
- 关闭依赖连接。

## 14. Prometheus 指标

建议指标：

```text
gotaskqueue_tasks_submitted_total
gotaskqueue_tasks_started_total
gotaskqueue_tasks_completed_total
gotaskqueue_tasks_failed_total
gotaskqueue_tasks_retried_total
gotaskqueue_tasks_dead_total
gotaskqueue_task_execution_duration_seconds
gotaskqueue_task_wait_duration_seconds
gotaskqueue_queue_backlog
gotaskqueue_worker_running_tasks
```

这些指标需要支持按 `task_type` 和 `status` 维度观察。

## 15. 简单 dashboard

MVP dashboard 可以由 Go HTTP Server 提供，展示：

- 总任务数。
- 各状态任务数量。
- 当前队列积压。
- 当前 running 数量。
- 最近失败任务。
- 最近 dead 任务。
- 平均等待时间。
- 平均执行时间。

dashboard 只做只读展示，不做复杂管理后台。

## 16. 推荐实现顺序

1. 初始化 Go 项目结构。
2. 添加 Docker Compose 依赖：Redis、Postgres、Prometheus。
3. 建立配置、日志、数据库连接。
4. 建立任务数据模型和状态机。
5. 实现任务提交和状态查询 API。
6. 实现 Redis Stream 投递。
7. 实现 worker 消费和状态更新。
8. 实现任务超时。
9. 实现重试和退避。
10. 实现延迟任务 scheduler。
11. 实现死信处理。
12. 实现幂等提交。
13. 实现 pending recovery。
14. 添加 Prometheus metrics。
15. 添加简单 dashboard。
16. 添加测试和基础 CI。

## 17. MVP 成功标准

MVP 完成后应能演示：

- 提交一个立即任务并异步执行成功。
- 提交一个延迟任务，到期后执行。
- 任务失败后按退避策略重试。
- 重试耗尽后进入 dead。
- 重复提交相同幂等键不会创建重复任务。
- worker 并发执行多个任务。
- 任务超时后进入失败或重试。
- worker 崩溃后任务能被恢复处理。
- Prometheus 能看到关键指标。
- dashboard 能看到任务状态和队列积压。

## 18. 实习作品 v1.0 截止范围

本项目作为 Go 后端实习作品时，最终版本应定位为：

```text
GoTaskQueue v1.0

一个基于 Go + Redis Streams + Postgres 的异步任务队列中间件，
支持任务提交、延迟调度、幂等、重试、超时、死信、trace_id、metrics、dashboard、
worker 并发与批量消费，并提供一个真实业务 handler 示例。
```

v1.0 的目标不是继续扩展成完整网站或工作流平台，而是形成一个可运行、可演示、可解释设计取舍的后端基础设施项目。

### 18.1 v1.0 必须完成

- 完成并验收 Worker 批量消费 Redis Stream 消息：
  - 支持 `WORKER_BATCH_SIZE` 配置。
  - 每次轮询可读取多条 Redis Stream 消息。
  - 实际同时执行任务数量仍由 `WORKER_CONCURRENCY` 限制。
  - 批内单条任务失败不影响其他任务处理。
  - 现有 ack、失败、重试、dead stream 链路保持不变。
- 新增至少一个真实业务 handler 示例，建议优先实现 `webhook.deliver`：
  - payload 包含目标 URL、请求方法、headers 和 body。
  - 使用 Go HTTP client 调用外部服务。
  - 支持请求超时。
  - 5xx、网络错误和超时可进入重试。
  - 明确区分可重试错误和不可重试错误。
  - 使用 `httptest.Server` 补充 handler 单元测试。
- 补齐 handler payload 校验：
  - handler 负责将 JSON payload 解析为明确结构体。
  - 缺少必要字段时返回清晰错误。
  - 无效 payload 不应导致 panic。
- 完成 v1.0 演示文档：
  - 写清楚项目定位：异步任务队列中间件 / 后端基础设施组件。
  - 提供一条完整演示路径：启动依赖、执行 migration、启动应用、提交任务、查看 dashboard、查看 metrics、制造失败任务。
  - 说明 dashboard 是只读运维页面，不是完整网站。
  - 说明 Prometheus 页面属于演示的一部分。
- 完成 v1.0 验收：
  - `make check` 通过。
  - `make integration-test` 通过，或明确记录无法运行原因。
  - README、`.env.example`、`docs/status.md`、`docs/log.md` 与最终能力一致。

### 18.2 v1.0 演示形态

v1.0 应按后端中间件项目演示，而不是按网站项目演示。

演示组成：

- 命令行 API 演示：
  - 提交立即任务。
  - 查询任务状态。
  - 提交延迟任务。
  - 提交失败任务并观察 retry / dead。
  - 提交真实业务 handler 任务，例如 webhook delivery。
- Dashboard 演示：
  - `/dashboard` 任务总览页。
  - `/dashboard/tasks/{id}` 任务详情页。
- 监控演示：
  - `/metrics` 原始指标。
  - Prometheus 查询页面。

### 18.3 v1.0 明确不做

- 不做 RabbitMQ、NATS、Kafka 多后端抽象。
- 不做复杂 Web 管理后台。
- 不做完整网站或 SaaS 产品。
- 不做用户登录、权限系统或多租户。
- 不做 DAG 工作流。
- 不做 Kubernetes 部署。
- 不承诺 exactly-once delivery。
- 不为了展示而引入过度封装。

### 18.4 v1.0 完成后停止扩展

达到 v1.0 后，项目应停止继续堆功能，将后续方向记录到 Future Work。实习投递版本应强调：

- 核心异步任务链路完整。
- 状态机、重试、死信、恢复路径清晰。
- Go 并发、context、graceful shutdown 使用合理。
- Redis 和 Postgres 的职责边界清楚。
- 可观测性、测试、文档和本地演示闭环完整。
