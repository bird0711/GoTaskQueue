# Decisions

## 已确认的 MVP 技术决策

- 本项目定位为 Go 分布式任务调度与异步队列系统。
- MVP 优先实现单一可靠链路，不做多队列后端抽象。
- MVP 队列后端选择 Redis Streams。
- 延迟任务使用 Redis ZSET。
- 任务元数据和状态使用 Postgres 持久化。
- 本地依赖使用 Docker Compose 管理。
- Go 应用在早期开发阶段本地直接运行，不强制放入 Docker。
- 监控使用 Prometheus metrics。
- dashboard 使用 Go HTTP Server 提供简单只读页面。
- Redis 客户端使用 `github.com/redis/go-redis/v9`。
- Postgres 客户端和连接池使用 `github.com/jackc/pgx/v5/pgxpool`。
- 基础日志使用 Go 标准库 `log/slog`。
- 初始 migration 使用顺序编号 SQL 文件，放在 `migrations/` 目录。
- 任务 ID 在应用层生成并以 `TEXT` 存储，MVP 初始 schema 不依赖数据库 UUID 扩展。
- 任务 payload 使用 Postgres `JSONB` 存储。
- Redis Stream 默认名称为 `tasks:stream`，可通过 `REDIS_STREAM_NAME` 配置。
- Redis consumer group 默认名称为 `gotaskqueue-workers`，可通过 `REDIS_CONSUMER_GROUP` 配置。
- Scheduler 默认每 2 秒扫描一次到期延迟任务，批量上限默认为 100。
- 最小重试退避使用指数秒级延迟：第 1 次重试 1 秒，第 2 次 2 秒，最多封顶 32 秒。
- 任务投递语义采用 at-least-once delivery。
- 系统不承诺 exactly-once delivery。
- 幂等通过 `idempotency_key`、数据库唯一约束和状态检查实现。
- worker 崩溃恢复通过 Redis Streams pending recovery 和 Postgres running timeout recovery 共同处理。
- MVP 暂不实现 RabbitMQ、NATS、Kafka 后端。
- MVP 暂不实现复杂 Web 管理后台、DAG 工作流、多租户和生产级分片调度。
- `tasks` 表新增可空 `trace_id TEXT` 列用于日志关联；trace_id 在任务首次创建时生成（若调用方未提供），后续重试不会改变 trace_id；不在 trace_id 上建索引。
- Redis Stream 消息体新增 `trace_id` 字段；scheduler 投递重试任务时复用 Postgres 中已存的 trace_id。
- HTTP API 创建任务请求体新增可选 `trace_id` 字段，返回体新增 `trace_id` 字段；外部调用方可传入自有 trace_id 以贯通跨服务追踪链路。
- 启用 mvp.md §7 标为 optional 的 `tasks:dead` Redis Stream；任务进入 `dead` 状态时由 worker / scheduler 失败链路写入死信流，消息体包含 `task_id`、`task_type`、`trace_id`、`last_error`、`retry_count`，不包含完整 payload（payload 可由消费方按 task_id 回查 Postgres）。
- 死信流名通过 `REDIS_DEAD_STREAM_NAME` 配置，默认 `tasks:dead`。
- 死信流写入失败仅记 warn 日志，不影响任务在 Postgres 的 `dead` 状态终态；at-least-once 语义不扩展到死信流，运维侧消费者需要自行处理重复消息。

## 设计方案入口

- 完整 MVP 方案见 `docs/mvp.md`。
