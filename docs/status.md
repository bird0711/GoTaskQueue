# Status

## 当前状态

- 仓库已初始化 Git。
- 项目已从纯设计阶段进入最小骨架实现阶段。
- 已创建 Go 应用代码基础骨架。
- 已初始化 Go module：`github.com/bird0711/GoTaskQueue`。
- 已创建 Docker Compose 本地依赖配置，包含 Redis、Postgres、Prometheus。
- 已建立 AI 控制文档：
  - `AGENTS.md`
  - `docs/rules.md`
  - `docs/status.md`
  - `docs/decisions.md`
  - `docs/next.md`
  - `docs/log.md`
- 已创建 MVP 方案文档：`docs/mvp.md`。
- 已提供最小可运行入口程序和基础 HTTP 健康检查端点。
- Go 配置中已预留 Redis、Postgres、Prometheus 连接配置项。
- 应用启动时会初始化 Redis 和 Postgres 连接并执行连通性检查。
- 应用支持基础结构化日志输出和关闭时释放依赖连接。
- 已定义任务领域模型、核心状态和统一状态流转规则。
- 已创建 `tasks` 表初始 migration。
- 已提供任务提交 API 和按 ID 查询任务 API，当前只写入/读取 Postgres。
- 创建立即任务时会投递消息到 Redis Stream；延迟任务仍只写入数据库。
- 已实现最小 Worker，可消费 Redis Stream 并将任务按 `pending -> running -> success` 更新。
- 已实现最小 Scheduler，可扫描到期延迟任务并投递到 Redis Stream。
- Worker 执行失败时会进入失败处理链路，写入 `last_error`、更新 `retry_count`，并转为 `retrying` 或 `dead`。
- Scheduler 会扫描到期 `scheduled` 和 `retrying` 任务，并统一转为 `pending` 后投递 Redis Stream。
- Worker 执行任务时会按任务的 `timeout_seconds` 设置执行超时，超时按失败处理。
- Worker 会周期检查 Redis Streams pending 消息，claim 超过 idle 时间的消息并复用现有执行链路处理；终态任务重复消息会直接 ack。
- Go HTTP Server 已暴露 `/metrics`，提供 `gotaskqueue_*` 前缀的基础 Prometheus 指标。
- 核心任务生命周期已接入提交、开始、成功、失败、重试、死信计数，以及任务执行耗时和等待耗时指标。
- `/metrics` 会基于 Postgres 当前状态提供队列积压和 running 任务数量 gauge。
- Go HTTP Server 已提供只读 dashboard，可展示任务总数、各状态数量、队列积压、running 数量、最近失败任务和最近 dead 任务。

## 已知问题

- 项目暂无 lint 或 CI 配置。
