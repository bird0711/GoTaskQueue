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
- 创建立即任务时会投递消息到 Redis Stream；创建延迟任务时会写入 Postgres 并写入 Redis ZSET `tasks:scheduled`。
- 任务提交支持 `idempotency_key` 幂等语义；重复提交会返回已有任务，不会新增任务记录或重复投递 Redis Stream。
- 已实现最小 Worker，可消费 Redis Stream 并将任务按 `pending -> running -> success` 更新。
- 已实现最小 Scheduler，可从 Redis ZSET 扫描到期延迟任务，通过 Postgres 条件状态更新确认后投递到 Redis Stream。
- Worker 执行失败时会进入失败处理链路，写入 `last_error`、更新 `retry_count`，并转为 `retrying` 或 `dead`。
- Scheduler 会扫描到期 `scheduled` 和 `retrying` 任务，并统一转为 `pending` 后投递 Redis Stream。
- Worker 执行任务时会按任务的 `timeout_seconds` 设置执行超时，超时按失败处理。
- Worker 会周期检查 Redis Streams pending 消息，claim 超过 idle 时间的消息并复用现有执行链路处理；终态任务重复消息会直接 ack。
- Scheduler 会周期恢复超过 `started_at + timeout_seconds` 的 running 任务，将其接入失败处理链路并转为 `retrying` 或 `dead`。
- Worker 已支持按 `task_type` 注册和分发真实 task handler。
- 应用启动时已注册示例 handler `demo.echo`；未注册的 `task_type` 会返回明确错误并进入现有失败/重试/死信链路。
- Go HTTP Server 已暴露 `/metrics`，提供 `gotaskqueue_*` 前缀的基础 Prometheus 指标。
- 核心任务生命周期已接入提交、开始、成功、失败、重试、死信计数，以及任务执行耗时和等待耗时指标。
- `/metrics` 会基于 Postgres 当前状态提供队列积压和 running 任务数量 gauge。
- Go HTTP Server 已提供只读 dashboard，可展示任务总数、各状态数量、队列积压、running 数量、最近任务、最近失败任务和最近 dead 任务。
- dashboard 已提供只读任务详情页，可查看任务 payload、错误、重试次数、运行时间等字段，并通过任务 ID 链接进入。
- 已添加基础本地验证入口 `make check`，会运行 `go test ./...`、`go vet ./...` 和 `golangci-lint run ./...`。
- 已添加集成测试入口 `make integration-test`，使用本地 Docker Compose 依赖验证 API、Postgres、Redis Stream、Redis ZSET、Worker、Scheduler 的协作链路。
- Makefile 已提供 `make up`、`make down`、`make migrate-up`、`make run`、`make check` 常用命令。
- 已添加 GitHub Actions CI，在 push 和 pull request 时运行 `make check`。
- 已添加 MVP 手动验收和演示文档：`docs/manual-acceptance.md`。
- MVP 收尾验收已通过：`make check`、立即任务、短延迟任务、dashboard、应用 metrics、Prometheus 查询均验证通过。
- `make migrate-up` 已支持 `schema_migrations` 版本记录，会按文件顺序执行未执行的 `migrations/*.up.sql` 并跳过已记录版本。
- 第二版最终验收已通过：`make check`、幂等提交、Redis ZSET 延迟任务、running timeout recovery、重复 `make migrate-up`、dashboard、应用 metrics 和 Prometheus 查询均验证通过。
- Worker 已支持进程内并发执行；通过 `WORKER_CONCURRENCY`（默认 4）控制同时执行的任务数量，使用信号量 + `sync.WaitGroup`，`Run` 在 ctx 取消后等待所有 in-flight goroutine 退出再返回。
- `internal/app/app.go` 已补全 graceful shutdown；shutdown 阶段会等待 server / worker / scheduler 三个 goroutine 全部退出（或达到 10 秒上限）后再触发 `deps.Close`，相关逻辑提取为 `waitForRunner` helper 并有单元测试覆盖正常退出、`http.ErrServerClosed`、超时三条路径。
- `tasks` 表新增可空 `trace_id TEXT` 列（migration `000002_add_trace_id`）；trace_id 在任务创建时由 service 自动生成（`trace_` 前缀 hex），调用方可通过请求体覆盖；scheduler 重试投递时复用 Postgres 中已存的 trace_id；Redis Stream 消息体包含 trace_id；worker 任务生命周期日志带上 trace_id；HTTP API 请求/响应都新增可选 `trace_id` 字段。
- 已启用 `tasks:dead` Redis Stream（流名通过 `REDIS_DEAD_STREAM_NAME` 配置，默认 `tasks:dead`）；任务进入 `dead` 状态时 worker 与 scheduler 失败链路写入死信流，消息体含 `task_id`、`task_type`、`trace_id`、`last_error`、`retry_count`，不含完整 payload；publish 失败仅记 warn，不影响任务终态、stream ack 或 metrics。

## 已知问题

- 暂无。
