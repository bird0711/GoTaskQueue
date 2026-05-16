# Log

- 2026-05-13：完成 Go 项目初始化，创建 `go.mod`、最小入口程序和基础目录结构；当前实现仅包含空壳应用装配与健康检查 HTTP 服务。
- 2026-05-13：添加本地开发依赖环境，创建 Docker Compose 配置、Prometheus 配置和 Go 依赖连接配置占位。
- 2026-05-13：建立应用配置、基础日志和依赖连接生命周期；启动时检查 Redis/Postgres 连通性，关闭时释放连接。
- 2026-05-13：建立任务状态定义、领域模型和 `tasks` 表初始 migration；状态流转规则集中在 `internal/task`。
- 2026-05-13：实现任务提交 API 和任务状态查询 API；当前仅写入/读取 Postgres，不投递 Redis Stream。
- 2026-05-13：实现立即任务 Redis Stream 投递；延迟任务仍等待后续 scheduler 处理。
- 2026-05-13：实现最小 Worker 消费 Redis Stream，任务成功路径更新为 `pending -> running -> success` 并 ack 消息。
- 2026-05-13：实现最小 Scheduler 扫描到期延迟任务，按 `scheduled -> pending` 转换后投递 Redis Stream。
- 2026-05-14：实现 Worker 失败处理基础链路，失败任务会转为 `failed` 后进入 `retrying` 或 `dead`，并记录错误和重试次数。
- 2026-05-14：扩展 Scheduler 处理到期 `retrying` 任务，统一通过 `retrying -> pending` 后重新投递 Redis Stream。
- 2026-05-14：实现 Worker 任务执行超时处理，超时任务进入现有失败处理链路并记录超时原因。
- 2026-05-16：实现 Redis Streams pending recovery；Worker 会周期 claim 超过 idle 时间的 pending 消息，并对终态任务重复消息直接 ack。
- 2026-05-16：实现 Prometheus `/metrics` 暴露和基础 `gotaskqueue_*` 指标，覆盖任务生命周期计数、耗时 summary、队列积压和 running 数量 gauge。
- 2026-05-16：实现简单只读 dashboard；HTTP 根路径和 `/dashboard` 展示任务概览、状态计数、队列/running 快照及最近失败/dead 任务。
