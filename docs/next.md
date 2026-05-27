# Next Task

实现 dead stream（mvp.md §7 标为 optional 的 `tasks:dead` Redis Stream）。

范围：
- 在 `internal/config/config.go` 中为 `RedisConfig` 新增 `DeadStreamName` 字段，环境变量 `REDIS_DEAD_STREAM_NAME`，默认 `tasks:dead`；`.env.example` 同步加入占位。
- 在 `internal/queue` 中新增 `RedisDeadStreamPublisher`（或扩展现有 publisher）：`PublishDead(ctx, message DeadTaskMessage) error`，写入字段 `task_id`、`task_type`、`trace_id`、`last_error`、`retry_count`（缺失字段时跳过写入而非写空串，以便消费者能区分"未提供"和"空值"）。
- 不在死信流中写入完整 `payload`，消费方需通过 `task_id` 回查 Postgres。
- `internal/worker/worker.go` 在 `handleExecutionFailure` 的 `decision.Status == StatusDead` 分支调用 `PublishDead`；publish 失败仅记 `logger.Warn`，不影响任务在 Postgres 的 `dead` 终态、不影响 stream ack、不返回错误。
- `internal/scheduler/scheduler.go` 在 `recoverRunningTask` 的 dead 分支同样调用 `PublishDead`，失败处理同上。
- `internal/app/app.go` 装配 `RedisDeadStreamPublisher` 并通过 `WithDeadPublisher`（或类似）链式方法注入 worker / scheduler。
- 单元测试：worker 与 scheduler 在任务进入 dead 时调用 `PublishDead` 并传入正确字段；publish 失败时不影响后续状态。
- 集成测试：在现有 `TestUnknownTaskTypeFailsToDead` 中追加断言——任务进入 dead 后，`tasks:dead` stream `XLEN` ≥ 1，最近一条消息的 `task_id` 与该任务一致、`trace_id` 与提交响应一致、`last_error` 含 `no handler registered`。
- 不引入第三方依赖；不修改 metrics 名称与维度；不修改任务状态机；不实现死信重投或归档（dashboard requeue 留作后续）。

完成标准：
- 任务进入 `dead` 状态时，`tasks:dead` Redis Stream 收到一条消息，包含 `task_id`、`task_type`、`trace_id`、`last_error`、`retry_count`。
- `tasks:dead` 流名可通过环境变量配置，默认 `tasks:dead`。
- 死信流写入失败时任务仍正确进入 `dead` 终态、stream ack 正常、应用日志只记 `warn`。
- 现有所有单元测试和集成测试继续通过。
- `make check` 通过。
- `make integration-test` 通过。
