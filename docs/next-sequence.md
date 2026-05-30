# Next Task Sequence

本文件记录 v1.0 前建议依次执行的任务。执行时应一次只复制其中一个任务到 `docs/next.md`。



# Next Task

验收并收尾 Worker 批量消费 Redis Stream 消息。

范围：
- 检查当前批量消费实现是否符合设计：批量读取、并发上限、单条失败隔离、终态重复消息 ack。
- 确认 `WORKER_BATCH_SIZE` 配置已接入 config、app wiring、README 和 `.env.example`。
- 运行 `make check`。
- 运行 `make integration-test`。
- 根据验证结果修复批量消费实现或测试。
- 更新 `docs/status.md` 和 `docs/log.md`，记录本次收尾结果。

非目标：
- 不切换到 `XAUTOCLAIM`。
- 不改变 HTTP API。
- 不改变任务状态机。
- 不新增业务 handler。

完成标准：
- Worker 可一次读取并调度多条 Redis Stream 消息。
- `WORKER_CONCURRENCY` 仍限制实际同时执行任务数量。
- 单条任务失败不影响同批其他任务处理。
- `make check` 通过。
- `make integration-test` 通过，或明确记录无法运行原因。
- 文档与配置一致。



# Next Task

实现真实业务 handler 示例 `webhook.deliver`。

范围：
- 新增 `webhook.deliver` task handler。
- 定义 webhook payload 结构，至少包含 `url`、`method`、`headers`、`body`。
- 使用 Go HTTP client 发起请求。
- 支持 handler 内部请求超时。
- 2xx 视为成功。
- 5xx、网络错误、超时返回可重试错误。
- 4xx 返回不可重试错误。
- 在应用启动时注册 `webhook.deliver`。
- 使用 `httptest.Server` 补充 handler 单元测试。
- 更新 README 的 API 示例和演示路径。

非目标：
- 不实现真实外部平台集成。
- 不增加认证系统。
- 不改变任务提交 API。
- 不引入新依赖。

完成标准：
- `webhook.deliver` 可通过 `POST /tasks` 提交并由 worker 执行。
- 成功 webhook 任务进入 `success`。
- 5xx / timeout 场景进入现有 retry / dead 链路。
- 4xx 场景返回明确错误。
- 单元测试覆盖成功、4xx、5xx、timeout、payload invalid。
- `make check` 通过。



# Next Task

补齐 handler payload 校验和错误分类。

范围：
- 为现有 handler 定义明确 payload 解析和校验逻辑。
- 确保无效 JSON、缺少必要字段、字段类型错误时返回清晰错误。
- 区分可重试错误和不可重试错误。
- 确认不可重试错误不会产生无意义重试，或在文档中明确当前限制。
- 补充 worker / handler 单元测试。
- 更新 README 或 `docs/mvp.md` 中关于 handler 设计的说明。

非目标：
- 不引入第三方 validator。
- 不改变 HTTP API 请求格式。
- 不改变全局任务状态机。
- 不实现复杂错误码系统。

完成标准：
- handler 对非法 payload 不 panic。
- handler 错误信息可读。
- 可重试和不可重试错误行为明确。
- 测试覆盖 payload invalid、retryable error、non-retryable error。
- `make check` 通过。



# Next Task

整理 v1.0 演示文档。

范围：
- 更新 README，明确项目定位为 Go 异步任务队列中间件 / 后端基础设施组件。
- 增加 v1.0 演示路径：
  - 启动依赖。
  - 执行 migration。
  - 启动应用。
  - 提交立即任务。
  - 提交延迟任务。
  - 提交 `webhook.deliver` 任务。
  - 制造失败任务并观察 retry / dead。
  - 打开 `/dashboard`。
  - 打开任务详情页。
  - 查看 `/metrics` 和 Prometheus。
- 明确 dashboard 是只读运维页面，不是完整网站。
- 增加面试讲解要点：at-least-once、Postgres source of truth、Redis Stream pending recovery、幂等、死信、graceful shutdown。
- 检查 `docs/mvp.md`、`docs/status.md`、`docs/log.md` 与 README 是否一致。

非目标：
- 不新增功能。
- 不重写项目结构。
- 不美化 dashboard UI。

完成标准：
- README 能独立指导他人完成本地演示。
- v1.0 定位和演示路径清楚。
- 文档之间无明显冲突。
- `make check` 通过。



# Next Task

执行 GoTaskQueue v1.0 最终验收。

范围：
- 从干净环境或当前本地环境执行完整演示流程。
- 运行 `make up`。
- 运行 `make migrate-up`。
- 运行 `make check`。
- 运行 `make integration-test`。
- 启动应用并提交：
  - 立即任务。
  - 延迟任务。
  - `webhook.deliver` 成功任务。
  - `webhook.deliver` 失败任务。
  - unknown task type dead 任务。
- 验证 `/dashboard` 和任务详情页。
- 验证 `/metrics` 和 Prometheus 查询。
- 检查 dead stream 中是否能看到 dead 任务。
- 更新 `docs/status.md` 和 `docs/log.md`，记录 v1.0 验收结果。
- 清理 `docs/next.md` 或写入“暂无下一任务”。

非目标：
- 不新增功能。
- 不做架构重构。
- 不继续扩展 v1.0 范围外能力。

完成标准：
- `make check` 通过。
- `make integration-test` 通过。
- v1.0 演示路径完整可跑。
- README 与实际行为一致。
- `docs/status.md` 记录 v1.0 已完成。
- `docs/log.md` 记录最终验收结果。
