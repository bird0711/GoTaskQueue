# Next Task

执行 MVP 3.0 阶段验收和封版记录。

范围：
- 运行 `make check`。
- 运行 `make integration-test`。
- 手动验证 dashboard 最近任务列表和任务详情页。
- 手动验证 `trace_id` 在 API 响应、Redis Stream 消息和日志中贯通。
- 手动验证未知 `task_type` 进入 dead 后写入 `tasks:dead`。
- 验证 worker 并发配置 `WORKER_CONCURRENCY` 不影响正常执行。
- 验证 graceful shutdown 能等待 worker / scheduler / server 正常退出。
- 更新 `docs/status.md`，明确 MVP 3.0 当前阶段验收完成。
- 更新 `docs/log.md`，记录本次验收结果。
- 不新增业务功能。

完成标准：
- `make check` 通过。
- `make integration-test` 通过。
- dashboard 任务详情页可用。
- trace_id 链路可验证。
- dead stream 可验证。
- graceful shutdown 可验证。
- `docs/status.md` 和 `docs/log.md` 已记录 MVP 3.0 验收结果。
