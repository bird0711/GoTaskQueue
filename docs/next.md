# Next Task

验收 dashboard 查询索引和模板拆分改动。

范围：
- 运行 `make check`，确认单元测试、vet、golangci-lint 通过。
- 运行 `make migrate-up`，确认 `000003_add_dashboard_query_indexes` 可执行。
- 重复运行 `make migrate-up`，确认已记录 migration 会被跳过。
- 手动访问 `/dashboard` 和任务详情页，确认模板拆分后页面仍正常渲染。
- 检查 `docs/status.md` 和 `docs/log.md` 是否已记录本次改动。

非目标：
- 不新增业务功能。

完成标准：
- `make check` 通过。
- `make migrate-up` 首次/重复执行行为正常。
- dashboard 页面正常。
- 任务详情页正常。
- 文档记录准确。
