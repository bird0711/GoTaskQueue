# Next Task

实现简单只读 dashboard。

范围：
- 在 Go HTTP Server 中提供 dashboard 页面。
- 展示任务总数和各状态任务数量。
- 展示当前队列积压和 running 任务数量。
- 展示最近失败任务和最近 dead 任务。
- 保持 dashboard 只读，不提供任务管理操作。
- 不实现复杂前端框架、权限、分页、筛选、CI。

完成标准：
- 浏览器访问 dashboard 路径可以看到任务概览。
- 页面数据来自 Postgres 和现有 metrics/snapshot 逻辑。
- dashboard 不影响现有 API、worker、scheduler、metrics。
- `go test ./...` 可以运行通过。