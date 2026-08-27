基于 Go 实现的 taskflow 项目，一款后端服务，完成延迟任务的提交、分桶排队、租约派发、失败重试与完成回调。

taskflow 是一个面向业务系统的分布式延迟任务调度服务。业务方通过 HTTP API 提交"延迟执行"或"定时执行"任务，服务将任务写入预写日志并按时间分桶入队，调度器扫描到期桶并把任务派发给工作协程执行，支持租约防重、失败重试与完成回调。

## 构建

```bash
go build -mod=vendor ./...
```

## 运行

```bash
go run -mod=vendor ./cmd/taskflowd -config config.json
```

默认监听 `:8080`，健康检查接口为 `GET /healthz`。

## 主要接口

- `POST /api/v1/tasks` 提交单个任务
- `POST /api/v1/tasks/batch` 批量提交任务
- `GET /api/v1/tasks/{id}` 查询任务
- `POST /api/v1/tasks/{id}/cancel` 取消任务

任务类型由服务端注册，内置 `echo`、`noop`、`fail-once` 三类处理器。
