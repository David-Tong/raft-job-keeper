# job-keeper-worker

`job-keeper-worker` 是独立运行的任务执行进程。它通过双向 gRPC 流连接 Server
Leader，注册自身容量，接收任务、发送心跳并回报执行结果。

## 实现分析

### 连接和任务生命周期

`JobKeeperWorkerApplication` 启动时调用 `WorkerRuntime.start()`，退出时停止运行时。
`WorkerRuntime` 建立 gRPC 流并发送注册消息；收到 Leader 重定向或连接中断后会重新
连接，失败重试间隔从 1 秒指数增长，最大为 30 秒。

运行时每 5 秒发送心跳，并使用固定大小线程池执行任务。接收任务时会检查 Leader
term、重复任务和容量上限；收到取消消息时尝试中断任务。执行完成后回报状态和结果，
每个任务最多回报一次。关闭时先停止心跳和任务接收，再等待执行线程最多 30 秒。

### Executor

`Executor` 定义统一的任务执行接口；Spring 会注入实现并按 `type()` 建立执行器表。

- `ShellExecutor`：通过 `ProcessBuilder` 直接运行 `command` 和 `args`，不会自动套用
  shell。`env` 会加入进程环境；执行超时会强制终止进程。标准输出与错误合并写入
  临时文件，结果最多保留末尾 64 KiB，临时文件在结束后删除。
- `HttpExecutor`：将 `command` 解析为 URL。`args` 的第一个元素若为 GET、POST、PUT、
  PATCH 或 DELETE 就作为请求方法，否则默认 POST；余下参数以空格连接为请求体。
  `env` 中以 `header.` 开头的键会转换为 HTTP 请求头。GET/DELETE 不发送请求体，
  仅 2xx 响应视为成功，响应结果最多保留 64 KiB。
- `TaskResult`：统一表达执行器返回的状态与输出。

执行器不包含容器执行实现。创建 Job 和配置执行命令属于高权限操作，应限制 API
写入权限，并只向可信任的 Worker 分发任务。

## 配置

配置通过环境变量读取：

| 环境变量 | 默认值 | 用途 |
|---|---|---|
| `WORKER_ID` | `${HOSTNAME:worker}-${random.uuid}` | Worker 标识 |
| `SERVER_ADDR` | `localhost:9090` | Server gRPC 地址；连接后可由服务端重定向 |
| `WORKER_TOKEN` | 回退到 `API_TOKEN`，再回退为空 | Worker 注册凭据 |
| `WORKER_CAPACITY` | `4` | 最大并行任务数，必须大于 0 |

Worker token 必须与 Server 配置一致。生产环境应通过安全的密钥管理方式注入凭据；
不要将令牌写入代码或提交到版本控制。

## 构建、运行与测试

从仓库根目录构建本模块及共享依赖，并运行测试：

```shell
mvn -pl job-keeper-worker -am test
mvn -pl job-keeper-worker -am package
```

可执行 JAR 为 `target/job-keeper-worker.jar`。例如，在本地 Server 已监听
`localhost:9090` 时启动：

```shell
WORKER_ID=worker-1 SERVER_ADDR=localhost:9090 WORKER_TOKEN=dev-token \
  java -jar target/job-keeper-worker.jar
```

当前 Worker 测试覆盖 HTTP 执行器的默认 POST、GET 请求、请求头和非 2xx 错误响应。
完整系统启动方式和部署资源见仓库根目录 [`README.md`](../README.md) 与
[`deploy/`](../deploy/)。
