# Raft Job Keeper

Raft Job Keeper 是基于 Java 21、Spring Boot 和 SOFAJRaft 的多模块任务调度项目。Server 通过 Raft 复制状态，只有 Leader 分发任务；独立运行的 Worker 通过 gRPC 双向流连接 Server 并执行任务。

## 项目结构

- [job-keeper-common](job-keeper-common/README.md)：共享领域模型、Raft 命令和 gRPC 协议。
- [job-keeper-server](job-keeper-server/README.md)：REST API、Raft 状态机、任务分发、租约和快照。
- [job-keeper-worker](job-keeper-worker/README.md)：Worker 运行时以及 shell、HTTP 执行器。
- [deploy](deploy/)：容器和 Kubernetes 部署资源。

## 本地测试环境（Windows PowerShell）

以下命令均从仓库根目录运行。需要 JDK 21、Maven 3.9 或更高版本，以及可用的 7000、8080、9090 端口。首次构建需要联网下载 Maven 依赖。确保 `java -version` 和 `mvn -version` 都显示 Java 21；如果安装了多个 JDK，可以在每个终端中设置：

```powershell
$env:JAVA_HOME = 'C:\path\to\jdk-21' # 改为实际安装路径
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
mvn -version
```

### 1. 构建并运行自动化测试

```powershell
mvn -B verify
```

成功后会生成 `job-keeper-server/target/job-keeper-server.jar` 和 `job-keeper-worker/target/job-keeper-worker.jar`。

### 2. 启动单节点 Server

在第一个 PowerShell 终端中执行，保持窗口运行：

```powershell
$env:API_TOKEN = 'dev-token'
$env:WORKER_TOKEN = 'dev-token'
java --add-opens=java.base/java.util=ALL-UNNAMED -jar job-keeper-server/target/job-keeper-server.jar --raft.data-dir=./data --raft.self-address=localhost:7000 --raft.initial-peers=localhost:7000
```

`./data` 保存本地 Raft 日志和快照。等待节点完成 Leader 选举后，在另一个终端检查：

```powershell
Invoke-RestMethod http://localhost:8080/healthz
Invoke-RestMethod http://localhost:8080/readyz
Invoke-RestMethod http://localhost:8080/leaderz
```

### 3. 启动 Worker

在第二个 PowerShell 终端中执行，保持窗口运行：

```powershell
$env:WORKER_ID = 'worker-1'
$env:SERVER_ADDR = 'localhost:9090'
$env:WORKER_TOKEN = 'dev-token'
java -jar job-keeper-worker/target/job-keeper-worker.jar
```

Worker 的令牌必须与 Server 的 `WORKER_TOKEN` 一致。等待 Worker 连接后，打开第三个终端执行下面的端到端测试。

### 4. 创建并运行任务

```powershell
$headers = @{ Authorization = 'Bearer dev-token' }
$body = @{
    name = 'local-example'
    payload = @{ executor = 'shell'; command = 'cmd.exe'; args = @('/c', 'echo hello') }
} | ConvertTo-Json -Depth 5
$job = Invoke-RestMethod -Method Post -Uri http://localhost:8080/jobs -Headers $headers -ContentType 'application/json' -Body $body
$job
$runHeaders = @{ Authorization = 'Bearer dev-token'; 'Idempotency-Key' = [guid]::NewGuid().ToString() }
$task = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/jobs/$($job.id)/run" -Headers $runHeaders
$task
Invoke-RestMethod "http://localhost:8080/tasks/$($task.id)"
```

任务是异步执行的；如果状态仍为 `PENDING` 或 `RUNNING`，稍后再次查询。成功时状态为 `SUCCEEDED`，结果应包含 `hello`。也可以用 `Invoke-RestMethod http://localhost:8080/tasks` 查看所有任务。重试同一次触发请求时复用其 `Idempotency-Key`，避免创建重复任务；新的运行使用新 key。

使用 Ctrl+C 分别停止 Worker 和 Server。需要全新状态时，先停止 Server，再手动删除本地 `data` 目录；这会删除其中的任务和 Raft 数据。若端口冲突，请检查占用或调整 Server 配置与 Worker 地址。

## 配置

| 配置 / 环境变量 | 默认值 | 用途 |
|---|---|---|
| `raft.data-dir` / `RAFT_DATA_DIR` | `/data` | Raft 持久化目录 |
| `raft.self-address` / `ADVERTISE_ADDR` | `localhost:7000` | 本节点 Raft 地址 |
| `raft.initial-peers` / `RAFT_INITIAL_PEERS` | `localhost:7000` | 逗号分隔的初始节点地址 |
| `scheduler.lease-duration` | `30s` | 任务租约时长 |
| `grpc.port` | `9090` | Worker gRPC 端口 |
| `API_TOKEN` | 未设置 | REST 写入和管理接口的 Bearer 令牌 |
| `WORKER_TOKEN` | 回退到 `API_TOKEN` | Worker 注册令牌 |
| `WORKER_ID` | 自动生成 | Worker 标识 |
| `SERVER_ADDR` | `localhost:9090` | Worker 连接的 gRPC 地址 |
| `WORKER_CAPACITY` | `4` | Worker 最大并发任务数 |

多节点部署时，各 Server 使用相同的初始节点列表、稳定且各不相同的 Raft 地址及独立持久卷，节点数量建议为奇数。Kubernetes 部署需要预先创建包含 `api-token` 和 `worker-token` 的 `raft-job-keeper-auth` Secret。

`shell` 执行器使用 `ProcessBuilder` 直接启动命令，不会自动调用 shell；上面的 Windows 示例显式运行 `cmd.exe`。创建任务可以执行任意本机命令，应严格保护 API 令牌并使用可信 Worker。容器执行器尚未实现。

## API 概览

- Job：`POST /jobs`、`GET /jobs`、`GET /jobs/{id}`、`PUT /jobs/{id}`、`DELETE /jobs/{id}`，以及 `POST /jobs/{id}/enable`、`disable`、`run`。
- Task：`GET /tasks`、`GET /tasks/{id}`。
- 健康与监控：`GET /healthz`、`/readyz`、`/leaderz`、`/metrics`。
- 管理：`GET /admin/cluster/status`、`POST /admin/snapshot`、`/admin/cluster/join`、`/admin/cluster/leave`、`/admin/transfer-leadership`。

Job 和 Task 读取默认使用强一致性；可添加 `?consistency=stale` 读取节点本地状态。写入及管理请求需要 Bearer 令牌。
