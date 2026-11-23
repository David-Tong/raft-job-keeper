# job-keeper-server 源码导读

这个模块负责接收 Job 操作、通过 Raft 复制数据、向 Worker 派发 Task，并记录执行结果。本文按“先看一个最小例子，再沿执行路径展开”的顺序介绍，适合在 IDE 中边读边跳转。

文中的文件路径以仓库根目录为起点；命令也从仓库根目录执行。运行示例使用 PowerShell 7.4+。

## 1. 先分清几个名字

| 名称 | 在这个项目中的意思 |
|---|---|
| `Job` | 任务定义，描述执行内容、超时、重试和并发策略 |
| 业务 `Task` | 一次触发生成的执行记录；重试时沿用 Task ID，增加 `attempt` |
| `Worker` | 独立的执行进程，通过 gRPC 接收任务和上报结果 |
| `Command` | 一次数据变更请求，例如创建 Job、分配 Task、记录结果 |
| `JobStore` | 在内存 Map 中保存 Job、Task、Worker 和幂等请求结果，执行具体业务规则 |
| `JobStateMachine` | Raft 已提交命令的应用入口：`onApply()` 解析命令，再调用 `JobStore.apply()` |

这里说的“Raft 状态机”具体指 `JobStateMachine` 类。它与 Task 的 `PENDING → RUNNING → SUCCEEDED` 状态变化是两个概念；后者的转换规则写在 `JobStore` 中。

每个 Server 都有自己的 Store，并按相同的 Raft 日志顺序更新数据；Follower 的应用进度可能暂时落后。只有 Leader 接受业务写入和派发任务。

创建 Job 只保存定义，调用 `/run` 才会创建 Task。当前没有 Cron 表达式解析和定时触发 Job 的实现，定时扫描负责处理已经存在的 Task。

## 2. 源码在哪里，按什么顺序读

服务端 Java 源码根目录为：

```text
job-keeper-server/src/main/java/io/ytong/raftjobkeeper/server/
```

| 顺序 | 文件（完整仓库相对路径，可点击） | 重点方法或职责 |
|---|---|---|
| 1 | [job-keeper-server/src/main/java/io/ytong/raftjobkeeper/server/JobKeeperServerApplication.java](src/main/java/io/ytong/raftjobkeeper/server/JobKeeperServerApplication.java) | 启动 Spring Boot，通过 `@EnableScheduling` 启用定时扫描 |
| 2 | [job-keeper-server/src/main/java/io/ytong/raftjobkeeper/server/api/JobController.java](src/main/java/io/ytong/raftjobkeeper/server/api/JobController.java) | `createJob()`、`runJob()`、`submit()`：HTTP 请求转成 Command |
| 3 | [job-keeper-server/src/main/java/io/ytong/raftjobkeeper/server/raft/RaftServer.java](src/main/java/io/ytong/raftjobkeeper/server/raft/RaftServer.java) | `start()`、`apply()`：启动 Raft、提交命令 |
| 4 | [job-keeper-server/src/main/java/io/ytong/raftjobkeeper/server/fsm/JobStateMachine.java](src/main/java/io/ytong/raftjobkeeper/server/fsm/JobStateMachine.java) | `onApply()`、`CommandClosure`：应用日志、完成回调 |
| 5 | [job-keeper-server/src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java) | `apply()` 及各分支：实际的数据修改和条件检查 |
| 6 | [job-keeper-server/src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java) | `scan()`、`dispatch()`：扫描、回收和派发 |
| 7 | [job-keeper-server/src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java) | 本地连接、Worker 选择、容量预留 |
| 8 | [job-keeper-server/src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java) | Worker 消息转成注册、续租和结果命令 |

配套文件：

- 领域模型：[job-keeper-common/src/main/java/io/ytong/raftjobkeeper/common/model/](../job-keeper-common/src/main/java/io/ytong/raftjobkeeper/common/model/)。
- 命令定义：[job-keeper-common/src/main/java/io/ytong/raftjobkeeper/common/command/](../job-keeper-common/src/main/java/io/ytong/raftjobkeeper/common/command/)。
- gRPC 协议：[job-keeper-common/src/main/proto/worker.proto](../job-keeper-common/src/main/proto/worker.proto)。
- 配置：[job-keeper-server/src/main/resources/application.yaml](src/main/resources/application.yaml)。
- Store 测试：[job-keeper-server/src/test/java/io/ytong/raftjobkeeper/server/store/JobStoreTest.java](src/test/java/io/ytong/raftjobkeeper/server/store/JobStoreTest.java)。

下文使用 `api/JobController.java` 这样的短路径时，均相对于上面的服务端 Java 源码根目录。

## 3. 先跟一个最小例子

先只看“创建 Job”，不启动 Worker，也不执行任务。假设请求发到 Leader，服务端已配置对应的 API token：

```http
POST /jobs
Authorization: Bearer dev-api-token
Content-Type: application/json

{
  "name": "hello",
  "payload": { "executor": "shell", "command": "echo", "args": ["hello"] }
}
```

打开四个文件，跟到最后的 `jobs.put(...)` 即可：

```text
api/JobController.java → createJob()
  根据请求构造 Job，包装成 CREATE_JOB 命令
        ↓
raft/RaftServer.java → apply()
  将命令序列化，交给 SOFAJRaft 提交
        ↓ 日志提交后
fsm/JobStateMachine.java → onApply()
  解析命令，调用 store.apply(command)
        ↓
store/JobStore.java → apply() → createJob()
  jobs.put(job.id(), job)，返回创建的 Job
```

先记住这一点：**Job 最终保存在 Store 的内存 Map 中，修改之前要经过 Raft 提交；日志和快照用于持久化与恢复。**

## 4. 一次请求如何提交，并把结果返回客户端

### 4.1 Controller 构造命令，Spring 等待结果

`JobController.createJob()` 生成 Job ID，`runJob()` 生成 Task ID；两者通过私有方法 `submit()` 构造 `Command(type, requestId, now, body)`。

ID 和时间在提交前确定。各节点应用命令时使用相同输入，例如用 `command.now()` 计算触发时间和重试时间，不会各自在应用时重新生成。

Controller 返回 `CompletableFuture<JsonNode>`，意思是：**结果可能还没准备好，请 Spring 等这个 Future 完成，再发送本次 HTTP 响应。**

```text
客户端请求 → Controller 返回 Future
                   ↓ 如果还未完成
             Spring 保持请求未响应，释放原请求处理线程
                   ↓ Raft 命令应用完成
             future.complete(result)
                   ↓
             Spring 将 result 写成 JSON 响应
```

客户端收到的是 JSON，不是 Java 的 Future 对象。回调在服务端内部发生，不是服务端另行调用客户端接口，也不会通过这个请求持续推送执行进度。

| HTTP 请求 | 等待什么完成 | 成功响应 |
|---|---|---|
| `POST /jobs` | `CREATE_JOB` 应用完成 | 201，创建的 Job |
| `POST /jobs/{id}/run` | `FIRE_JOB` 应用完成 | 202，新建的 Task，返回记录的状态为 `PENDING` |
| `GET /tasks/{id}` | 查询当前执行记录 | 当前 Task 状态和结果 |

`/run` 不等待 Worker 执行完成。后台派发可能在 HTTP 响应发出前就已开始，因此收到创建时的 `PENDING` 记录后，立即查询也可能看到 `RUNNING` 或终态。

如果 HTTP 超时或断开，完成 Future 不能重新连接客户端；Raft 命令仍可能成功。重试时如何避免重复创建，见第 5 节。

### 4.2 RaftServer.apply() 每句做什么

```java
CompletableFuture<JsonNode> future = new CompletableFuture<>();
try {
    Task task = new Task();
    task.setData(ByteBuffer.wrap(mapper.writeValueAsBytes(command)));
    task.setDone(new JobStateMachine.CommandClosure(future));
    current.apply(task);
} catch (Exception exception) {
    future.completeExceptionally(exception);
}
return future;
```

- `new CompletableFuture<>()`：创建结果容器，本身不启动线程。
- `new Task()`：创建 SOFAJRaft 的日志提交对象。它不是 `common.model.Task` 业务执行记录。
- `writeValueAsBytes(command)`：把命令转换成 JSON 字节；`ByteBuffer.wrap(...)` 包装成 Raft 接口需要的类型。
- `setDone(...)`：登记完成回调，让回调保存这个 Future。
- `current.apply(task)`：发起异步提交，不在这里等待业务结果。
- `completeExceptionally(...)`：序列化或当前提交调用当场抛异常时，将 Future 标记为失败。后续异步提交失败由 Raft 回调通知。
- `return future`：将结果容器交给调用方；此时可能尚未完成，也可能已经完成。

方法开头还有 Leader 检查，发生在上述 `try` 之前：不是 Leader 会直接抛 `NotLeaderException`，不会返回 Future。这种 HTTP 请求如何处理，见第 8 节。

### 4.3 CommandClosure 是谁，run() 由谁调用

```java
task.setDone(new JobStateMachine.CommandClosure(future));
```

等价于：

```java
JobStateMachine.CommandClosure callback =
        new JobStateMachine.CommandClosure(future);
task.setDone(callback);
```

`CommandClosure` 是 `JobStateMachine` 内的静态嵌套类，实现了 SOFAJRaft 的 `Closure` 接口。创建对象只执行构造方法：

```java
public CommandClosure(CompletableFuture<JsonNode> future) {
    this.future = future;
}
```

这只是保存同一个 Future 的引用，不会自动执行 `run()`。

日志提交后，SOFAJRaft 调用 `JobStateMachine.onApply()`。成功路径的关键代码是：

```java
Closure done = iterator.done();          // 取出登记的回调
var result = store.apply(command);       // 执行业务命令
if (done instanceof CommandClosure commandClosure) {
    commandClosure.complete(result);     // 只保存到 result 字段
}
if (done != null) {
    done.run(new Status());              // 明确调用回调的 run()
}
```

`CommandClosure.run()` 才真正调用 `future.complete(result)`；状态失败时调用 `future.completeExceptionally(...)`。不要混淆“回调对象保存结果的 `complete()`”与“完成 Future 的 `complete()`”。

Follower 也会应用日志，但没有原请求的本地回调。日志复制的是命令字节，不包含 Future、回调对象或 HTTP 连接。

## 5. 客户端重试，为什么不会重复创建

去重需要客户端配合：**同一次操作重试沿用同一个 `Idempotency-Key`，新的独立操作使用新 key。** 创建 Job 和触发 Task 都支持这个请求头。

以触发为例，Controller 将 `Idempotency-Key: run-001` 转成 `Command.requestId = FIRE_JOB:run-001`。`JobStore.apply()` 在进入业务分支前检查：

```java
CachedResult cached = command.requestId() == null
        ? null : resultsByRequestId.get(command.requestId());
if (cached != null) {
    return cached.result().deepCopy();
}
```

没命中时执行命令，成功后缓存结果：

```java
resultsByRequestId.put(command.requestId(),
        new CachedResult(result.deepCopy(), command.now()));
```

```text
首次请求 run-001 → 创建 task-1 → 缓存结果 → 响应丢失
重试请求 run-001 → 命中缓存 → 返回原 task-1，不再创建 Task
```

重试仍可能写入新的 Raft 日志，但不会重复产生这次创建的业务效果。缓存随日志复制，也包含在快照中。创建 Job 的前缀为 `CREATE_JOB:`。

PowerShell 中将 key 的生成放在重试操作之外：

```powershell
$key = [guid]::NewGuid().ToString() # 本次操作只生成一次
$headers = @{
    Authorization = 'Bearer dev-api-token'
    'Idempotency-Key' = $key
}
# 假设 $jobId 已有值；失败后重试这条请求，复用原 $headers
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/jobs/$jobId/run" -Headers $headers
```

当前实现有以下边界：

- 不传 key 时，Controller 每次生成随机值，无法识别跨请求重试。
- 只按操作类型和 key 去重，不比较请求体，也没有将 Job ID 加入触发 key；不同 Job 的独立操作也要用不同 key。
- 返回的是第一次缓存的响应，不是 Task 最新状态；看进度需查询 Task。
- 失败抛异常的命令不会写入成功结果缓存；旧缓存会随历史清理过期，去重不是永久的。

## 6. Task 怎样到达 Worker，再带着结果回来

### 6.1 先建立 Worker 会话

`WorkerGrpcService.connect()` 接收双向 gRPC 流。首条消息必须是 `REGISTER`：

```text
检查 Worker ID 和 token
  → 非 Leader：发送 REDIRECT 并结束流
  → Leader：提交 REGISTER_WORKER
  → 成功后登记本地会话，回复 REGISTERED
```

这里有两份不同的数据：

| 数据 | 保存位置 | 是否通过 Raft 复制 |
|---|---|---|
| Worker 信息、最后心跳，以及 Task 的当前分配和租约 | `JobStore` | 是 |
| Worker gRPC 流、Session、本地预留的 Task ID 集合 | `WorkerRegistry` | 否 |

Store 中有 Worker 记录不代表当前节点有可用连接。快照也无法恢复一个网络连接。

#### Worker 信息谁知道，load 又存在哪里

这里的“主节点”指 Server Leader。不能将“Worker 信息”“Worker 连接”和“Worker 实际负载”当成同一份数据：

| 数据 | 保存位置 | Follower 是否知道 | 当前如何更新 |
|---|---|---|---|
| Worker ID、地址、labels、capacity、lastHeartbeat | 各节点的 `JobStore.workers` | 应用相关日志后知道，可能暂时落后 | REGISTER_WORKER / WORKER_HEARTBEAT 经 Raft 复制 |
| Task 的状态、workerId、attempt、租约 | 各节点的 `JobStore.tasks` | 应用相关日志后知道 | 分配、续租、结果、过期命令经 Raft 复制 |
| 可直接发送消息的 gRPC 流 | 当前进程的 `WorkerRegistry.Session` | 不会因 Raft 复制而得到连接 | Worker 连接到该进程，注册成功后登记 |
| 调度使用的 load | Session 的本地 `taskIds` 集合大小 | 不复制，也不从 Store 自动重建 | Server 预留时增加，释放时减少 |
| Worker 实际正在运行的任务 | Worker 进程的 `running` Map | 没有完整的负载同步协议 | Worker 接收、完成或取消任务时维护 |

复制 Worker 信息的入口见 [WorkerGrpcService.register()，第 69 行](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java#L69) 和 [heartbeat()，第 100 行](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java#L100)，落地逻辑是 [JobStore.registerWorker()，第 187 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L187)。因此，**不是只有 Leader 知道 Worker 信息，而是当前调度只使用 Leader 本地持有的连接和预留记录。**

Follower 不负责持续维护调度连接。若本节点失去 Leader 身份，[Dispatcher.scan()，第 52 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L52) 会调用 [WorkerRegistry.closeAll()，第 34 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L34)，尝试重定向并关闭本地流；Worker 重连新 Leader，复制在 Store 中的信息不会因此删除。

#### load 不是 gRPC 自动同步的，是 Server 自己记的账

选择逻辑见 [WorkerRegistry.leastLoaded()，第 26 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L26)：

```java
session.taskIds().size() / (double) session.worker().capacity()
```

例如 Worker 声明 capacity=4，Leader 本地预留了两个 Task，就按 `2/4` 计算负载。这个值不代表 CPU、内存或执行端实时上报的 running 数。

正常情况下，账本变化来自 Server 自身的操作：

```text
Dispatcher 派发前调用 reserve() → taskIds 加入 Task ID
结果上报回调调用 releaseTask() → 移除 Task ID
租约回收成功后 releaseTask()   → 移除 Task ID
```

具体位置：[WorkerRegistry.reserve()，第 51 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L51)、[releaseTask()，第 69 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L69)、[WorkerGrpcService.report()，第 122 行](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java#L122)、[Dispatcher.expire()，第 102 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L102)。失败清理也会影响预留，因此预留数与实际运行数不保证时刻相等。

虽然共享模型 [job-keeper-common/src/main/java/io/ytong/raftjobkeeper/common/model/Worker.java，第 6 行](../job-keeper-common/src/main/java/io/ytong/raftjobkeeper/common/model/Worker.java#L6) 定义了 `running` 字段，但当前 [JobStore.registerWorker()，第 187 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L187) 对新 Worker 将它设为 0，对已有 Worker 保留原值；分配和结果处理没有同步增减它。**不能把这个字段当成已经实现的实时负载统计。**

协议 [worker.proto，第 12 行](../job-keeper-common/src/main/proto/worker.proto#L12) 也没有 running_count 或完整的运行任务列表字段。Worker 心跳可以携带单个 Task ID，但 [WorkerGrpcService.heartbeat()，第 100 行](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java#L100) 只更新 Worker 信息和续租，不会把该 Task ID 加回 Session 的预留集合。gRPC 负责传消息，不会替应用程序更新这些数据结构。

#### 切换 Leader 或重新注册后，当前负载账本有什么缺口

新会话的创建位置是 [WorkerRegistry.register()，第 18 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L18) 和 [Session 构造方法，第 85 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L85)：

```java
public Session(Worker worker, StreamObserver<ServerMessage> stream) {
    this(worker, stream, Set.of()); // 初始预留集合为空
}
```

因此，不仅 Leader 切换，同一 Leader 上断线重连、重新注册也会创建空账本。例如：

```text
Worker capacity=4，实际仍在执行 task-1 和 task-2
  → 旧连接断开，Worker 连接新 Leader 并注册
  → 新 Session.taskIds 为空，调度按 0/4 计算
  → 旧任务的心跳可以续租，但不会恢复这两个预留位置
  → 新 Leader 可能再分配超过实际剩余容量的任务
```

Store 里虽然保留了 RUNNING Task 与 workerId，当前注册代码并未用它们重建 Session，也没有注册后的完整任务对账。心跳携带的 capacity 可以更新 Store 中的 Worker，但当前不会替换 Session 中用于调度的容量快照。

执行端有一道容量检查：[WorkerRuntime.accept()，第 138 行](../job-keeper-worker/src/main/java/io/ytong/raftjobkeeper/worker/runtime/WorkerRuntime.java#L138) 根据本地 running 集合拒绝重复或超容量任务。然而当前拒绝只记日志，没有发回明确的“拒绝接单”响应；Server 可能已经将 Task 记为 RUNNING，随后只能依赖租约回收。

所以当前行为是：**Worker 信息可复制恢复，网络连接需要重建，而用于调度的本地 load 不会完整恢复。** 若要补齐，需要在注册/重连时重建或对账运行任务，并定义接单确认、拒绝及负载更新规则；这些是后续实现方向，不是当前代码已有的保证。

### 6.2 scan() 每轮做什么

源码位置：[Dispatcher.java 的 scan()，第 52 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L52)。结构如下：

```java
@Scheduled(fixedDelayString = "${scheduler.scan-interval-ms:1000}")
public void scan() {
    if (!raft.isLeader()) {
        workers.closeAll(raft.leaderGrpcAddress());
        return;
    }
    Instant now = Instant.now();
    store.expiredTasks(now).forEach(task -> expire(task, now));
    store.staleWorkers(now.minus(workerTimeout))
            .forEach(worker -> removeWorker(worker.id(), now));
    store.canceledTasks().forEach(this::cancel);
    store.pendingTasks(now).forEach(this::dispatch);
}
```

`fixedDelay` 从上一次方法结束开始计时，默认再等 1000 ms。它不会等待本轮提交的所有 Raft 命令完成。

| 步骤 | 选择哪些记录 | 发起什么处理 |
|---|---|---|
| 非 Leader | 所有本地 Worker 会话 | 尝试发送 Leader 重定向，结束流并移除本地会话，随后返回 |
| 过期回收 | `RUNNING` 且租约已到期 | 提交 `EXPIRE_TASK` |
| 失联清理 | 最后心跳早于 `now - workerTimeout` | 提交 `REMOVE_WORKER` |
| 取消通知 | 已为 `CANCELED` 且有 Worker ID | 向 Worker 推送 `CANCEL` |
| 待执行派发 | `PENDING` 且已到 `availableAt` | 调用 `dispatch()` |

四类处理按顺序发起，但不等待异步提交完成。因此本轮发起回收的 Task，不一定能在本轮末尾就作为 PENDING 被选到。

移除 Worker 只删除复制的 Worker 记录，不直接重新排队 Task，也不在此处关闭本地会话。取消通知没有送达标记，后续扫描可能重复发送。

### 6.3 scan() 读取本地副本，为什么还能保证一致

源码位置：

| 文件与方法 | 起始行 | 在这条链路中的作用 |
|---|---|---|
| [Dispatcher.java：scan()](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L52) | 52 | 只在 Leader 上读取本地 Store，发起后续操作 |
| [RaftServer.java：start()](src/main/java/io/ytong/raftjobkeeper/server/raft/RaftServer.java#L70) | 70 | 将 JobStateMachine 注册给 SOFAJRaft |
| [JobStateMachine.java：onApply()](src/main/java/io/ytong/raftjobkeeper/server/fsm/JobStateMachine.java#L48) | 48 | 按已提交日志顺序调用 Store 应用命令 |
| [JobStore.java：apply()](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L26) | 26 | 串行执行命令及业务检查 |
| [JobStore.java：assignTask()](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L118) | 118 | 修改分配前再次检查 Task 状态 |

**各节点不要求每一时刻内存相同，而是按同一份已提交日志、以相同顺序更新。** Follower 应用日志可能暂时落后：

```text
已提交日志 100：创建 task-1，PENDING
已提交日志 101：分配 task-1，RUNNING

节点 A 应用到 101：task-1 = RUNNING
节点 B 应用到 100：task-1 = PENDING
节点 B 再应用 101 后：task-1 = RUNNING
```

每个 Store 都是本节点已经应用的日志所形成的视图，不是只有 Leader 的副本才有效。SOFAJRaft 负责日志复制、提交和调用 `onApply()`；业务代码负责根据命令更新各自的 Store。

扫描只提出操作，不能直接改 Task 的复制状态。例如 `scan()` 读到 PENDING 后，`dispatch()` 提交分配命令；真正应用时，`assignTask()` 会重新检查是否仍为 PENDING。如果已经变为 RUNNING，就拒绝这次分配。扫描读到的条件可能过时，不能代替应用命令时的业务检查。

```text
本地 Store：已应用日志形成的视图
  → scan()：根据视图提出操作
  → Raft：确定命令及顺序
  → JobStore.apply()：在该顺序中检查并执行操作
```

**Leader 切换也不会用任意落后的副本覆盖集群。** Raft 的选举规则限制候选者的日志新旧程度，保证已经提交的日志可以保留并由新 Leader 继续应用。这是 SOFAJRaft 提供的协议保证，选举算法没有在本项目中重新实现。

但“日志已存在”和“日志已应用到内存”是两回事。当前 [JobStateMachine.onLeaderStart()（第 34 行）](src/main/java/io/ytong/raftjobkeeper/server/fsm/JobStateMachine.java#L34) 只记录 term；[RaftServer.isLeader()（第 144 行）](src/main/java/io/ytong/raftjobkeeper/server/raft/RaftServer.java#L144) 只查询节点身份。`scan()` 没有额外调用 [RaftServer.readIndex()（第 125 行）](src/main/java/io/ytong/raftjobkeeper/server/raft/RaftServer.java#L125) 或等待应用进度屏障，因此不能把一次扫描理解为严格读取最新集群状态。

日志顺序解决各副本如何一致更新的问题；具体命令能否安全处理过时判断，还取决于 Store 中的检查是否充分。这里的分配状态检查不能推广成所有命令都已完整处理并发和故障，相关边界见第 7 节。

### 6.4 dispatch() 先记录分配，再发消息

入口：[Dispatcher.java 的 dispatch()，第 113 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L113)。它接收一个扫描得到的业务 Task，负责找 Worker 和发消息，本身不执行任务。

假设 `task-1` 当前为 PENDING，下面按代码顺序看它怎样分配给 `worker-A`。

**第一步：选择有空余容量的 Worker。**

位置：[Dispatcher.java，第 114 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L114)；选择逻辑在 [WorkerRegistry.java 的 leastLoaded()，第 26 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L26)。

```java
WorkerRegistry.Session session = workers.leastLoaded();
if (session == null) {
    return;
}
```

`Session` 保存 Worker 信息、gRPC 流和本地已预留的 Task ID 集合。这里只从当前进程持有的会话中选择，不是遍历 Store 中所有 Worker。

`leastLoaded()` 排除满载会话，再比较“预留任务数 / capacity”。例如 A 为 `1/4`、B 为 `2/4`，选择 A。这里不比较 CPU 使用率。没有可用会话就返回，Task 保持 PENDING，等待下一轮扫描。

**第二步：取得 Job 的执行配置。**

位置：[Dispatcher.java，第 118 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L118)。

```java
Job job = store.getJob(task.jobId());
if (job == null) {
    return;
}
```

Task 描述这次执行，Job 提供执行器、命令、参数、环境变量和超时。如果 Job 不存在或已删除，就跳过。这里读取的是派发时的 Job 定义，Task 自身没有保存一份完整的执行配置快照。

**第三步：先占一个本地容量位置。**

位置：[Dispatcher.java，第 122 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L122)；预留实现在 [WorkerRegistry.java 的 reserve()，第 51 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L51)。

```java
if (!workers.reserve(session.worker().id(), task.id())) {
    return;
}
```

可以理解为“还没提交完分配，但先给 task-1 占一个位置”。因为 Raft 提交是异步的，不先占位，后面的 Task 可能继续把这个 Worker 当成空闲。

选择和预留是两个步骤，所以 `reserve()` 还会检查会话是否存在、容量是否已满。预留成功只修改本地 Session，不修改 Store，不经过 Raft。对同一 Session 中已经预留的 Task ID，它返回 true；重复提交的边界见第 7 节。

**第四步：计算租约，构造分配命令。**

位置：[Dispatcher.java，第 125 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L125)。

```java
Instant now = Instant.now();
Instant expiry = now.plus(leaseDuration);
var body = mapper.createObjectNode()
        .put("taskId", task.id())
        .put("workerId", session.worker().id())
        .put("leaseExpireAt", expiry.toString());
```

如果现在是 `12:00:00`、租约为 30 秒，命令表达的是：“将 task-1 分配给 worker-A，租约到 12:00:30。”租约从这里计算，不是从 Worker 真正开始执行时计算；提交和推送耗时也占用这段时间。

**第五步：交给 Raft，等分配应用成功。**

位置：[Dispatcher.java，第 129 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L129)。

```java
raft.apply(new Command(CommandType.ASSIGN_TASK,
        UUID.randomUUID().toString(), now, body))
    .thenAccept(result -> {
        // 分配成功后，才向 Worker 推送 TASK
    });
```

`ASSIGN_TASK` 经过前面介绍的提交和回调链路，最终进入 [JobStore.java 的 assignTask()，第 118 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L118)：

```text
检查 Task 当前仍为 PENDING
  → 改成 RUNNING
  → 记录 worker-A、租约到期时间和 startedAt
  → 返回分配结果，完成此次提交的 Future
```

`RUNNING` 在这里表示服务端已记录分配，不是 Worker 已确认开始执行。`startedAt` 也在应用分配时设置，不是由执行端上报的实际开始时间。

`thenAccept(...)` 登记成功回调，不阻塞等待。它的 `result` 参数是分配结果，但当前推送代码没有使用这个参数，而是使用之前读取的 `task`、`job` 等对象。Future 已经完成时，回调也可能在登记时立即执行；不能把这个写法理解为一定创建了新线程。

**第五步中间的关键过程：谁在应用日志后触发推送回调？**

不是 `raft.apply(...)` 一返回就推送，而是等待此次提交对应的 Future 成功完成。完整调用链如下：

```text
Dispatcher.dispatch()
  → raft.apply(ASSIGN_TASK)，得到 Future，登记 thenAccept 推送回调
  → SOFAJRaft 复制日志并提交
  → SOFAJRaft 调用 JobStateMachine.onApply()
      → store.apply(command)
          → assignTask() 检查并将 Task 改成 RUNNING
      → commandClosure.complete(result)，暂存结果
      → done.run(成功状态)
          → CommandClosure.run()
              → future.complete(result)
                  → 触发 thenAccept(result -> ...) 中的推送代码
```

对照以下位置，就能找到每一步的实际调用：

| 位置 | 关键动作 |
|---|---|
| [RaftServer.apply()，第 108 行](src/main/java/io/ytong/raftjobkeeper/server/raft/RaftServer.java#L108) | 将 Future 放进 CommandClosure，通过 setDone 绑定到 Raft Task，再调用 Node.apply |
| [JobStateMachine.onApply()，第 48 行](src/main/java/io/ytong/raftjobkeeper/server/fsm/JobStateMachine.java#L48) | 取出回调，应用命令，保存结果并调用 done.run |
| [JobStore.assignTask()，第 118 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L118) | 检查 PENDING，写入 RUNNING 及分配信息 |
| [CommandClosure.complete()，第 113 行](src/main/java/io/ytong/raftjobkeeper/server/fsm/JobStateMachine.java#L113) | 仅设置回调对象的 result 字段，此处还没有完成 Future |
| [CommandClosure.run()，第 122 行](src/main/java/io/ytong/raftjobkeeper/server/fsm/JobStateMachine.java#L122) | 成功时调用 future.complete(result)，让成功回调可以执行 |
| [Dispatcher.dispatch() 的 thenAccept，第 130 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L130) | 真正进入下一步的消息构造和推送 |

这里等待的是提交节点本地将命令应用到 Store，不是等待所有 Follower 都应用完。其他节点会继续按日志顺序追赶；它们没有此次提交的本地 Future，也不会因应用这条日志而各自推送任务。

若本地应用时业务检查失败，Future 异常完成，不会进入 thenAccept。若完成得很快、在 thenAccept 登记前就已成功，登记时也可以执行回调；不论时机如何，成功应用始终在这次成功推送回调之前。

**第六步：Future 成功后，通过已建立的 gRPC 流推送消息。**

位置：[Dispatcher.java，第 131 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L131)；流的查找在 [WorkerRegistry.java 的 stream()，第 80 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L80)。

消息类型为 `TASK`，内容包括：

| 字段 | 来源和用途 |
|---|---|
| Task ID、Job ID | 标识这次执行及其所属定义 |
| executor、command、args、env | 从 Job 读取，告诉 Worker 执行什么 |
| attempt | 扫描得到的 Task 执行轮次，用于结果校验 |
| timeoutMillis | Job 的执行超时 |
| term | 推送时读取的本节点 Leader term |

消息由独立的 [job-keeper-worker](../job-keeper-worker/README.md) 进程接收和执行。Server 并没有在这个回调中启动业务命令。

发送实现在 [Dispatcher.java 的 StreamObserverGuard.send()，第 158 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L158)：流不存在或 `onNext()` 抛出运行时异常时返回 false，否则返回 true。**true 只说明这次本地发送调用没有抛异常，不代表 Worker 已收到、已开始或已执行成功。** 最终结果要走第 6.5 节的上报链路。

**最后：区分失败发生在哪一步。**

失败回调位于 [Dispatcher.java，第 150 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L150)，本地释放逻辑在 [WorkerRegistry.java 的 releaseTask()，第 69 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/WorkerRegistry.java#L69)。

```java
.exceptionally(error -> {
    workers.releaseTask(task.id());
    log.warn("Task {} could not be assigned", task.id(), error);
    return null;
});
```

| 情况 | 当前实现的处理 |
|---|---|
| 返回的 Future 异常完成，例如 Store 拒绝重复分配 | 跳过成功推送，释放本地预留并记录日志；分配是否已提交不能只凭异常判断 |
| 分配成功，但发送返回 false | 第 145 行只记录警告，不进入 exceptionally；Task 保持 RUNNING，预留不在此处释放，等待租约回收等后续处理 |
| `raft.apply(...)` 当场抛异常，例如扫描期间失去 Leader 身份 | Future 链尚未建立，exceptionally 接不到；当前方法没有 try/catch，本次预留不会通过此分支释放 |
| 成功回调中的其他代码抛异常 | 后面的 exceptionally 也会执行，但已经提交的分配不会因此自动回滚 |

因此，不能把“执行了失败回调”一概理解成“Raft 中没有分配记录”。当前清理主要处理本地预留，不是撤销业务状态。

把整个方法串起来就是：

```text
选择 Worker → 预留本地容量 → 提交 ASSIGN_TASK
                                  ↓ 应用成功
                            Store 记录 RUNNING
                                  ↓
                            推送 TASK 给 Worker
```

先记录再推送，保证已提交的分配可以在其他节点恢复；但记录分配和网络推送不是一个原子操作，所以需要租约处理“已记录分配、却没能正常执行”的情况。


### 6.5 心跳、结果与重试：沿 gRPC 协议读代码

这一节从协议读到 Server 和 Worker 两端。先记住：**gRPC 负责传消息，Raft 负责确认这些消息引起的数据变更。收到消息不等于更新已经提交。**

**第一步：看协议，理解一条流里能传什么。**

位置：[job-keeper-common/src/main/proto/worker.proto，第 8 行](../job-keeper-common/src/main/proto/worker.proto#L8)。

```protobuf
service WorkerService {
  rpc Connect(stream WorkerMessage) returns (stream ServerMessage);
}
```

两个 `stream` 表示双向流：Worker 主动调用一次 Connect，随后双方都可以持续发送多条消息，不是每次心跳都重新发起一次 RPC。

Java 代码中的 `WorkerServiceGrpc`、`WorkerMessage` 等类型由 protobuf/gRPC 构建插件根据这个文件生成。`java_package` 指定生成类的包，`java_multiple_files` 让消息等类型分别生成 Java 文件。

协议用字符串 `type` 区分消息，没有使用 enum 或 oneof；哪些字段必须填写，主要由业务代码校验。字段后面的 `= 1`、`= 2` 是 protobuf 字段编号，不是默认值。

Worker 发给 Server 的字段定义在 [worker.proto，第 12 行](../job-keeper-common/src/main/proto/worker.proto#L12)：

| type | 当前 Worker 发送的主要字段 | 用途 |
|---|---|---|
| `REGISTER` | worker_id、address、capacity、token | 认证并注册；协议还支持 labels |
| `HEARTBEAT` | worker_id、address、capacity；有运行任务时带 task_id、attempt | 更新在线时间，并为指定 Task 续租 |
| `RESULT` | worker_id、task_id、attempt、status、result | 上报某轮执行的状态与输出 |

Server 发给 Worker 的定义在 [worker.proto，第 25 行](../job-keeper-common/src/main/proto/worker.proto#L25)：

| type | 内容 | 当前 Worker 如何处理 |
|---|---|---|
| `REGISTERED` | 注册成功通知 | 收到后不额外处理 |
| `TASK` | task 中包含 TaskAssignment | 检查 term、重复任务及容量，再提交执行线程池 |
| `CANCEL` | task 中只需 Task ID | 尝试取消本地运行任务 |
| `REDIRECT` | leader 地址 | 记下地址，当前流结束后用于重连 |
| `ACK` | 结果处理成功通知 | 收到后不额外处理 |
| `STALE` | 结果处理失败通知，可带 leader | 仅记录日志，不因此自动重连或重报 |

`TaskAssignment` 的字段见 [worker.proto，第 31 行](../job-keeper-common/src/main/proto/worker.proto#L31)；它只用于派发任务，不能直接当成 Store 中的完整 Task 记录。

**第二步：看 connect()，区分收消息和发消息的对象。**

位置：[WorkerGrpcService.java 的 connect()，第 46 行](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java#L46)。

```java
public StreamObserver<WorkerMessage> connect(
        StreamObserver<ServerMessage> response) {
    return new StreamObserver<>() {
        // onNext(message)：处理 Worker 发来的消息
        // response.onNext(...)：向 Worker 发消息
    };
}
```

每次 Connect 调用创建一个接收对象，并在其中保存该连接的 `workerId` 和 `registered`。尚未注册时进入 `register()`，注册完成后才按 type 分发到 `heartbeat()` 或 `report()`。

注册流程见 [WorkerGrpcService.java，第 69 行](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java#L69)：校验首条消息和 token，非 Leader 重定向；Leader 等 REGISTER_WORKER 应用成功后才登记本地流并回复 REGISTERED。之后处理心跳和结果时使用连接中保存的 workerId，不是直接相信每条消息里的 worker_id。

服务启动入口是 [GrpcServerLifecycle.java，第 16 行](src/main/java/io/ytong/raftjobkeeper/server/grpc/GrpcServerLifecycle.java#L16)。Worker 的连接及消息分发入口是 [job-keeper-worker/src/main/java/io/ytong/raftjobkeeper/worker/runtime/WorkerRuntime.java 的 connectAndWait()，第 90 行](../job-keeper-worker/src/main/java/io/ytong/raftjobkeeper/worker/runtime/WorkerRuntime.java#L90)。Worker 当前不会等 REGISTERED 才开始定时发心跳；服务端注册又是异步的，因此“注册完成前收到其他消息”的情况不能假定已被协议握手排除。

**第三步：心跳既报在线，也可以为一个 Task 续租。**

Worker 在 [WorkerRuntime.java 的 start()，第 55 行](../job-keeper-worker/src/main/java/io/ytong/raftjobkeeper/worker/runtime/WorkerRuntime.java#L55) 启动每 5 秒一次的心跳定时任务，具体发送在 [sendHeartbeats()，第 184 行](../job-keeper-worker/src/main/java/io/ytong/raftjobkeeper/worker/runtime/WorkerRuntime.java#L184)：

```text
有运行任务 → 每个 Task 分别发一条 HEARTBEAT，携带 task_id 和 attempt
没有运行任务 → 发一条不带 task_id 的 HEARTBEAT
```

Server 在 [WorkerGrpcService.java 的 heartbeat()，第 100 行](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java#L100) 处理：

```text
每条 HEARTBEAT
  → 提交 WORKER_HEARTBEAT，更新 Worker 的 lastHeartbeat
  → 如果 task_id 非空，再提交 RENEW_LEASE
       leaseExpireAt = Server 接收时的 now + leaseDuration
```

例如 Server 在 12:00:10 收到 task-1 的心跳，租约为 30 秒，就请求将租约设为 12:00:40。不是在旧到期时间上再加 30 秒，也不是使用 Worker 的本地时间。

这两次提交各自异步完成，不是一个原子操作，没有心跳 ACK。在线时间更新成功，不代表 Task 续租也成功；失败会记录日志。

业务检查位置是 [JobStore.java 的 renewLease()，第 132 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L132)：

```java
if (task.status() != TaskStatus.RUNNING
        || !Objects.equals(task.workerId(), text(body, "workerId"))) {
    throw new IllegalStateException("Task is not running on this worker");
}
```

**Worker 虽然在心跳中发送 attempt，但 Server 没有将它放进续租命令，Store 也不检查执行轮次。** 因此这里不能声称可以完整排除同一 Worker 上旧轮次心跳的影响。

Worker 心跳信息的写入位置是 [JobStore.java 的 registerWorker()，第 187 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L187)，它被注册和心跳两类命令共用。这里更新复制的 Worker 记录，不更新 WorkerRegistry 中 Session 的容量快照。

**第四步：执行结果先提交 Raft，再回复 ACK。**

Worker 收到 TASK 后，在 [WorkerRuntime.java 的 accept()，第 138 行](../job-keeper-worker/src/main/java/io/ytong/raftjobkeeper/worker/runtime/WorkerRuntime.java#L138) 检查 term、重复 Task ID 和容量，再放进线程池。执行入口是 [execute()，第 157 行](../job-keeper-worker/src/main/java/io/ytong/raftjobkeeper/worker/runtime/WorkerRuntime.java#L157)，按 executor 类型调用执行器。

执行结束后，[RunningTask.reportOnce()，第 277 行](../job-keeper-worker/src/main/java/io/ytong/raftjobkeeper/worker/runtime/WorkerRuntime.java#L277) 发送 RESULT，例如：

```text
type      = RESULT
worker_id = worker-A
task_id   = task-1
attempt   = 1
status    = SUCCEEDED
result    = 执行器输出
```

这只是字段示意，线上传输的是 protobuf 消息，不是上述文本或 JSON。

Server 处理入口：[WorkerGrpcService.java 的 report()，第 122 行](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java#L122)。

```java
raft.apply(new Command(CommandType.REPORT_TASK, ...))
    .whenComplete((result, error) -> {
        registry.releaseTask(message.getTaskId());
        if (error == null) {
            response.onNext(ServerMessage.newBuilder().setType("ACK").build());
        } else {
            // 发送 STALE，可附带 Leader 地址
        }
    });
```

此处省略了命令构造参数。`whenComplete` 同时处理成功和失败：先释放本地预留，提交及应用成功则回复 ACK，失败则回复 STALE。STALE 在当前实现中覆盖所有异步处理错误，不能只理解成“结果轮次过旧”。若 `raft.apply()` 同步抛异常，则由外层 onNext 的 catch 结束响应流，不会进入这个回调。

Store 的结果校验在 [JobStore.java 的 reportTask()，第 145 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L145)：

```text
Task 必须仍为 RUNNING
  且 Worker ID 与当前分配一致
  且 attempt 与当前轮次一致
  且 status 为 SUCCEEDED、FAILED 或 TIMEOUT
```

例如 Task 已进入 attempt=2，attempt=1 的旧结果就不能覆盖当前记录。Worker 取消时还可能上报 CANCELED，但 Store 不接受这个结果状态；REPLACE 的取消状态已由触发命令写入，不能将这个 RESULT 当成支持 CANCELED 的确认协议。

**第五步：失败是否重试，由 Store 决定，不由 ACK 决定。**

仍在 [JobStore.reportTask()，第 145 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L145) 中：

| 上报情况 | Store 更新 |
|---|---|
| SUCCEEDED | 进入成功终态 |
| FAILED / TIMEOUT，且 `attempt <= maxRetry` | 同一 Task 回到 PENDING，attempt 加一，availableAt 设为 `now + retryBackoff` |
| FAILED / TIMEOUT，无剩余重试额度 | 进入对应失败或超时终态 |

例如 maxRetry=2，第一次失败后进入 attempt=2，第二次失败后进入 attempt=3，第三次再失败才结束。退避时长固定；到 availableAt 后，Dispatcher 才会再次选择这个 Task。

ACK 表示“结果命令处理成功”，不表示“业务执行成功”。一个 FAILED 结果被正确记录并安排重试，同样会得到 ACK。

Worker 没有上报结果或无法续租时，回收入口是 [Dispatcher.java 的 expire()，第 102 行](src/main/java/io/ytong/raftjobkeeper/server/dispatcher/Dispatcher.java#L102)，应用逻辑在 [JobStore.java 的 expireTask()，第 171 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L171)。它会再次检查租约是否仍过期；仍可重试就回到 PENDING，否则标为 FAILED。

触发同一 Job 的 ALLOW / FORBID / REPLACE 规则是另一个层次，见 [JobStore.java 的 fireJob()，第 94 行](src/main/java/io/ytong/raftjobkeeper/server/store/JobStore.java#L94)：它决定是否创建新的 Task，重试则沿用旧 Task ID。

**最后：连接恢复不等于结果可靠重传。**

断流后，Server 在 [WorkerGrpcService.java 的 onError()/onCompleted()，第 140 行](src/main/java/io/ytong/raftjobkeeper/server/api/WorkerGrpcService.java#L140) 移除对应的本地会话，不直接把 Task 改成失败。Worker 在 [WorkerRuntime.java 的 connectLoop()，第 65 行](../job-keeper-worker/src/main/java/io/ytong/raftjobkeeper/worker/runtime/WorkerRuntime.java#L65) 重连；旧任务不会仅因流关闭就在这条路径被自动停止，运行任务后续发送会使用当时的 activeConnection。

当前 ACK / STALE 不携带 Task ID，Worker 的消息分发也不维护“等待确认的结果队列”。`reportOnce()` 先将 reported 标记设为 true，再尝试通过当前连接发送；没有连接时不会保存待发结果，也不会在未收到 ACK 后自动重报。

因此当前代码提供了重连、租约和执行重试，但没有实现结果消息的持久化重传。结果丢失后，即使外部执行已经成功，Store 仍可能因租约到期而安排再执行一次。外部业务幂等的边界见第 7 节。


## 7. 重复分配和 Leader 切换会发生什么

### 7.1 不切换 Leader，也可能重复提交分配

这不要求两轮扫描方法同时运行，只需要第一次提交尚未完成：

```text
第 1 轮读到 task-1 为 PENDING → 提交 ASSIGN_TASK，scan() 返回
下一轮开始时，第一次提交还没应用
  → 仍读到 PENDING → 可能再次提交 ASSIGN_TASK
```

`WorkerRegistry.reserve()` 对同一 Session 内已预留的 Task ID 返回 true，也没有全局的“这个 Task 正在提交分配”集合，因此它没有完全阻止重复提交。

`JobStore.assignTask()` 在修改前检查：

```java
if (task.status() != TaskStatus.PENDING) {
    throw new IllegalStateException("Only pending tasks can be assigned");
}
```

如果两条分配命令之间没有其他状态转换，第一次将 Task 改成 RUNNING，第二次就被拒绝。Store 按 Task ID 只保存一条当前记录，不会同时保存两条互相矛盾的当前分配。

### 7.2 日志已经提交，为什么还能拒绝

Raft 确定的是命令及顺序，命令仍可能因业务条件不满足而不修改数据：

```text
日志 100：分配 task-1 → PENDING，接受并改成 RUNNING
日志 101：再次分配   → 已是 RUNNING，拒绝，保持原记录
日志 102：继续处理下一条命令
```

`JobStore.apply()` 使用 `synchronized` 串行处理命令。`onApply()` 捕获第二次的异常，保存到回调，通过失败状态完成 Future，然后执行 `iterator.next()`。日志不会撤销，这条路径也没有调用 Raft 状态机回滚或停止接口。

这次 Future 的调用方是 Dispatcher：它的 `thenAccept(...)` 不会运行，所以不会为被拒绝的分配推送任务；失败分支释放本地预留并记日志。它不会通过最初 `/run` 的 HTTP 请求另发一个失败响应，也不会自动成为 Task 的执行结果。

当前本地预留按 Task ID 释放，未区分重复提交，因此失败清理还可能释放其他有效分配的预留。业务状态检查保护的是 Store 中的当前分配，不能据此认为本地容量记账已完全覆盖并发情况。

此外，`onApply()` 当前捕获所有 `Exception` 后继续。上述拒绝发生在数据修改前；若出现数据损坏、程序缺陷或部分修改后才抛异常，需要区别处理，不能假定都有相同的安全性。

### 7.3 Leader 切换后，旧执行可能还活着

新 Leader 拥有复制的分配与租约记录，但没有旧 Leader 的本地连接和容量预留；Worker 需要重新连接。只要仍为 RUNNING，扫描不会直接重新派发，而是等待结果或租约回收。

如果旧执行没有及时续租，并且允许重试：

```text
Worker-A 执行 attempt=1
  → Leader 切换或网络中断，续租未能成功
  → 新 Leader 提交租约回收，Task 回到 PENDING，attempt=2
  → 派给 Worker-B
  → Worker-A 的旧执行可能仍未停止
```

上报结果的 `attempt` 检查能拒绝旧轮次结果，但不能撤销外部系统中已经发生的写入。**记录只有一个当前分配，不等于外部业务只执行一次。** 当前实现不保证外部副作用恰好一次，执行器调用的业务应考虑幂等。

## 8. 请求发到 Follower，以及查询一致性

`RaftServer.apply()` 发现非 Leader 时，在提交前直接抛出 `NotLeaderException`。它继承 `RuntimeException`，所以方法不必声明 `throws`。

HTTP 307 来自 [`ApiExceptionHandler`](src/main/java/io/ytong/raftjobkeeper/server/api/ApiExceptionHandler.java) 中的明确处理：

```text
Controller 调用 RaftServer.apply()
  → 抛出 NotLeaderException
  → Spring 匹配 @ExceptionHandler(NotLeaderException.class)
  → 返回 ResponseEntity，状态为 TEMPORARY_REDIRECT（307）
  → 已知 Leader 时设置 Location 响应头
```

服务端没有代为代理请求。项目也没有提交 Job 的专用 SDK/CLI；PowerShell 是调用 REST API 的客户端，Worker 则是另一类 gRPC 客户端。

PowerShell 7.4+ 可显式保留鉴权头和请求方法，跟随可信集群地址的重定向：

```powershell
# 假设 $headers 和 $body 已准备好
$params = @{
    Uri = 'http://localhost:8080/jobs'
    Method = 'Post'
    Headers = $headers
    ContentType = 'application/json'
    Body = $body
    MaximumRedirection = 5
    PreserveAuthorizationOnRedirect = $true
    PreserveHttpMethodOnRedirect = $true
}
Invoke-RestMethod @params
```

默认重定向会移除 Authorization；保留请求方法的参数从 7.4 开始提供，详见 [PowerShell 官方文档](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.utility/invoke-restmethod?view=powershell-7.5)。没有 Leader 地址、重定向次数耗尽或再次切换 Leader 时仍可能失败，需稍后使用相同幂等 key 重试。

Job/Task 查询支持：

| 参数 | 当前实现 |
|---|---|
| `consistency=strong`，默认 | 要求当前节点为 Leader，等待 `readIndex()` 成功后读取 Store |
| `consistency=stale` | 不执行 ReadIndex，直接读本节点 Store，可读 Follower，但可能落后 |

因此默认查询发到 Follower 也会收到 307。入口预检查不能代替 Store 应用命令时的判断；扫描开始时的 Leader 身份检查也不能保证整个异步提交期间身份不变。

## 9. 数据如何恢复和清理

`RaftServer.start()` 创建 Raft 分组和数据目录，注册 `JobStateMachine`。持久化路径为：

```text
<raft.data-dir>/<raft.group-id>/
├── log/
├── meta/
└── snapshot/
```

`onSnapshotSave()` 将 `JobStore.snapshot()` 写到 `job-store.json`；`onSnapshotLoad()` 读取后调用 `restore()`。快照包含 jobs、tasks、workers 和幂等结果缓存。恢复过程由 SOFAJRaft 驱动，结合快照与后续日志恢复 Store。

`WorkerRegistry` 的连接与容量预留不在快照里。`onLeaderStart(term)` / `onLeaderStop()` 更新本地 term，派发消息携带该 term。

另一个定时方法 `purgeHistory()` 默认每小时提交 `PURGE_HISTORY`，清理超过保留期的终态 Task 和旧幂等结果；默认保留 168 小时。

Job 禁用只阻止新的触发，不取消已有 Task；删除将 Job 标记为 DELETED 并从正常查询中隐藏，不级联删除 Task。当前派发会跳过已删除 Job，结果和租约处理也依赖 Job 仍可查到，因此不要将删除理解为完整的任务终止流程。

## 10. 本地跑一遍，再沿断点读代码

需要 JDK 21、Maven 3.9+。构建 Server 和 Worker：

```powershell
mvn -pl job-keeper-server,job-keeper-worker -am package
```

第一个终端启动单节点 Server：

```powershell
$env:API_TOKEN = 'dev-api-token'
$env:WORKER_TOKEN = 'dev-worker-token'
java --add-opens=java.base/java.util=ALL-UNNAMED -jar job-keeper-server/target/job-keeper-server.jar --raft.data-dir=./data --raft.self-address=localhost:7000 --raft.initial-peers=localhost:7000
```

第二个终端启动 Worker：

```powershell
$env:WORKER_ID = 'worker-1'
$env:SERVER_ADDR = 'localhost:9090'
$env:WORKER_TOKEN = 'dev-worker-token'
java -jar job-keeper-worker/target/job-keeper-worker.jar
```

第三个终端检查 Leader，再创建一个通过 HTTP 执行器访问本地健康接口的 Job：

```powershell
Invoke-RestMethod http://localhost:8080/leaderz
$redirect = @{
    MaximumRedirection = 5
    PreserveAuthorizationOnRedirect = $true
    PreserveHttpMethodOnRedirect = $true
}
$createHeaders = @{
    Authorization = 'Bearer dev-api-token'
    'Idempotency-Key' = [guid]::NewGuid().ToString()
}
$body = @{
    name = 'learn-server'
    payload = @{
        executor = 'http'
        command = 'http://localhost:8080/healthz'
        args = @('GET')
        env = @{}
    }
    timeout = 'PT30S'
    maxRetry = 2
    retryBackoff = 'PT5S'
    concurrency = 'FORBID'
} | ConvertTo-Json -Depth 5

# 创建失败后重试此请求时，复用原来的 createHeaders 和 body
$job = Invoke-RestMethod -Method Post -Uri http://localhost:8080/jobs -Headers $createHeaders -ContentType 'application/json' -Body $body @redirect

$runHeaders = @{
    Authorization = 'Bearer dev-api-token'
    'Idempotency-Key' = [guid]::NewGuid().ToString()
}
# 重复这条请求，复用 runHeaders，会返回同一个 Task 的创建结果
$task = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/jobs/$($job.id)/run" -Headers $runHeaders @redirect

# 重复查询，观察当前状态
Invoke-RestMethod "http://localhost:8080/tasks/$($task.id)" @redirect
```

`JobRequest` 未指定时默认超时 5 分钟、重试退避 5 秒、`maxRetry = 0`、并发策略 ALLOW。Duration 请求值使用 `PT30S`、`PT5M` 等格式。

建议分阶段下断点，避免长时间暂停整个 Server 干扰心跳和租约：

| 阶段 | 断点 | 观察内容 |
|---|---|---|
| 创建 | `JobController.createJob()`、`JobStore.createJob()` | 请求怎样变成命令，再存入 Map |
| 回调 | `RaftServer.apply()`、`JobStateMachine.onApply()`、`CommandClosure.run()` | Future 和回调是同一次提交的本地对象 |
| 派发 | `Dispatcher.dispatch()`、`JobStore.assignTask()` | 先提交 RUNNING，再推送 TASK |
| 结果 | `WorkerGrpcService.report()`、`JobStore.reportTask()` | Worker、attempt、结果及重试条件 |

已有 `JobStoreTest` 覆盖触发幂等、FORBID、租约回收和 Store 快照恢复；它不是完整 Raft 集群或 gRPC 集成测试。

```powershell
mvn -pl job-keeper-server -am test
```

## 11. 配置与接口索引

配置来源是 `application.yaml` 和各组件构造器中的 `@Value`。

| 配置 | 默认值 | 用途 |
|---|---|---|
| `server.port` | `8080` | REST 端口 |
| `grpc.port` | `9090` | Worker gRPC 端口 |
| `raft.data-dir` / `RAFT_DATA_DIR` | `/data` | 持久化根目录 |
| `raft.group-id` | `raft-job-keeper` | Raft 分组与数据子目录 |
| `raft.self-address` / `ADVERTISE_ADDR` | `localhost:7000` | 本节点 Raft 地址 |
| `raft.initial-peers` / `RAFT_INITIAL_PEERS` | `localhost:7000` | 初始成员，逗号分隔 |
| `raft.election-timeout-ms` | `1000` | 选举超时 |
| `raft.snapshot-interval-secs` | `600` | 自动快照间隔 |
| `scheduler.scan-interval-ms` | `1000` | 调度扫描间隔 |
| `scheduler.lease-duration` | `30s` | 派发与续租的租约时长 |
| `scheduler.worker-timeout` | `15s` | Worker 心跳超时 |
| `scheduler.task-retention` | `168h` | 终态 Task 和幂等缓存保留期 |
| `scheduler.retention-scan-ms` | `3600000` | 清理扫描间隔 |
| `API_TOKEN` | 空 | REST 写操作及所有 `/admin/` 请求的 Bearer token |
| `WORKER_TOKEN` | 回退到 `API_TOKEN`，再回退为空 | Worker 注册 token |

[`ApiTokenFilter`](src/main/java/io/ytong/raftjobkeeper/server/api/ApiTokenFilter.java) 对未配置 API_TOKEN 的受保护请求返回 503，凭据错误返回 401。Worker token 为空或不匹配时注册失败。

| 接口 | 方法与用途 | 源码 |
|---|---|---|
| `/jobs` | POST 创建、GET 列表 | `JobController` |
| `/jobs/{id}` | GET 详情、PUT 更新、DELETE 删除 | `JobController` |
| `/jobs/{id}/enable`、`disable`、`run` | POST 启停或触发 | `JobController` |
| `/tasks`、`/tasks/{id}` | GET 执行记录 | `JobController` |
| `/admin/cluster/status` | GET 成员与角色 | [`AdminController`](src/main/java/io/ytong/raftjobkeeper/server/api/AdminController.java) |
| `/admin/cluster/join`、`leave` | POST，body 为 `{"address":"host:7000"}` | `AdminController` |
| `/admin/snapshot`、`/admin/transfer-leadership` | POST 快照或转移 Leader | `AdminController` |
| `/healthz`、`/readyz`、`/leaderz` | GET 健康和角色 | [`HealthController`](src/main/java/io/ytong/raftjobkeeper/server/api/HealthController.java) |
| `/metrics` | GET Prometheus 文本格式的当前状态指标 | [`MetricsController`](src/main/java/io/ytong/raftjobkeeper/server/api/MetricsController.java) |

当前 `readyz` 固定返回 200，不能据此判断集群拥有多数派；`leaderz` 检查本节点是否为 Leader。gRPC 服务启动与关闭见 [`GrpcServerLifecycle`](src/main/java/io/ytong/raftjobkeeper/server/grpc/GrpcServerLifecycle.java)。

多节点部署见根目录 [`README.md`](../README.md) 和 [`deploy/`](../deploy/)。当前 `leaderHttpAddress()` / `leaderGrpcAddress()` 使用 Leader 的 Raft 主机地址加本节点配置的服务端口推导地址，因此各节点需要保持对应的 HTTP/gRPC 端口一致。
