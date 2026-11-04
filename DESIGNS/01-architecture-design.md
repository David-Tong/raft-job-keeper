# Raft Job Keeper 架构设计

> 📖 **前置阅读：[Raft 共识算法简介（00-raft-primer.md）](./00-raft-primer.md)**
> 本文假定读者了解 Leader / Follower / 任期 / 日志复制 / 多数派等 Raft 基本概念；如不熟悉，请先阅读该文档。本文只描述 Raft 在本系统中的使用方式。
>
> 💬 **待决的架构问题**（Raft 内嵌还是独立服务、跨 DC 部署）见 [04-architecture-discussion.md](./04-architecture-discussion.md)。

## 1. 目标与范围

使用 Raft 共识算法实现一个高可用、强一致的分布式作业调度系统。

目标：
- 作业元数据（定义、状态、调度计划）强一致，Leader 故障后自动切换，不丢已提交数据。
- 作业由用户通过 API 手动触发运行（不含定时/周期调度，该能力与分布式一致性主题无关，不在范围内）。
- 作业每次触发只被执行一次（at-least-once 投递 + 幂等/租约保证有效的 exactly-once 触发）。
- 支持水平扩展 Worker，Worker 无状态。
- 可在 Kubernetes 上以 StatefulSet 部署，支持滚动升级与节点增减。

非目标：跨地域多活、作业 DAG 编排（后续版本）。

技术选型：Java 21 + Spring Boot 3 + Maven；Raft 库 [SOFAJRaft](https://github.com/sofastack/sofa-jraft)（内置 RocksDB 存储日志/元数据/快照，自带 Leader 选举、成员变更、ReadIndex 线性一致读）；指标 Micrometer + Prometheus。

接口协议划分：

| 通信 | 协议 | 端口 |
|------|------|------|
| 用户/CLI/控制台 → Server（作业与任务管理、集群状态） | REST/JSON（Spring MVC） | 8080 |
| 健康检查 `/healthz` `/readyz`、指标 `/metrics`、集群管理 Join/Leave | REST | 8080 |
| Worker ↔ Leader（注册、任务推送、心跳、租约续约、结果上报） | gRPC 双向流（grpc-java） | 9090 |
| Server ↔ Server（Raft 日志复制、选举、快照传输） | SOFAJRaft 内置 bolt RPC | 7000 |

对外不使用 grpc-gateway：用户接口只有 REST；gRPC 仅供 Worker 使用。

术语说明：本系统**没有固定的 Master/Slave**。所有 Server 节点运行同一程序、地位对等，只是在某一时刻角色不同——一个 Leader，其余为 Follower，Leader 故障后由 Follower 选举产生新的 Leader。Worker 不属于 Raft 集群，是独立的无状态进程，只与当前 Leader 通信。

## 2. 总体架构

```
   Client / CLI / SDK（外部调用方，不属于系统角色）
          |  REST :8080（经 K8s Service，任一 Server 可接收，写请求重定向到 Leader）
          v
 +----------------- Raft 集群（3 或 5 个 Server，Raft :7000）-----------------+
 |                                                                            |
 |  +-----------------+     +-----------------+     +-----------------+      |
 |  | Server-0        |<--->| Server-1        |<--->| Server-2        |      |
 |  | 角色: Leader    |     | 角色: Follower  |     | 角色: Follower  |      |
 |  +--------+--------+     +-----------------+     +-----------------+      |
 |           |                                                               |
 |  每个 Server 内部（同一程序）：                                             |
 |    REST API -> Raft Core -> FSM(JobStore) -> Snapshot                      |
 |    Scheduler / Dispatcher / Worker gRPC 服务（仅 Leader 上运行）            |
 +-----------+---------------------------------------------------------------+
             | gRPC 双向流 :9090（Worker 只连 Leader）
             v
   +----------+  +----------+  +----------+
   | Worker-1 |  | Worker-2 |  | Worker-N |   Worker（独立进程，不参与 Raft）
   +----------+  +----------+  +----------+
```

### 2.1 角色

本系统只有两类组件：**Server** 和 **Worker**。Leader / Follower 不是独立组件，而是 Server 在某一时刻的角色。

| 组件 | 角色 | 说明 |
|------|------|------|
| **Server**（raft-job-keeper-server） | — | Raft 集群成员。所有 Server 运行同一程序，都包含 Raft、状态机、REST API、调度器，对等无主从之分；每个 Server 在任一时刻是 Leader 或 Follower 之一，角色随选举变化。 |
| ↳ Server | **Leader** | 集群中唯一一个。处理写请求、运行调度循环（Scheduler / Dispatcher）、接受 Worker 的 gRPC 连接。 |
| ↳ Server | **Follower** | 其余所有 Server。复制 Leader 日志；可处理只读请求（可选 stale read）；写请求重定向到 Leader；Leader 故障时参与选举，任一 Follower 都可能成为新 Leader。 |
| **Worker** | — | 独立进程，不参与 Raft。通过 gRPC 向 Leader 注册、领取任务、执行、上报结果。Worker 始终作为独立进程部署，不内嵌在 Server 中，以保证任务执行的资源与故障与 Raft 集群隔离。 |

### 2.2 核心原则

1. **所有状态变更必须先经过 Raft 日志**。调度器决定“谁在何时执行什么”也要作为命令（`AssignTask`）提交，再由 FSM 应用，避免 Leader 切换后重复分派。
2. **FSM（状态机）必须确定性**：Apply 中不得读取本地时间、随机数。时间戳由 Leader 在构造命令时写入命令体。
3. **调度只在 Leader 运行**：通过 `StateMachineAdapter.onLeaderStart(term)` / `onLeaderStop(status)` 回调感知角色，成为 Leader 时启动 Scheduler，失去领导权时立即停止。
4. **租约（Lease）**：任务分配给 Worker 带租约到期时间；Worker 通过心跳续约；到期未续约则 Leader 提交 `ExpireTask`，任务重新入队。

## 3. 模块划分

```
Maven 多模块工程，根包 `io.ytong.raftjobkeeper`：

job-keeper-common       领域模型、命令、protobuf/gRPC 定义
job-keeper-server       Server 节点（Spring Boot 应用，Raft 集群成员）
  ...server.api         REST 接口 + Worker gRPC 服务
  ...server.raft        Raft 节点封装（启动、加入、离开、Leader 监控）
  ...server.fsm         状态机（StateMachine）、命令处理、快照
  ...server.store       内存作业存储与索引（按下次触发时间的优先队列）
  ...server.scheduler   调度器（租约回收、重试）
  ...server.dispatcher  任务分派、租约管理、Worker 注册表
  ...server.cluster     成员发现（k8s DNS）、自动 bootstrap/join
  ...server.metrics     Prometheus 指标
job-keeper-worker       Worker 应用（运行时、执行器 shell/http/容器）
```

## 4. 数据模型

- **Job**：`id, name, payload(executor, command, args, env), timeout, max_retry, retry_backoff, concurrency_policy(ALLOW/FORBID/REPLACE), status(ENABLED/DISABLED/DELETED), version`
- **Task**（一次触发的执行实例）：`id, job_id, fire_time, status(PENDING/RUNNING/SUCCEEDED/FAILED/TIMEOUT/CANCELED), worker_id, lease_expire_at, attempt, result, started_at, finished_at`
- **Worker**：`id, address, labels, capacity, last_heartbeat, status(ONLINE/OFFLINE)`

任务状态机：

```
PENDING --assign--> RUNNING --report ok--> SUCCEEDED
                       |--report fail--> FAILED --(attempt<max_retry)--> PENDING
                       |--lease expire--> PENDING (attempt+1)
                       |--cancel--> CANCELED
```

## 5. Raft 命令（日志条目）

| 命令 | 触发方 | 作用 |
|------|--------|------|
| `CreateJob / UpdateJob / DeleteJob / EnableJob / DisableJob` | API | 作业管理 |
| `FireJob{job_id, fire_time, task_id}` | API（用户触发） | 生成 PENDING Task（`fire_time` 为 Leader 写入的触发时间） |
| `AssignTask{task_id, worker_id, lease_expire_at}` | Dispatcher | 分配任务 |
| `RenewLease{task_id, worker_id, lease_expire_at}` | 心跳 | 续约 |
| `ReportTask{task_id, status, result}` | Worker | 上报结果 |
| `ExpireTask{task_id, now}` | Scheduler | 租约超时回收 |
| `RegisterWorker / WorkerHeartbeat / RemoveWorker` | Worker | Worker 管理（心跳可批量合并以降低日志量） |

幂等：每个命令带 `request_id`/`task_id`，FSM 对重复应用忽略。`FireJob` 使用客户端传入的 `request_id` 去重，用户重试同一次触发不会产生多个 Task。

## 6. 关键流程

本节只描述本系统特有的流程；选举、日志复制等通用机制见文首的前置阅读。

以下例子使用统一的初始状态：

- 3 个 Server：`S0`（Leader）、`S1`、`S2`，当前任期 `term=3`，三者日志都已到 `index=100` 且全部应用。
- 2 个 Worker：`W1`（capacity=4，正在运行 3 个任务）、`W2`（capacity=4，正在运行 0 个任务），都已向 S0 注册，gRPC 流在线。
- 用户要创建并运行作业 `job-42`（执行 `backup.sh`，租约 30s，最多重试 2 次）。

### 6.0 数据存放在哪里（Task 元数据与调度）

以 `t-9001` 在 `FireJob(index=102)` 提交之后为例，S0、S1、S2 三台节点上都有：

| 位置 | 内容 | 是否集群一致 |
|------|------|-------------|
| Raft 日志（RocksDB，磁盘） | index=102 `FireJob{t-9001}` | 是，多数派提交，顺序一致 |
| FSM 内存（`InMemoryJobStore`） | `tasks[t-9001] = PENDING` | 是，各节点 apply 同样的日志，FSM 确定性，得到同样的状态 |
| 内存索引（`pendingTasks`、`leaseQueue`、`tasksByJob`） | 从 FSM 状态派生 | 不单独复制；每个节点 apply 时自己维护，重启或加载快照后重建 |
| 快照（磁盘，定期） | 某个 index 处 Job / Task / Worker 全量 | 是 |
| Worker 的 gRPC 流（`WorkerRegistry`） | 谁连在 Leader 上 | 否，仅 Leader 内存，Worker 重连后重建，不进 Raft |

原则：**读本地内存，写走 Raft。**
- Task 元数据不在某个专门的存储里，而是每个 Server 内存中各有一份，由 Raft 日志保证一致，因此 Follower 内存里也有 PENDING 的 Task，只是它们不调度。
- Scheduler / Dispatcher 只在 Leader 上运行，读 **Leader 本机** 的 `pendingTasks` 做决策（选哪个 Worker）；这个决策只是提议，必须提交 `AssignTask` 到 Raft，多数派提交后所有节点的 FSM 才把 Task 改成 RUNNING，并把它从 `pendingTasks` 移到 `leaseQueue`。之后才通过 gRPC 推送。
- 因此 Leader 切换时不需要迁移 Task 数据：新 Leader 的 FSM 已含全部状态，只需等 Worker 重连（见 6.4）。
- 代价：全部 Job / Task 都要装进每个节点的内存，所以必须有保留期清理（见 6.5）；数据量超过单机内存后需要分片（多 Raft Group），当前版本不支持。

### 6.1 写请求的通用路径
Client → 任意 Server → 若非 Leader 则返回 `NOT_LEADER` + Leader 地址 → Leader 通过 `node.apply()` 提交命令 → 多数派提交 → 各 Server 的 FSM 应用 → 返回。

**例 1：请求打到 Follower**
```
Client  --POST /jobs-->  S1(Follower)
S1      --307 Location: S0:8080-->  Client      （NOT_LEADER，附 Leader 地址）
Client  --POST /jobs-->  S0(Leader)             （重发，进入例 2）
```

### 6.2 提交作业：一个 Follower 因网络问题没收到日志

**例 2：S2 网络中断，创建 job-42**
```
t0  Client -> S0: POST /jobs {name: job-42, ...}
t1  S0 写本地日志：index=101, term=3, CreateJob(job-42)
t2  S0 并行发 AppendEntries(index=101) 给 S1、S2
t3  S1 写入日志并确认 ✔
    S2 网络中断，请求超时 ✘
t4  S0 + S1 = 2/3，达到多数派 → commitIndex=101
t5  S0、S1 的 FSM 应用 index=101，job-42 可见；S0 返回 200 给 Client
```
此时三台状态：

| 节点 | 日志末尾 | commitIndex | 能看到 job-42 |
|------|---------|-------------|--------------|
| S0 | 101 | 101 | 是 |
| S1 | 101 | 101 | 是（S1 在下一次心跳/AppendEntries 中得知 commitIndex=101 后应用） |
| S2 | 100 | 100 | 否 |

- **用户读 job-42**：默认 `consistency=strong`，请求走 Leader 的 ReadIndex，能读到；若直接读 S2 且 `stale=true`，读不到（旧数据），这是预期行为。
- **S2 恢复**：S0 重试 AppendEntries 时发现 S2 的 `matchIndex=100`，补发 index=101；S2 写入并应用后追平。
- **S2 落后太多**：如果 S2 断开期间 S0 已做过快照并截断了 index≤500 的日志，而 S2 只到 100，S0 改发 `InstallSnapshot`，S2 加载快照后再接收后续日志。

**例 3：S2 落后时 S0 崩溃（已提交的数据不丢）**
```
t6  S0 宕机。S1（日志到 101）与 S2（日志到 100）发起选举，term=4
t7  S2 向 S1 要票：S2 的最后日志 (term=3, index=100) 比 S1 的 (term=3, index=101) 旧
    → S1 拒绝投票（Raft 要求候选人日志至少和自己一样新）
t8  S1 向 S2 要票：S1 日志更新 → S2 投票；S1 获得 2/3 → S1 成为 term=4 的 Leader
t9  S1 复制 index=101 给 S2，job-42 在 S1、S2 上都存在
```
结论：committed 的 `CreateJob(job-42)` 一定在多数派上，选举规则保证新 Leader 必然持有它，所以不会丢。

**例 4：客户端超时重试（幂等）**
```
Client -> S0: POST /jobs/job-42/run  request_id=r-777
S0 已提交 FireJob(index=102)，但响应在网络上丢失，Client 超时
Client -> S0: 用同一个 request_id=r-777 重发
S0 提交 FireJob(index=103)，FSM 发现 r-777 已处理过 → 忽略并返回已有的 task_id
结果：job-42 只产生一个 Task，不会重复运行
```

### 6.3 触发运行与调度（Leader）

**谁在跑、数据放哪**

| 问题 | 回答 |
|------|------|
| 谁运行 Scheduler / Dispatcher？ | **只有 Leader**。`onLeaderStart(term)` 启动，`onLeaderStop` 立即停止。Follower 不调度、不接受 Worker 连接，只复制日志和 apply。 |
| 数据都在内存里，不用写 Raft 吗？ | **要写。** 内存只是 FSM 的运行时形态，不是数据的唯一来源。凡是改变 Task / Worker 状态的事件（触发、分派、续约、上报、回收），都必须先作为命令提交到 Raft，各节点 apply 后内存才变化。**只读不写的动作**（查看 `pendingTasks`、选择 Worker）才是纯内存操作。 |
| 为什么必须写 Raft？ | 如果“分给 W2”只改 Leader 内存，Leader 一崩，新 Leader 不知道 `t-9001` 已分出去，会再分一次给别人，造成重复执行；Follower 内存里也没有这个状态。 |
| 什么可以不写 Raft？ | Worker 的 gRPC 连接、Dispatcher 的临时计算、各类排序索引：它们能从 FSM 状态或 Worker 重连中重建。 |

判断标准：**这个信息在 Leader 切换后，新 Leader 还需要吗？需要就写 Raft，不需要（可重建）才留在内存。**

**Leader 上的运行方式**

Leader 上有三个相互独立的循环，都是“读本机内存 → 决定 → 提交 Raft → apply 后生效”：

| 循环 | 读什么（本机内存） | 提交什么命令 | 触发条件 |
|------|------------------|-------------|---------|
| API 线程（用户请求） | Job 表 | `FireJob` | 用户 `POST /jobs/{id}/run` |
| Dispatcher | `pendingTasks`、`WorkerRegistry`（Worker 容量与标签） | `AssignTask`，成功后 gRPC 推送 | `FireJob` apply 后唤醒，或每秒兜底扫描一次 |
| Reclaimer | `leaseQueue`（按 `leaseExpireAt` 排序）、Worker 心跳时间 | `ExpireTask` | 每秒扫描，到期的租约或离线的 Worker |

另外，Worker 的 gRPC 请求（心跳续约、结果上报）由 Leader 收到后分别提交 `RenewLease`、`ReportTask`。

**执行步骤**

1. 当选后等待历史日志应用完毕（SOFAJRaft 当选时自动提交 no-op 日志），再启动上述循环。
2. 用户 `POST /jobs/{id}/run`（带 `request_id`），Leader 提交 `FireJob`，应用后产生 PENDING Task。
3. Dispatcher 从本机 `pendingTasks` 取出 Task，选择有空闲容量的 ONLINE Worker（标签匹配 + 最少负载），提交 `AssignTask`，Task 变为 RUNNING 并带租约。
4. `AssignTask` 提交成功后，才通过 gRPC 双向流把任务推送给 Worker。
5. Worker 执行期间发送心跳续约（`RenewLease`），结束后 `ReportTask`。
6. Reclaimer 周期扫描过期租约与离线 Worker，提交 `ExpireTask`。

**例 5：正常流程，用户触发 job-42 并调度到 Worker**（接例 2，S2 已追平）
```
t0  Client -> S0: POST /jobs/job-42/run  request_id=r-777
t1  S0 提交 FireJob(index=102, task_id=t-9001, fire_time=T)   → 多数派提交
    FSM 应用：Task t-9001 状态 PENDING；Client 收到 {task_id: t-9001}
t2  Dispatcher 取出 t-9001，查 WorkerRegistry：
      W1 负载 3/4，W2 负载 0/4 → 按"最少负载"选 W2
t3  S0 提交 AssignTask(index=103, t-9001, worker=W2, lease_expire_at=T+30s)
    → 多数派提交；FSM 应用：t-9001 变 RUNNING，W2 running=1
t4  S0 通过 gRPC 流把 t-9001（含 task_id、attempt=1、command）推给 W2
t5  W2 执行 backup.sh；每 10s 心跳续约，S0 提交 RenewLease，lease_expire_at 顺延
t6  W2 完成，上报 ReportTask(t-9001, SUCCEEDED) → 提交 → Task 变 SUCCEEDED
```
关键点：**先把"分给 W2"写入 Raft 日志（t3），再推送（t4）**。这样不管 S0 在哪一步崩溃，新 Leader 都能从状态机里看到这个任务的真实状态。

**例 6：AssignTask 已提交，推送前 Leader 崩溃**
```
t3  AssignTask(index=103) 已提交：t-9001 RUNNING，worker=W2，租约到 T+30s
t3.5 S0 宕机，W2 根本没收到任务
t4  S1 当选 term=4，状态机里 t-9001 是 RUNNING；给予一个租约周期宽限（不立刻回收）
t5  W2 重连 S1，注册并续约——但 W2 上没有 t-9001，不会续约该任务
t6  T+30s 租约到期，S1 提交 ExpireTask(t-9001)：Task 回到 PENDING，attempt=2
t7  Dispatcher 重新选 Worker 并分派
```
代价：任务延迟约一个租约周期（30s）才重新执行，换来的是不会丢任务、不会被两个 Worker 同时领走。

**例 7：Worker 执行期间 Leader 切换**
```
W2 正在执行 t-9001（已运行 20s）
S0 宕机，S1 当选 term=4；W2 的 gRPC 流断开，指数退避后重连 S1
W2 重连成功，上报"我在运行 t-9001"，S1 提交 RenewLease → 租约顺延
任务继续执行，最终 ReportTask 成功；用户感知不到切换
```

**例 8：Worker 与 Leader 分区，任务被重复执行**
```
t0  t-9001 在 W2 上执行，租约到 T+30s
t1  W2 与 S0 之间网络断开（W2 本身没挂，仍在跑 backup.sh）
t2  T+30s 租约到期，S0 提交 ExpireTask → t-9001 回到 PENDING，attempt=2
t3  S0 把 t-9001（attempt=2）分给 W1，W1 开始执行
t4  此时 W1、W2 同时在执行同一个任务
t5  W2 网络恢复，上报 ReportTask(t-9001, attempt=1) → FSM 发现当前 attempt=2，
    拒绝这次上报；W2 收到"任务已被回收"后停止该任务
```
所以执行器必须幂等（例如备份写入以 `task_id` 命名的文件，重复写同一个文件而不是追加），并带 `task_id` + `attempt` 供去重。系统保证的是"至少执行一次、结果只采纳一次"，而不是任何时刻只有一个进程在跑。

**例 9：任务失败后重试**
```
W2 执行失败，上报 ReportTask(t-9001, FAILED)
FSM：attempt=1 < max_retry=2 → 回到 PENDING，attempt=2，按 retry_backoff 延迟后重新分派
第二次仍失败：attempt=2 已达上限 → 终态 FAILED，不再重试
```

**防止旧 Leader 继续调度（Leader 读一致性与 fencing）**

Dispatcher 读本机内存做决策，因此要保证“本机确实是最新的 Leader”，否则会基于旧状态分派：

1. **提交前**：分派前检查 `node.isLeader()` 且 FSM 已应用到本任期 no-op 日志；对关键决策可先 `readIndex()` 确认自己仍被多数派承认。
2. **提交时**：`AssignTask` 带 `term`。被分区的旧 Leader 提交不了（得不到多数派），Raft 会拒绝。
3. **推送时（fencing）**：gRPC 推送带 `term`，Worker 记住见过的最大 `term`，拒绝更小 `term` 的指令。

**例 10：被分区的旧 Leader 仍在推送**
```
t0  S0(term=3) 与 S1、S2 分区；S1 当选 term=4
t1  S0 还没发现自己已退位，仍在向 W1 推送 t-9100（term=3）
    但 S0 的 AssignTask 无法得到多数派，t-9100 在集群里仍是 PENDING
t2  S1(term=4) 把 t-9100 分给 W1，推送带 term=4；W1 记录 maxTerm=4
t3  S0 再向 W1 推送 term=3 的任务 → W1 发现 3 < 4，拒绝
```
没有 fencing 时，W1 可能同时执行 S0 与 S1 各自推来的同一个任务；有了它，旧 Leader 的指令会被 Worker 拒绝。

### 6.4 Leader 切换
新 Leader 重建内存索引（FSM 已含全部状态），对 RUNNING 任务给予一个租约周期的宽限，Worker 重连后续约，未续约的任务被回收重试；PENDING 任务重新进入分派。具体见例 3、例 6、例 7、例 10。

### 6.5 快照
FSM 实现 `onSnapshotSave()` / `onSnapshotLoad()`，以 protobuf 序列化 Job / Task / Worker，默认每 600s 触发一次。已完成且超过保留期（默认 7 天）的 Task 在快照前清理。

### 6.6 成员变更（K8s 场景）
- 首次启动：`server-0` 等待 `BOOTSTRAP_EXPECT` 个节点可解析后以完整节点列表引导；其他 Server 启动后向 Leader 调用 `Join`（`CliService.addPeer`）。
- 节点地址使用 StatefulSet 稳定 DNS：`raft-job-keeper-N.raft-job-keeper-headless.<ns>.svc`。
- 缩容时调用 `Leave`（`CliService.removePeer`）；禁止缩到少于多数派。

## 7. 一致性与可用性

- 写：线性一致，多数派提交。读：默认 ReadIndex 线性一致，可选 `stale=true` 读 Follower。
- 容错：3 节点容忍 1 故障，5 节点容忍 2。
- 网络分区：少数派侧无法提交，Leader 自动退位；少数派侧 Worker 租约到期后任务被重新分派，因此执行器必须幂等，并携带 `task_id` + `attempt` 供去重。

## 8. 可观测性与安全

- 指标：`raft_state`、`raft_commit_latency`、`jobs_total`、`tasks_by_status`、`task_schedule_lag_seconds`、`worker_online`。
- 日志：结构化 JSON；追踪 `task_id`。
- 安全：节点间与 Worker 通信启用 mTLS（cert-manager 签发）；API 使用 token / RBAC；容器以非 root 运行。

## 9. 测试策略

- 单元：FSM 命令幂等与确定性、Worker 选择策略。
- 集成：进程内 3 节点集群，注入 Leader 崩溃、分区、延迟（同 JVM 内多端口启动 3 个 `RaftGroupService`，用 JUnit 5 + Awaitility 验证；网络故障可用 toxiproxy）。
- 一致性：Jepsen 风格线性一致检查，验证“每次触发仅产生一个 Task”。
- 混沌：k8s 上随机删除 Pod。

## 10. 里程碑

1. M1：Raft 集群 + 作业 CRUD + 快照。
2. M2：调度器 + Worker + 租约。
3. M3：k8s 部署、指标、mTLS。
4. M4：重试、并发策略、Web 控制台。
