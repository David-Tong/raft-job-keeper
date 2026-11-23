# job-keeper-common

`job-keeper-common` 是 Raft Job Keeper 的共享模型与通信协议模块。Server 和
Worker 都依赖它；本模块不包含可独立运行的服务。

## 实现内容

- `common.model`：使用 Java record 表示 `Job`、`Task`、`Worker` 及其状态、
  执行载荷和并发策略。`Payload` 与 `Worker` 会将集合复制为不可变集合，并把
  `null` 集合规范化为空集合。
- `common.command`：`Command` 封装需要写入 Raft 日志的命令类型、请求 ID、时间
  和 JSON body。命令类型覆盖 Job 管理、任务调度与租约、Worker 管理及历史清理。
- `src/main/proto/worker.proto`：定义 Worker 与 Server 之间的 gRPC 双向流
  `WorkerService.Connect`，以及注册/心跳/结果消息和任务分配字段。
- Maven 在构建时通过 Protobuf 插件生成 Java 消息类和 gRPC stub；生成代码位于
  `target/`，不应手工维护。

## 构建与测试

在仓库根目录使用 JDK 21 和 Maven：

```shell
mvn -pl job-keeper-common -am test
mvn -pl job-keeper-common -am package
```

当前测试覆盖载荷与 Worker 标签集合的空值处理、防御性复制和不可变行为。

## 代码结构

```text
src/main/java/io/ytong/raftjobkeeper/common/
  command/  Raft 命令封装与命令类型
  model/    Job、Task、Worker 领域模型
src/main/proto/
  worker.proto  Worker gRPC 协议
```

本模块的设计背景与整个系统的构建和运行方式见仓库根目录的
[`README.md`](../README.md) 和 [`DESIGNS/`](../DESIGNS/)。
