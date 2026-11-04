# Raft Job Keeper 部署设计（Docker 与 Kubernetes）

相关文件位于仓库根目录的 `deploy/`：

| 文件 | 说明 |
|------|------|
| `Dockerfile` | Server 节点镜像（多阶段构建） |
| `Dockerfile.worker` | Worker 镜像 |
| `k8s/00-namespace.yaml` | 命名空间 |
| `k8s/01-configmap.yaml` | 节点配置 |
| `k8s/02-services.yaml` | Headless Service（Raft 对等通信）与 ClusterIP Service（API） |
| `k8s/03-statefulset.yaml` | Server StatefulSet（3 副本，持久卷存放 Raft 日志与快照） |
| `k8s/04-pdb.yaml` | PodDisruptionBudget（保证多数派） |
| `k8s/05-worker-deployment.yaml` | Worker Deployment（可 HPA 扩缩容） |
| `k8s/06-networkpolicy.yaml` | 网络策略 |

## 1. 部署拓扑

- Server 使用 **StatefulSet**：稳定网络标识 + 稳定存储，是 Raft 节点的必要条件。
- Pod 名 `raft-job-keeper-{0,1,2}`，Raft 节点 ID 即 Pod 名，地址为
  `<pod>.raft-job-keeper-headless.raft-job-keeper.svc.cluster.local:7000`。
- 端口：`7000` Raft、`8080` REST API/管理接口/metrics、`9090` Worker gRPC。
- 数据目录 `/data`：SOFAJRaft 的 RocksDB 日志目录、元数据目录与 `snapshot/` 快照目录，由 PVC 提供，Pod 重建后不丢。
- `podManagementPolicy: OrderedReady`：`pod-0` 先启动并 bootstrap，其余 Pod 随后 Join。
- 副本数必须为奇数（3 或 5）；PDB `minAvailable: 2` 保证滚动升级与节点维护时多数派不丢失。
- `updateStrategy: RollingUpdate`，从高序号向低序号逐个更新；就绪探针要求节点已加入集群才放行流量，因此一次只会有一个节点重启。
- 对 API 的 ClusterIP Service 通过就绪探针 `/readyz` 工作；可选 `/leaderz` 仅对 Leader 返回 200，用于把写流量定向到 Leader。

## 2. 启动与成员管理

1. 启动参数通过环境变量传入：`NODE_ID=$(POD_NAME)`、`PEERS_DNS=<headless>`、`BOOTSTRAP_EXPECT=3`。
2. 若 `/data` 已有 Raft 状态：直接以原身份恢复。
3. 若无状态且序号为 0：等待 `BOOTSTRAP_EXPECT` 个对等节点 DNS 可解析后，使用完整节点列表 bootstrap（避免单节点脑裂式自举）；
4. 其他序号：轮询各 peer 的 `/leaderz` 找到 Leader，调用其 REST 管理接口 `POST /admin/cluster/join`。
5. 缩容时由运维调用 `POST /admin/cluster/leave` 摘除成员；滚动重启不应摘除成员，因此 preStop 钩子只做 `POST /admin/transfer-leadership`（用 `curl` 调本机 8080）。
6. `terminationGracePeriodSeconds: 60`，保证 Raft 刷盘与 Leader 主动转让（`transferLeadershipTo`）。

## 3. 资源与存储

- 每个 Server：requests `250m / 256Mi`，limits `1 / 1Gi`。
- JVM：容器内 `-XX:MaxRAMPercentage=70`，内存 limit 1Gi 时堆约 700MB；RocksDB 使用堆外内存，需预留。
- PVC：`10Gi`，使用低延迟 SSD 的 StorageClass（Raft fsync 延迟直接影响提交延迟）。
- 反亲和：`requiredDuringScheduling` 按 `kubernetes.io/hostname`，并建议加 `topology.kubernetes.io/zone` 的 spread 约束，使三个节点分散在不同故障域。

## 4. Worker

- 无状态 Deployment，副本数按负载伸缩（HPA 可基于 CPU 或自定义指标 `tasks_pending`）。
- 通过 `SERVER_ADDR=raft-job-keeper-api.raft-job-keeper.svc:9090` 发现集群，由 Server 返回当前 Leader 地址，Worker 再直连 Leader。
- Worker 退出时（SIGTERM）停止领取新任务，等待当前任务完成或超时后注销。

## 5. 安全

- 容器 `runAsNonRoot`、只读根文件系统、丢弃所有 capabilities。
- NetworkPolicy：仅允许 Server 之间访问 7000，Worker/客户端访问 9090/8080。
- 生产环境启用 mTLS（cert-manager 签发证书，挂载 Secret），本文示例未包含证书资源。

## 6. 构建与部署

```bash
# 构建镜像（在仓库根目录执行，需先有 Maven 多模块工程的 pom.xml）
docker build -f deploy/Dockerfile        -t raft-job-keeper:0.1.0 .
docker build -f deploy/Dockerfile.worker -t raft-job-keeper-worker:0.1.0 .

# 部署
kubectl apply -f deploy/k8s/

# 验证
kubectl -n raft-job-keeper get pods -w
kubectl -n raft-job-keeper exec raft-job-keeper-0 -- curl -s localhost:8080/admin/cluster/status
```

## 7. 运维要点

- **Leader 故障**：选举通常在 1~3 秒内完成，期间写请求失败并由客户端重试。
- **整集群丢失**：从 PVC 快照恢复；若多数节点数据永久丢失，需手工重置集群配置（SOFAJRaft 的 `CliService.resetPeers`），并接受可能的数据回退。
- **升级**：先升级 Follower 再升级 Leader；Raft 命令格式需向后兼容（版本号字段）。
- **备份**：定期由 CronJob 调用 `POST /admin/snapshot` 触发快照，并把快照目录导出到对象存储。
- **告警**：无 Leader 超过 10s、提交延迟 P99 > 500ms、调度延迟 > 30s、在线 Worker 为 0。

> 注：Dockerfile 假定仓库根目录为 Maven 多模块工程（`job-keeper-common`、`job-keeper-server`、`job-keeper-worker`），打包产物为 `job-keeper-server.jar`、`job-keeper-worker.jar`（需在 pom 中设置 `finalName`）；示例清单可在代码完成后微调。
