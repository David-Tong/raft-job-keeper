# Raft Job Keeper

Raft Job Keeper is a Java 21, Maven multi-module job scheduler built around SOFAJRaft.
Every server runs the same Raft state machine and REST API; only the elected leader
dispatches tasks. Workers are separate processes and communicate with the leader over
gRPC streams.

## Modules

- `job-keeper-common`: domain records, Raft command envelope, and worker gRPC protocol.
- `job-keeper-server`: Spring Boot REST API, SOFAJRaft node/FSM, snapshots, lease reclaimer,
  task dispatcher, and worker gRPC service.
- `job-keeper-worker`: reconnecting worker runtime and shell task executor.

## Build

Use JDK 21 and Maven 3.9 or newer:

```shell
mvn -B verify
```

The server stores Raft logs, metadata, and snapshots under `/data` by default. For local
development, set a writable `RAFT_DATA_DIR`, `API_TOKEN`, and `WORKER_TOKEN`, then start a
single-node server:

```shell
mvn -pl job-keeper-server -am package
JAVA_TOOL_OPTIONS="--add-opens=java.base/java.util=ALL-UNNAMED" \
  java -jar job-keeper-server/target/job-keeper-server.jar \
  --raft.data-dir=./data --raft.self-address=localhost:7000 --raft.initial-peers=localhost:7000
```

Start a worker in another process:

```shell
WORKER_ID=worker-1 SERVER_ADDR=localhost:9090 WORKER_TOKEN=dev-token \
  java -jar job-keeper-worker/target/job-keeper-worker.jar
```

Use the same bearer token configured as `API_TOKEN` for API write/admin requests:

```shell
curl -H "Authorization: Bearer dev-token" -H "Content-Type: application/json" \
  -d '{"name":"example","payload":{"executor":"shell","command":"echo","args":["hello"]}}' \
  http://localhost:8080/jobs
```

`GET /jobs`, `GET /tasks`, and individual resource reads are linearizable by default.
Pass `?consistency=stale` to read a follower's local state. Use the `Idempotency-Key`
header when calling `POST /jobs/{id}/run` so client retries cannot create duplicate tasks.

## Configuration

| Setting | Default | Description |
|---|---|---|
| `raft.data-dir` / `RAFT_DATA_DIR` | `/data` | Persistent Raft data directory |
| `raft.self-address` / `ADVERTISE_ADDR` | `localhost:7000` | This node's advertised Raft address |
| `raft.initial-peers` / `RAFT_INITIAL_PEERS` | `localhost:7000` | Comma-separated Raft peer addresses |
| `scheduler.lease-duration` | `30s` | Worker task lease |
| `grpc.port` | `9090` | Worker gRPC service port |
| `API_TOKEN` | unset | Required bearer token for REST writes/admin |
| `WORKER_TOKEN` | unset | Required token on worker registration |

For multi-node deployments all servers must use the same `raft.initial-peers` list and
stable per-node `ADVERTISE_ADDR` values. Keep the peer set odd-sized and provision a
persistent volume for each server. Kubernetes manifests expect the `raft-job-keeper-auth`
Secret with `api-token` and `worker-token` keys to be provisioned before deployment.

The worker's `shell` executor executes the configured executable directly with
`ProcessBuilder` (it does not implicitly invoke a shell). Treat job creation as a
privileged operation and restrict API token access accordingly. The `container`
executor is not enabled yet.

## API

- `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `PUT /jobs/{id}`, `DELETE /jobs/{id}`
- `POST /jobs/{id}/enable`, `POST /jobs/{id}/disable`, `POST /jobs/{id}/run`
- `GET /tasks`, `GET /tasks/{id}`
- Worker gRPC bidirectional stream on port `9090`
- `GET /healthz`, `GET /readyz`, `GET /leaderz`, `GET /metrics`
- `GET /admin/cluster/status`, `POST /admin/snapshot`
- `POST /admin/cluster/join`, `POST /admin/cluster/leave`,
  `POST /admin/transfer-leadership`

Deployment assets are in [`deploy/`](./deploy/); Maven artifact names match the Dockerfiles.
