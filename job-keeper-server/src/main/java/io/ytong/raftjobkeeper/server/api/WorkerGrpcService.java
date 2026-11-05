package io.ytong.raftjobkeeper.server.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.stub.StreamObserver;
import io.ytong.raftjobkeeper.common.command.Command;
import io.ytong.raftjobkeeper.common.command.CommandType;
import io.ytong.raftjobkeeper.common.grpc.ServerMessage;
import io.ytong.raftjobkeeper.common.grpc.WorkerMessage;
import io.ytong.raftjobkeeper.common.grpc.WorkerServiceGrpc;
import io.ytong.raftjobkeeper.common.model.Worker;
import io.ytong.raftjobkeeper.common.model.WorkerStatus;
import io.ytong.raftjobkeeper.server.dispatcher.WorkerRegistry;
import io.ytong.raftjobkeeper.server.raft.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

@Component
public class WorkerGrpcService extends WorkerServiceGrpc.WorkerServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(WorkerGrpcService.class);

    private final RaftServer raft;
    private final WorkerRegistry registry;
    private final ObjectMapper mapper;
    private final Duration leaseDuration;
    private final String workerToken;

    public WorkerGrpcService(RaftServer raft, WorkerRegistry registry, ObjectMapper mapper,
                             @Value("${scheduler.lease-duration:30s}") Duration leaseDuration,
                             @Value("${WORKER_TOKEN:${API_TOKEN:}}") String workerToken) {
        this.raft = raft;
        this.registry = registry;
        this.mapper = mapper;
        this.leaseDuration = leaseDuration;
        this.workerToken = workerToken;
    }

    @Override
    public StreamObserver<WorkerMessage> connect(StreamObserver<ServerMessage> response) {
        return new StreamObserver<>() {
            private String workerId;
            private volatile boolean registered;

            @Override
            public void onNext(WorkerMessage message) {
                try {
                    if (!registered) {
                        register(message);
                        return;
                    }
                    switch (message.getType()) {
                        case "HEARTBEAT" -> heartbeat(message, response);
                        case "RESULT" -> report(message, response);
                        default -> response.onError(new IllegalArgumentException(
                                "Unsupported worker message type: " + message.getType()));
                    }
                } catch (RuntimeException exception) {
                    response.onError(exception);
                }
            }

            private void register(WorkerMessage message) {
                if (!"REGISTER".equals(message.getType()) || message.getWorkerId().isBlank()) {
                    throw new IllegalArgumentException("The first worker message must register a worker");
                }
                if (workerToken.isBlank() || !MessageDigest.isEqual(
                        workerToken.getBytes(StandardCharsets.UTF_8),
                        message.getToken().getBytes(StandardCharsets.UTF_8))) {
                    throw new SecurityException("Worker token is invalid or not configured");
                }
                if (!raft.isLeader()) {
                    response.onNext(ServerMessage.newBuilder().setType("REDIRECT")
                            .setLeader(raft.leaderGrpcAddress() == null ? "" : raft.leaderGrpcAddress()).build());
                    response.onCompleted();
                    return;
                }
                workerId = message.getWorkerId();
                Worker worker = new Worker(workerId, message.getAddress(), message.getLabelsMap(),
                        message.getCapacity(), 0, Instant.now(), WorkerStatus.ONLINE);
                raft.apply(new Command(CommandType.REGISTER_WORKER, UUID.randomUUID().toString(),
                                Instant.now(), mapper.valueToTree(worker)))
                        .whenComplete((result, error) -> {
                            if (error != null) {
                                response.onError(error);
                                return;
                            }
                            registered = true;
                            registry.register(worker, response);
                            response.onNext(ServerMessage.newBuilder().setType("REGISTERED").build());
                        });
            }

            private void heartbeat(WorkerMessage message, StreamObserver<ServerMessage> response) {
                Instant now = Instant.now();
                Worker current = new Worker(workerId, message.getAddress(), message.getLabelsMap(),
                        Math.max(1, message.getCapacity()), 0, now, WorkerStatus.ONLINE);
                raft.apply(new Command(CommandType.WORKER_HEARTBEAT, UUID.randomUUID().toString(), now,
                                mapper.valueToTree(current)))
                        .exceptionally(error -> {
                            log.warn("Failed to persist heartbeat for worker {}", workerId, error);
                            return null;
                        });
                if (!message.getTaskId().isBlank()) {
                    var body = mapper.createObjectNode().put("taskId", message.getTaskId())
                            .put("workerId", workerId)
                            .put("leaseExpireAt", now.plus(leaseDuration).toString());
                    raft.apply(new Command(CommandType.RENEW_LEASE, UUID.randomUUID().toString(), now, body))
                            .exceptionally(error -> {
                                log.warn("Failed to renew lease for task {}", message.getTaskId(), error);
                                return null;
                            });
                }
            }

            private void report(WorkerMessage message, StreamObserver<ServerMessage> response) {
                Instant now = Instant.now();
                var body = mapper.createObjectNode().put("taskId", message.getTaskId())
                        .put("workerId", workerId).put("attempt", message.getAttempt())
                        .put("status", message.getStatus()).put("result", message.getResult());
                raft.apply(new Command(CommandType.REPORT_TASK, UUID.randomUUID().toString(), now, body))
                        .whenComplete((result, error) -> {
                            registry.releaseTask(message.getTaskId());
                            if (error == null) {
                                response.onNext(ServerMessage.newBuilder().setType("ACK").build());
                            } else {
                            response.onNext(ServerMessage.newBuilder().setType("STALE")
                                    .setLeader(raft.leaderGrpcAddress() == null ? "" : raft.leaderGrpcAddress()).build());
                        }
                        });
            }

            @Override
            public void onError(Throwable error) {
                if (registered) {
                    registry.remove(workerId, response);
                }
            }

            @Override
            public void onCompleted() {
                if (registered) {
                    registry.remove(workerId, response);
                }
                response.onCompleted();
            }
        };
    }
}
