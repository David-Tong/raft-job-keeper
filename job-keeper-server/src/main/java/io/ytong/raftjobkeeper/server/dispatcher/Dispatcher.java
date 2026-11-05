package io.ytong.raftjobkeeper.server.dispatcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ytong.raftjobkeeper.common.command.Command;
import io.ytong.raftjobkeeper.common.command.CommandType;
import io.ytong.raftjobkeeper.common.grpc.ServerMessage;
import io.ytong.raftjobkeeper.common.grpc.TaskAssignment;
import io.ytong.raftjobkeeper.common.model.Job;
import io.ytong.raftjobkeeper.common.model.Task;
import io.ytong.raftjobkeeper.server.raft.RaftServer;
import io.ytong.raftjobkeeper.server.store.JobStore;
import org.springframework.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class Dispatcher {
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private final RaftServer raft;
    private final JobStore store;
    private final WorkerRegistry workers;
    private final ObjectMapper mapper;
    private final Duration leaseDuration;
    private final Duration taskRetention;
    private final Duration workerTimeout;
    private final Set<String> removingWorkers = ConcurrentHashMap.newKeySet();

    public Dispatcher(RaftServer raft, JobStore store, WorkerRegistry workers, ObjectMapper mapper,
                      @Value("${scheduler.lease-duration:30s}") Duration leaseDuration,
                      @Value("${scheduler.task-retention:168h}") Duration taskRetention,
                      @Value("${scheduler.worker-timeout:15s}") Duration workerTimeout) {
        this.raft = raft;
        this.store = store;
        this.workers = workers;
        this.mapper = mapper;
        this.leaseDuration = leaseDuration;
        this.taskRetention = taskRetention;
        this.workerTimeout = workerTimeout;
    }

    @Scheduled(fixedDelayString = "${scheduler.scan-interval-ms:1000}")
    public void scan() {
        if (!raft.isLeader()) {
            workers.closeAll(raft.leaderGrpcAddress());
            return;
        }
        Instant now = Instant.now();
        store.expiredTasks(now).forEach(task -> expire(task, now));
        store.staleWorkers(now.minus(workerTimeout)).forEach(worker -> removeWorker(worker.id(), now));
        store.canceledTasks().forEach(this::cancel);
        store.pendingTasks(now).forEach(this::dispatch);
    }

    private void cancel(Task task) {
        StreamObserverGuard.send(workers.stream(task.workerId()),
                ServerMessage.newBuilder().setType("CANCEL")
                        .setTask(TaskAssignment.newBuilder().setTaskId(task.id()).build())
                        .build());
    }

    private void removeWorker(String workerId, Instant now) {
        if (!removingWorkers.add(workerId)) {
            return;
        }
        var body = mapper.createObjectNode().put("workerId", workerId);
        raft.apply(new Command(CommandType.REMOVE_WORKER, UUID.randomUUID().toString(), now, body))
                .whenComplete((result, error) -> {
                    removingWorkers.remove(workerId);
                    if (error == null) {
                        return;
                    }
                    log.warn("Failed to remove stale worker {}", workerId, error);
                });
    }

    @Scheduled(fixedDelayString = "${scheduler.retention-scan-ms:3600000}")
    public void purgeHistory() {
        if (!raft.isLeader()) {
            return;
        }
        Instant now = Instant.now();
        Instant cutoff = now.minus(taskRetention);
        var body = mapper.createObjectNode().put("before", cutoff.toString());
        raft.apply(new Command(CommandType.PURGE_HISTORY,
                        "purge:" + cutoff.getEpochSecond(), now, body))
                .exceptionally(error -> {
                    log.warn("Failed to purge expired task history", error);
                    return null;
                });
    }

    private void expire(Task task, Instant now) {
        var body = mapper.createObjectNode().put("taskId", task.id());
        raft.apply(new Command(CommandType.EXPIRE_TASK,
                        "expire:" + task.id() + ":" + task.attempt(), now, body))
                .thenRun(() -> workers.releaseTask(task.id()))
                .exceptionally(error -> {
                    log.warn("Could not reclaim expired task {}", task.id(), error);
                    return null;
                });
    }

    private void dispatch(Task task) {
        WorkerRegistry.Session session = workers.leastLoaded();
        if (session == null) {
            return;
        }
        Job job = store.getJob(task.jobId());
        if (job == null) {
            return;
        }
        if (!workers.reserve(session.worker().id(), task.id())) {
            return;
        }
        Instant now = Instant.now();
        Instant expiry = now.plus(leaseDuration);
        var body = mapper.createObjectNode().put("taskId", task.id())
                .put("workerId", session.worker().id()).put("leaseExpireAt", expiry.toString());
        raft.apply(new Command(CommandType.ASSIGN_TASK, UUID.randomUUID().toString(), now, body))
                .thenAccept(result -> {
                    boolean sent = StreamObserverGuard.send(workers.stream(session.worker().id()),
                            ServerMessage.newBuilder().setType("TASK")
                                    .setTask(TaskAssignment.newBuilder()
                                            .setTaskId(task.id())
                                            .setJobId(job.id())
                                            .setExecutor(job.payload().executor())
                                            .setCommand(job.payload().command() == null ? "" : job.payload().command())
                                            .addAllArgs(job.payload().args())
                                            .putAllEnv(job.payload().env())
                                            .setAttempt(task.attempt())
                                            .setTimeoutMillis(job.timeout().toMillis())
                                            .setTerm(raft.currentTerm())
                                            .build())
                                    .build());
                    if (!sent) {
                        log.warn("Task {} is assigned but could not be pushed to worker {}",
                                task.id(), session.worker().id());
                    }
                })
                .exceptionally(error -> {
                    workers.releaseTask(task.id());
                    log.warn("Task {} could not be assigned", task.id(), error);
                    return null;
                });
    }

    private static final class StreamObserverGuard {
        static boolean send(io.grpc.stub.StreamObserver<ServerMessage> stream, ServerMessage message) {
            if (stream == null) {
                return false;
            }
            try {
                stream.onNext(message);
                return true;
            } catch (RuntimeException ignored) {
                LoggerFactory.getLogger(Dispatcher.class).warn("Unable to push task to worker stream", ignored);
                return false;
            }
        }
    }
}
