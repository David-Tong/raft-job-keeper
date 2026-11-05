package io.ytong.raftjobkeeper.worker.runtime;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.ytong.raftjobkeeper.common.grpc.ServerMessage;
import io.ytong.raftjobkeeper.common.grpc.TaskAssignment;
import io.ytong.raftjobkeeper.common.grpc.WorkerMessage;
import io.ytong.raftjobkeeper.common.grpc.WorkerServiceGrpc;
import io.ytong.raftjobkeeper.worker.executor.Executor;
import io.ytong.raftjobkeeper.worker.executor.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class WorkerRuntime {
    private static final Logger log = LoggerFactory.getLogger(WorkerRuntime.class);

    private final String workerId;
    private final String serverAddress;
    private final String workerToken;
    private final int capacity;
    private final Map<String, Executor> executors;
    private final ConcurrentMap<String, RunningTask> running = new ConcurrentHashMap<>();
    private final AtomicLong highestTerm = new AtomicLong();
    private final ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor();
    private final java.util.concurrent.ExecutorService taskPool;
    private volatile boolean stopping;
    private volatile Thread connectionThread;

    public WorkerRuntime(
            List<Executor> executors,
            @Value("${WORKER_ID:${HOSTNAME:worker}-${random.uuid}}") String workerId,
            @Value("${SERVER_ADDR:localhost:9090}") String serverAddress,
            @Value("${WORKER_TOKEN:${API_TOKEN:}}") String workerToken,
            @Value("${WORKER_CAPACITY:4}") int capacity) {
        this.workerId = workerId;
        this.serverAddress = serverAddress;
        this.workerToken = workerToken;
        this.capacity = capacity;
        this.executors = executors.stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                item -> item.type().toLowerCase(java.util.Locale.ROOT), item -> item));
        this.taskPool = Executors.newFixedThreadPool(capacity);
    }

    public void start() {
        if (capacity < 1) {
            throw new IllegalArgumentException("WORKER_CAPACITY must be positive");
        }
        heartbeats.scheduleAtFixedRate(this::sendHeartbeats, 5, 5, TimeUnit.SECONDS);
        connectionThread = new Thread(() -> connectLoop(serverAddress), "worker-connection");
        connectionThread.setDaemon(false);
        connectionThread.start();
    }

    private void connectLoop(String initialAddress) {
        String address = initialAddress;
        long backoffSeconds = 1;
        while (!stopping) {
            try {
                String redirect = connectAndWait(address);
                if (redirect != null && !redirect.isBlank()) {
                    address = redirect;
                }
                backoffSeconds = 1;
            } catch (Exception exception) {
                log.warn("Worker connection to {} failed: {}", address, exception.getMessage());
            }
            if (!stopping) {
                try {
                    Thread.sleep(Duration.ofSeconds(backoffSeconds).toMillis());
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    return;
                }
                backoffSeconds = Math.min(30, backoffSeconds * 2);
            }
        }
    }

    private String connectAndWait(String address) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        CountDownLatch closed = new CountDownLatch(1);
        CompletableFuture<String> redirect = new CompletableFuture<>();
        Connection connection = new Connection();
        StreamObserver<WorkerMessage> requests = WorkerServiceGrpc.newStub(channel).connect(new StreamObserver<>() {
            @Override
            public void onNext(ServerMessage message) {
                switch (message.getType()) {
                    case "TASK" -> accept(message.getTask());
                    case "CANCEL" -> cancel(message.getTask().getTaskId());
                    case "REDIRECT" -> {
                        if (!message.getLeader().isBlank()) {
                            redirect.complete(message.getLeader());
                        }
                    }
                    case "REGISTERED", "ACK" -> {
                    }
                    case "STALE" -> log.info("Worker received a stale task response; current leader is {}",
                            message.getLeader());
                    default -> log.warn("Unknown server message type: {}", message.getType());
                }
            }

            @Override
            public void onError(Throwable error) {
                log.warn("Worker gRPC stream closed: {}", error.getMessage());
                closed.countDown();
            }

            @Override
            public void onCompleted() {
                closed.countDown();
            }
        });
        connection.requests = requests;
        activeConnection = connection;
        try {
            send(requests, WorkerMessage.newBuilder().setType("REGISTER").setWorkerId(workerId)
                    .setAddress(workerId).setCapacity(capacity).setToken(workerToken).build());
            closed.await();
            return redirect.getNow(null);
        } finally {
            activeConnection = null;
            channel.shutdownNow();
        }
    }

    private void accept(TaskAssignment task) {
        long maxTerm = highestTerm.get();
        if (task.getTerm() < maxTerm) {
            log.warn("Rejected task {} from stale leader term {}", task.getTaskId(), task.getTerm());
            return;
        }
        highestTerm.accumulateAndGet(task.getTerm(), Math::max);
        if (running.size() >= capacity || running.containsKey(task.getTaskId())) {
            log.warn("Rejected duplicate or over-capacity task {}", task.getTaskId());
            return;
        }
        RunningTask state = new RunningTask(task);
        if (running.putIfAbsent(task.getTaskId(), state) != null) {
            return;
        }
        Future<?> future = taskPool.submit(() -> execute(state));
        state.setFuture(future);
    }

    private void execute(RunningTask state) {
        state.markStarted();
        TaskResult result;
        try {
            Executor executor = executors.get(state.assignment().getExecutor().toLowerCase(java.util.Locale.ROOT));
            if (executor == null) {
                throw new IllegalArgumentException("Unsupported executor: " + state.assignment().getExecutor());
            }
            result = executor.execute(state.assignment());
        } catch (Exception exception) {
            result = state.cancelled()
                    ? new TaskResult("CANCELED", "Task was canceled")
                    : new TaskResult("FAILED", exception.getMessage() == null
                            ? exception.getClass().getSimpleName() : exception.getMessage());
        } finally {
            running.remove(state.assignment().getTaskId(), state);
        }
        state.reportOnce(result);
    }

    private void cancel(String taskId) {
        RunningTask task = running.get(taskId);
        if (task != null) {
            task.cancel();
        }
    }

    private void sendHeartbeats() {
        for (RunningTask task : running.values()) {
            task.send(WorkerMessage.newBuilder().setType("HEARTBEAT").setWorkerId(workerId)
                    .setAddress(workerId).setCapacity(capacity)
                    .setTaskId(task.assignment().getTaskId()).setAttempt(task.assignment().getAttempt()).build());
        }
        if (running.isEmpty()) {
            Connection current = activeConnection;
            if (current != null) {
                current.send(WorkerMessage.newBuilder().setType("HEARTBEAT").setWorkerId(workerId)
                        .setAddress(workerId).setCapacity(capacity).build());
            }
        }
    }

    private static void send(StreamObserver<WorkerMessage> stream, WorkerMessage message) {
        synchronized (stream) {
            stream.onNext(message);
        }
    }

    public void stop() {
        stopping = true;
        heartbeats.shutdownNow();
        taskPool.shutdown();
        try {
            if (!taskPool.awaitTermination(30, TimeUnit.SECONDS)) {
                taskPool.shutdownNow();
            }
        } catch (InterruptedException exception) {
            taskPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        Thread thread = connectionThread;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private final class RunningTask {
        private final TaskAssignment assignment;
        private volatile Future<?> future;
        private final AtomicBoolean canceled = new AtomicBoolean();
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean reported = new AtomicBoolean();

        private RunningTask(TaskAssignment assignment) {
            this.assignment = assignment;
        }

        TaskAssignment assignment() {
            return assignment;
        }

        void setFuture(Future<?> value) {
            future = value;
            if (canceled.get()) {
                value.cancel(true);
                reportCanceledIfNotStarted();
            }
        }

        void markStarted() {
            started.set(true);
        }

        boolean cancelled() {
            return canceled.get();
        }

        void cancel() {
            canceled.set(true);
            Future<?> current = future;
            if (current != null) {
                current.cancel(true);
                reportCanceledIfNotStarted();
            }
        }

        private void reportCanceledIfNotStarted() {
            if (!started.get()) {
                running.remove(assignment.getTaskId(), this);
                reportOnce(new TaskResult("CANCELED", "Task was canceled"));
            }
        }

        void send(WorkerMessage heartbeat) {
            Connection current = activeConnection;
            if (current != null) {
                current.send(heartbeat);
            }
        }

        void reportOnce(TaskResult result) {
            if (!reported.compareAndSet(false, true)) {
                return;
            }
            Connection current = activeConnection;
            if (current != null) {
                current.send(WorkerMessage.newBuilder().setType("RESULT").setWorkerId(workerId)
                        .setTaskId(assignment.getTaskId()).setAttempt(assignment.getAttempt())
                        .setStatus(result.status()).setResult(result.output()).build());
            }
        }
    }

    private volatile Connection activeConnection;

    private final class Connection {
        private volatile StreamObserver<WorkerMessage> requests;

        void send(WorkerMessage message) {
            StreamObserver<WorkerMessage> stream = requests;
            if (stream != null) {
                WorkerRuntime.send(stream, message);
            }
        }
    }
}
