package io.ytong.raftjobkeeper.server.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ytong.raftjobkeeper.common.command.Command;
import io.ytong.raftjobkeeper.common.command.CommandType;
import io.ytong.raftjobkeeper.common.model.*;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class JobStore {
    private final ObjectMapper mapper;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final Map<String, Task> tasks = new ConcurrentHashMap<>();
    private final Map<String, Worker> workers = new ConcurrentHashMap<>();
    private final Map<String, CachedResult> resultsByRequestId = new ConcurrentHashMap<>();

    public JobStore(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public synchronized JsonNode apply(Command command) {
        CachedResult cached = command.requestId() == null ? null : resultsByRequestId.get(command.requestId());
        if (cached != null) {
            return cached.result().deepCopy();
        }
        JsonNode body = command.body();
        JsonNode result = switch (command.type()) {
            case CREATE_JOB -> createJob(read(body, Job.class));
            case UPDATE_JOB -> updateJob(read(body, Job.class));
            case DELETE_JOB -> deleteJob(text(body, "jobId"));
            case SET_JOB_ENABLED -> setJobEnabled(text(body, "jobId"), body.path("enabled").asBoolean());
            case FIRE_JOB -> fireJob(text(body, "jobId"), text(body, "taskId"), command.now());
            case ASSIGN_TASK -> assignTask(body, command.now());
            case RENEW_LEASE -> renewLease(body);
            case REPORT_TASK -> reportTask(body, command.now());
            case EXPIRE_TASK -> expireTask(text(body, "taskId"), command.now());
            case REGISTER_WORKER, WORKER_HEARTBEAT -> registerWorker(read(body, Worker.class), command.now());
            case REMOVE_WORKER -> removeWorker(text(body, "workerId"));
            case PURGE_HISTORY -> purgeHistory(Instant.parse(text(body, "before")));
        };
        if (command.requestId() != null) {
            resultsByRequestId.put(command.requestId(), new CachedResult(result.deepCopy(), command.now()));
        }
        return result;
    }

    private JsonNode createJob(Job requested) {
        if (jobs.containsKey(requested.id())) {
            throw new IllegalStateException("Job already exists: " + requested.id());
        }
        Job job = new Job(requested.id(), requested.name(), requested.payload(), requested.timeout(),
                requested.maxRetry(), requested.retryBackoff(), requested.concurrency(), JobStatus.ENABLED, 1);
        jobs.put(job.id(), job);
        return mapper.valueToTree(job);
    }

    private JsonNode updateJob(Job requested) {
        Job current = requireJob(requested.id());
        if (current.status() == JobStatus.DELETED) {
            throw new IllegalStateException("Deleted jobs cannot be updated");
        }
        Job job = new Job(current.id(), requested.name(), requested.payload(), requested.timeout(),
                requested.maxRetry(), requested.retryBackoff(), requested.concurrency(), current.status(),
                current.version() + 1);
        jobs.put(job.id(), job);
        return mapper.valueToTree(job);
    }

    private JsonNode deleteJob(String id) {
        Job job = requireJob(id);
        Job deleted = new Job(job.id(), job.name(), job.payload(), job.timeout(), job.maxRetry(),
                job.retryBackoff(), job.concurrency(), JobStatus.DELETED, job.version() + 1);
        jobs.put(id, deleted);
        return mapper.valueToTree(deleted);
    }

    private JsonNode setJobEnabled(String id, boolean enabled) {
        Job job = requireJob(id);
        if (job.status() == JobStatus.DELETED) {
            throw new IllegalStateException("Deleted jobs cannot be enabled or disabled");
        }
        Job changed = new Job(job.id(), job.name(), job.payload(), job.timeout(), job.maxRetry(),
                job.retryBackoff(), job.concurrency(), enabled ? JobStatus.ENABLED : JobStatus.DISABLED,
                job.version() + 1);
        jobs.put(id, changed);
        return mapper.valueToTree(changed);
    }

    private JsonNode fireJob(String jobId, String taskId, Instant now) {
        Job job = requireJob(jobId);
        if (job.status() != JobStatus.ENABLED) {
            throw new IllegalStateException("Only enabled jobs can be triggered");
        }
        List<Task> activeTasks = tasks.values().stream()
                .filter(task -> task.jobId().equals(jobId))
                .filter(task -> task.status() == TaskStatus.PENDING || task.status() == TaskStatus.RUNNING)
                .toList();
        if (job.concurrency() == ConcurrencyPolicy.FORBID && !activeTasks.isEmpty()) {
            throw new IllegalStateException("Job already has an active task");
        }
        if (job.concurrency() == ConcurrencyPolicy.REPLACE) {
            for (Task active : activeTasks) {
                tasks.put(active.id(), new Task(active.id(), active.jobId(), active.fireTime(),
                        TaskStatus.CANCELED, active.workerId(), null, active.attempt(),
                        "Replaced by a newer run", active.startedAt(), now, active.availableAt()));
            }
        }
        Task task = new Task(taskId, jobId, now, TaskStatus.PENDING, null, null, 1, null, null, null, now);
        tasks.put(task.id(), task);
        return mapper.valueToTree(task);
    }

    private JsonNode assignTask(JsonNode body, Instant now) {
        String taskId = text(body, "taskId");
        Task task = requireTask(taskId);
        if (task.status() != TaskStatus.PENDING) {
            throw new IllegalStateException("Only pending tasks can be assigned");
        }
        String workerId = text(body, "workerId");
        Instant expiry = Instant.parse(text(body, "leaseExpireAt"));
        Task assigned = new Task(task.id(), task.jobId(), task.fireTime(), TaskStatus.RUNNING,
                workerId, expiry, task.attempt(), null, now, null, task.availableAt());
        tasks.put(taskId, assigned);
        return mapper.valueToTree(assigned);
    }

    private JsonNode renewLease(JsonNode body) {
        String taskId = text(body, "taskId");
        Task task = requireTask(taskId);
        if (task.status() != TaskStatus.RUNNING || !Objects.equals(task.workerId(), text(body, "workerId"))) {
            throw new IllegalStateException("Task is not running on this worker");
        }
        Task renewed = new Task(task.id(), task.jobId(), task.fireTime(), task.status(), task.workerId(),
                Instant.parse(text(body, "leaseExpireAt")), task.attempt(), task.result(), task.startedAt(),
                task.finishedAt(), task.availableAt());
        tasks.put(taskId, renewed);
        return mapper.valueToTree(renewed);
    }

    private JsonNode reportTask(JsonNode body, Instant now) {
        String taskId = text(body, "taskId");
        Task task = requireTask(taskId);
        if (task.status() != TaskStatus.RUNNING
                || !Objects.equals(task.workerId(), text(body, "workerId"))
                || task.attempt() != body.path("attempt").asInt()) {
            throw new IllegalStateException("Task assignment is no longer current");
        }
        TaskStatus reported = TaskStatus.valueOf(text(body, "status"));
        if (reported != TaskStatus.SUCCEEDED && reported != TaskStatus.FAILED && reported != TaskStatus.TIMEOUT) {
            throw new IllegalArgumentException("Invalid reported task status");
        }
        String result = body.path("result").isNull() ? null : body.path("result").asText();
        Job job = requireJob(task.jobId());
        Task updated;
        if (reported != TaskStatus.SUCCEEDED && task.attempt() <= job.maxRetry()) {
            updated = new Task(task.id(), task.jobId(), task.fireTime(), TaskStatus.PENDING, null, null,
                    task.attempt() + 1, result, null, null, now.plus(job.retryBackoff()));
        } else {
            updated = new Task(task.id(), task.jobId(), task.fireTime(), reported, task.workerId(), null,
                    task.attempt(), result, task.startedAt(), now, now);
        }
        tasks.put(taskId, updated);
        return mapper.valueToTree(updated);
    }

    private JsonNode expireTask(String taskId, Instant now) {
        Task task = requireTask(taskId);
        if (task.status() != TaskStatus.RUNNING || task.leaseExpireAt() == null
                || task.leaseExpireAt().isAfter(now)) {
            return mapper.valueToTree(task);
        }
        Job job = requireJob(task.jobId());
        Task expired = task.attempt() <= job.maxRetry()
                ? new Task(task.id(), task.jobId(), task.fireTime(), TaskStatus.PENDING, null, null,
                        task.attempt() + 1, "Lease expired", null, null, now.plus(job.retryBackoff()))
                : new Task(task.id(), task.jobId(), task.fireTime(), TaskStatus.FAILED, task.workerId(), null,
                        task.attempt(), "Lease expired", task.startedAt(), now, now);
        tasks.put(taskId, expired);
        return mapper.valueToTree(expired);
    }

    private JsonNode registerWorker(Worker requested, Instant now) {
        if (requested.id() == null || requested.id().isBlank() || requested.capacity() < 1) {
            throw new IllegalArgumentException("Worker id and positive capacity are required");
        }
        Worker current = workers.get(requested.id());
        Worker worker = new Worker(requested.id(), requested.address(), requested.labels(), requested.capacity(),
                current == null ? 0 : current.running(), now, WorkerStatus.ONLINE);
        workers.put(worker.id(), worker);
        return mapper.valueToTree(worker);
    }

    private JsonNode removeWorker(String workerId) {
        Worker removed = workers.remove(workerId);
        return mapper.valueToTree(removed);
    }

    private JsonNode purgeHistory(Instant before) {
        int taskCountBefore = tasks.size();
        tasks.values().removeIf(task -> isTerminal(task.status()) && task.finishedAt() != null
                && task.finishedAt().isBefore(before));
        resultsByRequestId.values().removeIf(cached -> cached.processedAt().isBefore(before));
        return mapper.createObjectNode().put("tasksRemoved", taskCountBefore - tasks.size());
    }

    public Job getJob(String id) {
        Job job = jobs.get(id);
        return job == null || job.status() == JobStatus.DELETED ? null : job;
    }

    public List<Job> listJobs() {
        return jobs.values().stream().filter(job -> job.status() != JobStatus.DELETED)
                .sorted(Comparator.comparing(Job::id)).toList();
    }

    public Task getTask(String id) {
        return tasks.get(id);
    }

    public List<Task> listTasks() {
        return tasks.values().stream().sorted(Comparator.comparing(Task::fireTime).reversed()).toList();
    }

    public List<Task> pendingTasks() {
        return pendingTasks(Instant.MAX);
    }

    public List<Task> pendingTasks(Instant now) {
        return tasks.values().stream().filter(task -> task.status() == TaskStatus.PENDING)
                .filter(task -> task.availableAt() == null || !task.availableAt().isAfter(now))
                .sorted(Comparator.comparing(Task::fireTime)).toList();
    }

    public List<Task> canceledTasks() {
        return tasks.values().stream().filter(task -> task.status() == TaskStatus.CANCELED
                && task.workerId() != null).toList();
    }

    public List<Task> expiredTasks(Instant now) {
        return tasks.values().stream().filter(task -> task.status() == TaskStatus.RUNNING
                && task.leaseExpireAt() != null && !task.leaseExpireAt().isAfter(now)).toList();
    }

    public List<Worker> onlineWorkers() {
        return workers.values().stream().filter(worker -> worker.status() == WorkerStatus.ONLINE).toList();
    }

    public List<Worker> staleWorkers(Instant lastHeartbeatBefore) {
        return workers.values().stream()
                .filter(worker -> worker.lastHeartbeat() != null
                        && worker.lastHeartbeat().isBefore(lastHeartbeatBefore))
                .toList();
    }

    public synchronized void restore(Snapshot snapshot) {
        jobs.clear();
        tasks.clear();
        workers.clear();
        resultsByRequestId.clear();
        jobs.putAll(snapshot.jobs());
        tasks.putAll(snapshot.tasks());
        workers.putAll(snapshot.workers());
        resultsByRequestId.putAll(snapshot.resultsByRequestId());
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(Map.copyOf(jobs), Map.copyOf(tasks), Map.copyOf(workers),
                Map.copyOf(resultsByRequestId));
    }

    private Job requireJob(String id) {
        Job job = jobs.get(id);
        if (job == null || job.status() == JobStatus.DELETED) {
            throw new IllegalArgumentException("Job not found: " + id);
        }
        return job;
    }

    private Task requireTask(String id) {
        Task task = tasks.get(id);
        if (task == null) {
            throw new IllegalArgumentException("Task not found: " + id);
        }
        return task;
    }

    private <T> T read(JsonNode node, Class<T> type) {
        return mapper.convertValue(node, type);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || value.asText().isBlank()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return value.asText();
    }

    private static boolean isTerminal(TaskStatus status) {
        return status == TaskStatus.SUCCEEDED || status == TaskStatus.FAILED
                || status == TaskStatus.TIMEOUT || status == TaskStatus.CANCELED;
    }

    public record CachedResult(JsonNode result, Instant processedAt) {
    }

    public record Snapshot(Map<String, Job> jobs, Map<String, Task> tasks, Map<String, Worker> workers,
                           Map<String, CachedResult> resultsByRequestId) {
    }
}
