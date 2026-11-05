package io.ytong.raftjobkeeper.server.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ytong.raftjobkeeper.common.command.Command;
import io.ytong.raftjobkeeper.common.command.CommandType;
import io.ytong.raftjobkeeper.common.model.*;
import io.ytong.raftjobkeeper.server.raft.RaftServer;
import io.ytong.raftjobkeeper.server.store.JobStore;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping
public class JobController {
    private final RaftServer raft;
    private final JobStore store;
    private final ObjectMapper mapper;

    public JobController(RaftServer raft, JobStore store, ObjectMapper mapper) {
        this.raft = raft;
        this.store = store;
        this.mapper = mapper;
    }

    @PostMapping("/jobs")
    @ResponseStatus(HttpStatus.CREATED)
    public CompletableFuture<JsonNode> createJob(
            @Valid @RequestBody JobRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String requestId) {
        String id = UUID.randomUUID().toString();
        Job job = new Job(id, request.name(), request.payload(), request.timeout(), request.maxRetry(),
                request.retryBackoff(), request.concurrency(), JobStatus.ENABLED, 1);
        return submit(CommandType.CREATE_JOB, requestId(CommandType.CREATE_JOB, requestId),
                mapper.valueToTree(job));
    }

    @GetMapping("/jobs")
    public Object listJobs(@RequestParam(defaultValue = "strong") String consistency) {
        ensureConsistency(consistency);
        return store.listJobs();
    }

    @GetMapping("/jobs/{id}")
    public Job getJob(@PathVariable String id,
                      @RequestParam(defaultValue = "strong") String consistency) {
        ensureConsistency(consistency);
        return findJob(id);
    }

    private Job findJob(String id) {
        Job job = store.getJob(id);
        if (job == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found");
        }
        return job;
    }

    @PutMapping("/jobs/{id}")
    public CompletableFuture<JsonNode> updateJob(@PathVariable String id, @Valid @RequestBody JobRequest request) {
        Job current = findJob(id);
        Job job = new Job(id, request.name(), request.payload(), request.timeout(), request.maxRetry(),
                request.retryBackoff(), request.concurrency(), current.status(), current.version() + 1);
        return submit(CommandType.UPDATE_JOB, UUID.randomUUID().toString(), mapper.valueToTree(job));
    }

    @DeleteMapping("/jobs/{id}")
    public CompletableFuture<JsonNode> deleteJob(@PathVariable String id) {
        return submit(CommandType.DELETE_JOB, UUID.randomUUID().toString(),
                mapper.createObjectNode().put("jobId", id));
    }

    @PostMapping("/jobs/{id}/enable")
    public CompletableFuture<JsonNode> enableJob(@PathVariable String id) {
        return setEnabled(id, true);
    }

    @PostMapping("/jobs/{id}/disable")
    public CompletableFuture<JsonNode> disableJob(@PathVariable String id) {
        return setEnabled(id, false);
    }

    @PostMapping("/jobs/{id}/run")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<JsonNode> runJob(
            @PathVariable String id,
            @RequestHeader(value = "Idempotency-Key", required = false) String requestId) {
        findJob(id);
        String idempotencyKey = requestId == null || requestId.isBlank()
                ? UUID.randomUUID().toString() : requestId;
        return submit(CommandType.FIRE_JOB, requestId(CommandType.FIRE_JOB, idempotencyKey),
                mapper.createObjectNode().put("jobId", id).put("taskId", UUID.randomUUID().toString()));
    }

    @GetMapping("/tasks")
    public Object listTasks(@RequestParam(defaultValue = "strong") String consistency) {
        ensureConsistency(consistency);
        return store.listTasks();
    }

    @GetMapping("/tasks/{id}")
    public Task getTask(@PathVariable String id,
                        @RequestParam(defaultValue = "strong") String consistency) {
        ensureConsistency(consistency);
        Task task = store.getTask(id);
        if (task == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found");
        }
        return task;
    }

    private CompletableFuture<JsonNode> setEnabled(String id, boolean enabled) {
        return submit(CommandType.SET_JOB_ENABLED, UUID.randomUUID().toString(),
                mapper.createObjectNode().put("jobId", id).put("enabled", enabled));
    }

    private CompletableFuture<JsonNode> submit(CommandType type, String requestId, JsonNode body) {
        return raft.apply(new Command(type, requestId, Instant.now(), body));
    }

    private static String requestId(CommandType type, String requestId) {
        String key = requestId == null || requestId.isBlank() ? UUID.randomUUID().toString() : requestId;
        return type.name() + ":" + key;
    }

    private void ensureConsistency(String consistency) {
        if ("stale".equalsIgnoreCase(consistency)) {
            return;
        }
        if (!"strong".equalsIgnoreCase(consistency)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "consistency must be strong or stale");
        }
        raft.readIndex().join();
    }

    public record JobRequest(
            @NotBlank String name,
            @NotNull Payload payload,
            Duration timeout,
            @Min(0) int maxRetry,
            Duration retryBackoff,
            ConcurrencyPolicy concurrency) {
        public JobRequest {
            timeout = timeout == null ? Duration.ofMinutes(5) : timeout;
            retryBackoff = retryBackoff == null ? Duration.ofSeconds(5) : retryBackoff;
            concurrency = concurrency == null ? ConcurrencyPolicy.ALLOW : concurrency;
            if (timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("timeout must be positive");
            }
            if (retryBackoff.isNegative()) {
                throw new IllegalArgumentException("retryBackoff cannot be negative");
            }
            if (payload != null && (payload.executor() == null || payload.executor().isBlank())) {
                throw new IllegalArgumentException("payload.executor is required");
            }
        }
    }
}
