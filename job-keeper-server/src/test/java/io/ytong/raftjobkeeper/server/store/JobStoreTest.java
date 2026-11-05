package io.ytong.raftjobkeeper.server.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.ytong.raftjobkeeper.common.command.Command;
import io.ytong.raftjobkeeper.common.command.CommandType;
import io.ytong.raftjobkeeper.common.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobStoreTest {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private JobStore store;
    private Instant now;

    @BeforeEach
    void setUp() {
        store = new JobStore(mapper);
        now = Instant.parse("2026-01-01T00:00:00Z");
    }

    @Test
    void fireJobIsIdempotentByRequestId() {
        createJob(ConcurrencyPolicy.ALLOW);

        var first = store.apply(command(CommandType.FIRE_JOB, "run-1",
                mapper.createObjectNode().put("jobId", "job-1").put("taskId", "task-1")));
        var retry = store.apply(command(CommandType.FIRE_JOB, "run-1",
                mapper.createObjectNode().put("jobId", "job-1").put("taskId", "different-task")));

        assertEquals(first, retry);
        assertEquals(1, store.listTasks().size());
        assertEquals("task-1", store.listTasks().get(0).id());
    }

    @Test
    void concurrencyForbidRejectsSecondActiveRun() {
        createJob(ConcurrencyPolicy.FORBID);
        store.apply(command(CommandType.FIRE_JOB, "run-1",
                mapper.createObjectNode().put("jobId", "job-1").put("taskId", "task-1")));

        assertThrows(IllegalStateException.class, () -> store.apply(command(CommandType.FIRE_JOB, "run-2",
                mapper.createObjectNode().put("jobId", "job-1").put("taskId", "task-2"))));
    }

    @Test
    void expiredTaskCanBeReclaimedAndSnapshotRestored() {
        createJob(ConcurrencyPolicy.ALLOW);
        store.apply(command(CommandType.FIRE_JOB, "run-1",
                mapper.createObjectNode().put("jobId", "job-1").put("taskId", "task-1")));
        store.apply(command(CommandType.ASSIGN_TASK, "assign-1",
                mapper.createObjectNode().put("taskId", "task-1").put("workerId", "worker-1")
                        .put("leaseExpireAt", now.plusSeconds(10).toString())));
        store.apply(new Command(CommandType.EXPIRE_TASK, "expire-1", now.plusSeconds(11),
                mapper.createObjectNode().put("taskId", "task-1")));

        assertEquals(TaskStatus.PENDING, store.getTask("task-1").status());
        assertEquals(2, store.getTask("task-1").attempt());

        JobStore restored = new JobStore(mapper);
        restored.restore(mapper.convertValue(store.snapshot(), JobStore.Snapshot.class));
        assertEquals(store.getTask("task-1"), restored.getTask("task-1"));
        assertEquals(store.getJob("job-1"), restored.getJob("job-1"));
    }

    private void createJob(ConcurrencyPolicy concurrency) {
        Job job = new Job("job-1", "test", new Payload("shell", "echo", List.of(), Map.of()),
                Duration.ofSeconds(30), 2, Duration.ZERO, concurrency, JobStatus.ENABLED, 1);
        store.apply(command(CommandType.CREATE_JOB, "create-1", mapper.valueToTree(job)));
    }

    private Command command(CommandType type, String requestId, com.fasterxml.jackson.databind.JsonNode body) {
        return new Command(type, requestId, now, body);
    }
}
