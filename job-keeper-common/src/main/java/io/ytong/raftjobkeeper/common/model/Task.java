package io.ytong.raftjobkeeper.common.model;

import java.time.Instant;

public record Task(
        String id,
        String jobId,
        Instant fireTime,
        TaskStatus status,
        String workerId,
        Instant leaseExpireAt,
        int attempt,
        String result,
        Instant startedAt,
        Instant finishedAt,
        Instant availableAt) {
}
