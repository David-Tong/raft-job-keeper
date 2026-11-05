package io.ytong.raftjobkeeper.common.model;

import java.time.Duration;

public record Job(
        String id,
        String name,
        Payload payload,
        Duration timeout,
        int maxRetry,
        Duration retryBackoff,
        ConcurrencyPolicy concurrency,
        JobStatus status,
        long version) {
}
