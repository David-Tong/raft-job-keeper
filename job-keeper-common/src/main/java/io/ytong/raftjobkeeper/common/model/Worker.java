package io.ytong.raftjobkeeper.common.model;

import java.time.Instant;
import java.util.Map;

public record Worker(
        String id,
        String address,
        Map<String, String> labels,
        int capacity,
        int running,
        Instant lastHeartbeat,
        WorkerStatus status) {
    public Worker {
        labels = labels == null ? Map.of() : Map.copyOf(labels);
    }
}
