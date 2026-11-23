package io.ytong.raftjobkeeper.common.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WorkerTest {
    @Test
    void nullLabelsBecomeEmpty() {
        Worker worker = new Worker("w1", "localhost", null, 2, 0, Instant.EPOCH, WorkerStatus.ONLINE);
        assertEquals(Map.of(), worker.labels());
    }

    @Test
    void labelsAreDefensivelyCopiedAndImmutable() {
        var labels = new HashMap<>(Map.of("region", "local"));
        Worker worker = new Worker("w1", "localhost", labels, 2, 0, Instant.EPOCH, WorkerStatus.ONLINE);
        labels.put("region", "remote");
        assertEquals(Map.of("region", "local"), worker.labels());
        assertThrows(UnsupportedOperationException.class, () -> worker.labels().clear());
    }
}
