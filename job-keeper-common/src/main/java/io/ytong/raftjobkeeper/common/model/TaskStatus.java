package io.ytong.raftjobkeeper.common.model;

public enum TaskStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    TIMEOUT,
    CANCELED
}
