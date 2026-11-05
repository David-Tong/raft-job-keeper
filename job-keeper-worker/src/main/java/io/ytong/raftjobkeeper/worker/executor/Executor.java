package io.ytong.raftjobkeeper.worker.executor;

import io.ytong.raftjobkeeper.common.grpc.TaskAssignment;

public interface Executor {
    String type();

    TaskResult execute(TaskAssignment task) throws Exception;
}
