package io.ytong.raftjobkeeper.common.command;

public enum CommandType {
    CREATE_JOB,
    UPDATE_JOB,
    DELETE_JOB,
    SET_JOB_ENABLED,
    FIRE_JOB,
    ASSIGN_TASK,
    RENEW_LEASE,
    REPORT_TASK,
    EXPIRE_TASK,
    REGISTER_WORKER,
    WORKER_HEARTBEAT,
    REMOVE_WORKER,
    PURGE_HISTORY
}
