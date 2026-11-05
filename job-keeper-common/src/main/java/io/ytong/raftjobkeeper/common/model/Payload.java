package io.ytong.raftjobkeeper.common.model;

import java.util.List;
import java.util.Map;

public record Payload(String executor, String command, List<String> args, Map<String, String> env) {
    public Payload {
        args = args == null ? List.of() : List.copyOf(args);
        env = env == null ? Map.of() : Map.copyOf(env);
    }
}
