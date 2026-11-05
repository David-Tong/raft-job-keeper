package io.ytong.raftjobkeeper.server.api;

import io.ytong.raftjobkeeper.server.raft.RaftServer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HealthController {
    private final RaftServer raft;

    public HealthController(RaftServer raft) {
        this.raft = raft;
    }

    @GetMapping("/healthz")
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, String>> ready() {
        return ResponseEntity.ok(Map.of("status", "READY", "raft", raft.clusterStatus()));
    }

    @GetMapping("/leaderz")
    public ResponseEntity<Map<String, String>> leader() {
        if (raft.isLeader()) {
            return ResponseEntity.ok(Map.of("status", "LEADER"));
        }
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("status", "FOLLOWER",
                        "leader", raft.leaderHttpAddress() == null ? "" : raft.leaderHttpAddress()));
    }
}
