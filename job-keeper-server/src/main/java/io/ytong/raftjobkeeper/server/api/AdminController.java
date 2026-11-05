package io.ytong.raftjobkeeper.server.api;

import io.ytong.raftjobkeeper.server.raft.RaftServer;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/admin")
public class AdminController {
    private final RaftServer raft;

    public AdminController(RaftServer raft) {
        this.raft = raft;
    }

    @GetMapping("/cluster/status")
    public Map<String, Object> status() {
        return Map.of("groupId", raft.groupId(), "self", raft.selfAddress(),
                "role", raft.clusterStatus(), "leader", raft.leaderAddress() == null ? "" : raft.leaderAddress(),
                "peers", raft.peers());
    }

    @PostMapping("/cluster/join")
    public CompletableFuture<Void> join(@RequestBody PeerRequest request) {
        return raft.addPeer(request.address());
    }

    @PostMapping("/cluster/leave")
    public CompletableFuture<Void> leave(@RequestBody PeerRequest request) {
        return raft.removePeer(request.address());
    }

    @PostMapping("/snapshot")
    public CompletableFuture<Void> snapshot() {
        return raft.snapshot();
    }

    @PostMapping("/transfer-leadership")
    public Map<String, String> transferLeadership() {
        return Map.of("status", "transferring", "leader", raft.transferLeadership());
    }

    public record PeerRequest(String address) {
    }
}
