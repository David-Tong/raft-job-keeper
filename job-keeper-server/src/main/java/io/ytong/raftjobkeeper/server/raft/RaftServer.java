package io.ytong.raftjobkeeper.server.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ytong.raftjobkeeper.common.command.Command;
import io.ytong.raftjobkeeper.server.fsm.JobStateMachine;
import io.ytong.raftjobkeeper.server.store.JobStore;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class RaftServer {
    private final JobStore store;
    private final ObjectMapper mapper;
    private final String dataDir;
    private final String groupId;
    private final String selfAddress;
    private final String initialPeers;
    private final int electionTimeoutMs;
    private final int snapshotIntervalSecs;
    private final int httpPort;
    private final int grpcPort;

    private volatile Node node;
    private volatile RaftGroupService groupService;
    private volatile JobStateMachine stateMachine;

    public RaftServer(
            JobStore store,
            ObjectMapper mapper,
            @Value("${raft.data-dir:/data}") String dataDir,
            @Value("${raft.group-id:raft-job-keeper}") String groupId,
            @Value("${raft.self-address:${ADVERTISE_ADDR:localhost:7000}}") String selfAddress,
            @Value("${raft.initial-peers:${RAFT_INITIAL_PEERS:localhost:7000}}") String initialPeers,
            @Value("${raft.election-timeout-ms:1000}") int electionTimeoutMs,
            @Value("${raft.snapshot-interval-secs:600}") int snapshotIntervalSecs,
            @Value("${server.port:8080}") int httpPort,
            @Value("${grpc.port:9090}") int grpcPort) {
        this.store = store;
        this.mapper = mapper;
        this.dataDir = dataDir;
        this.groupId = groupId;
        this.selfAddress = selfAddress;
        this.initialPeers = initialPeers;
        this.electionTimeoutMs = electionTimeoutMs;
        this.snapshotIntervalSecs = snapshotIntervalSecs;
        this.httpPort = httpPort;
        this.grpcPort = grpcPort;
    }

    @jakarta.annotation.PostConstruct
    public void start() throws Exception {
        Path root = Path.of(dataDir, groupId);
        Files.createDirectories(root.resolve("log"));
        Files.createDirectories(root.resolve("meta"));
        Files.createDirectories(root.resolve("snapshot"));

        PeerId self = PeerId.parsePeer(selfAddress);
        if (self == null) {
            throw new IllegalArgumentException("Invalid Raft self address: " + selfAddress);
        }
        Configuration configuration = new Configuration();
        List<String> peers = Arrays.stream(initialPeers.split(","))
                .map(String::trim).filter(value -> !value.isEmpty()).toList();
        for (String address : peers) {
            PeerId peer = PeerId.parsePeer(address);
            if (peer == null || !configuration.addPeer(peer)) {
                throw new IllegalArgumentException("Invalid or duplicate Raft peer: " + address);
            }
        }
        if (configuration.isEmpty()) {
            throw new IllegalArgumentException("At least one Raft peer must be configured");
        }

        NodeOptions options = new NodeOptions();
        options.setElectionTimeoutMs(electionTimeoutMs);
        options.setSnapshotIntervalSecs(snapshotIntervalSecs);
        options.setInitialConf(configuration);
        stateMachine = new JobStateMachine(store, mapper);
        options.setFsm(stateMachine);
        options.setLogUri(root.resolve("log").toString());
        options.setRaftMetaUri(root.resolve("meta").toString());
        options.setSnapshotUri(root.resolve("snapshot").toString());

        groupService = new RaftGroupService(groupId, self, options,
                RaftRpcServerFactory.createRaftRpcServer(self.getEndpoint()));
        node = groupService.start();
    }

    public CompletableFuture<JsonNode> apply(Command command) {
        Node current = requireNode();
        if (!current.isLeader()) {
            throw new NotLeaderException(leaderHttpAddress());
        }
        CompletableFuture<JsonNode> future = new CompletableFuture<>();
        try {
            Task task = new Task();
            task.setData(ByteBuffer.wrap(mapper.writeValueAsBytes(command)));
            task.setDone(new JobStateMachine.CommandClosure(future));
            current.apply(task);
        } catch (Exception exception) {
            future.completeExceptionally(exception);
        }
        return future;
    }

    public CompletableFuture<Void> readIndex() {
        Node current = requireNode();
        if (!current.isLeader()) {
            throw new NotLeaderException(leaderHttpAddress());
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        current.readIndex(null, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] requestContext) {
                if (status.isOk()) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(new IllegalStateException(status.getErrorMsg()));
                }
            }
        });
        return future;
    }

    public boolean isLeader() {
        return node != null && node.isLeader();
    }

    public String leaderAddress() {
        Node current = node;
        return current == null || current.getLeaderId() == null ? null : current.getLeaderId().toString();
    }

    public String leaderHttpAddress() {
        return withPort(leaderAddress(), httpPort);
    }

    public String leaderGrpcAddress() {
        return withPort(leaderAddress(), grpcPort);
    }

    public String selfAddress() {
        return selfAddress;
    }

    public String groupId() {
        return groupId;
    }

    public long currentTerm() {
        JobStateMachine current = stateMachine;
        return current == null ? 0 : current.currentTerm();
    }

    public CompletableFuture<Void> snapshot() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Node current = requireNode();
        current.snapshot(new Closure() {
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(new IllegalStateException(status.getErrorMsg()));
                }
            }
        });
        return future;
    }

    public CompletableFuture<Void> addPeer(String address) {
        PeerId peer = parsePeer(address);
        CompletableFuture<Void> future = new CompletableFuture<>();
        requireNode().addPeer(peer, completion(future));
        return future;
    }

    public CompletableFuture<Void> removePeer(String address) {
        Node current = requireNode();
        PeerId peer = parsePeer(address);
        int peerCount = current.listPeers().size();
        if (peerCount <= 1 || peerCount - 1 < (peerCount / 2 + 1)) {
            throw new IllegalStateException("Removing this peer would leave the cluster without a majority");
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        current.removePeer(peer, completion(future));
        return future;
    }

    public String transferLeadership() {
        Node current = requireNode();
        if (!current.isLeader()) {
            throw new NotLeaderException(leaderAddress());
        }
        PeerId target = current.listPeers().stream()
                .filter(peer -> !peer.toString().equals(selfAddress))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No other Raft peer is available"));
        Status status = current.transferLeadershipTo(target);
        if (!status.isOk()) {
            throw new IllegalStateException(status.getErrorMsg());
        }
        return target.toString();
    }

    public List<String> peers() {
        Node current = node;
        return current == null ? List.of() : current.listPeers().stream().map(PeerId::toString).toList();
    }

    public String clusterStatus() {
        Node current = node;
        if (current == null) {
            return "STARTING";
        }
        return current.isLeader() ? "LEADER" : "FOLLOWER";
    }

    private Node requireNode() {
        Node current = node;
        if (current == null) {
            throw new IllegalStateException("Raft node is not started");
        }
        return current;
    }

    private static PeerId parsePeer(String address) {
        PeerId peer = PeerId.parsePeer(address);
        if (peer == null) {
            throw new IllegalArgumentException("Invalid Raft peer address: " + address);
        }
        return peer;
    }

    private static String withPort(String address, int port) {
        if (address == null) {
            return null;
        }
        int separator = address.lastIndexOf(':');
        return separator < 0 ? address : address.substring(0, separator + 1) + port;
    }

    private static Closure completion(CompletableFuture<Void> future) {
        return status -> {
            if (status.isOk()) {
                future.complete(null);
            } else {
                future.completeExceptionally(new IllegalStateException(status.getErrorMsg()));
            }
        };
    }

    @PreDestroy
    public void stop() {
        RaftGroupService service = groupService;
        if (service != null) {
            service.shutdown();
            try {
                service.join();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
