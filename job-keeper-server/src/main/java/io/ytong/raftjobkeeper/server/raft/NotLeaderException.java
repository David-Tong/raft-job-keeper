package io.ytong.raftjobkeeper.server.raft;

public class NotLeaderException extends RuntimeException {
    private final String leader;

    public NotLeaderException(String leader) {
        super("This server is not the Raft leader");
        this.leader = leader;
    }

    public String leader() {
        return leader;
    }
}
