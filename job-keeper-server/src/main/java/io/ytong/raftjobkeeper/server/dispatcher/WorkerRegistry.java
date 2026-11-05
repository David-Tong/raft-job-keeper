package io.ytong.raftjobkeeper.server.dispatcher;

import io.grpc.stub.StreamObserver;
import io.ytong.raftjobkeeper.common.grpc.ServerMessage;
import io.ytong.raftjobkeeper.common.model.Worker;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class WorkerRegistry {
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    public void register(Worker worker, StreamObserver<ServerMessage> stream) {
        sessions.put(worker.id(), new Session(worker, stream));
    }

    public void remove(String workerId, StreamObserver<ServerMessage> stream) {
        sessions.computeIfPresent(workerId, (key, session) -> session.stream() == stream ? null : session);
    }

    public Session leastLoaded() {
        return sessions.values().stream()
                .filter(session -> session.taskIds().size() < session.worker().capacity())
                .min(Comparator.comparingDouble(session ->
                        (double) session.taskIds().size() / session.worker().capacity()))
                .orElse(null);
    }

    public void closeAll(String leader) {
        sessions.forEach((workerId, session) -> {
            try {
                session.stream().onNext(ServerMessage.newBuilder().setType("REDIRECT")
                        .setLeader(leader == null ? "" : leader).build());
                session.stream().onCompleted();
            } finally {
                remove(workerId, session.stream());
            }
        });
    }

    public int activeCount(String workerId) {
        Session session = sessions.get(workerId);
        return session == null ? 0 : session.taskIds().size();
    }

    public boolean reserve(String workerId, String taskId) {
        boolean[] reserved = {false};
        sessions.computeIfPresent(workerId, (key, session) -> {
            if (session.taskIds().contains(taskId)) {
                reserved[0] = true;
                return session;
            }
            if (session.taskIds().size() >= session.worker().capacity()) {
                return session;
            }
            Set<String> taskIds = new HashSet<>(session.taskIds());
            taskIds.add(taskId);
            reserved[0] = true;
            return session.withTasks(taskIds);
        });
        return reserved[0];
    }

    public void releaseTask(String taskId) {
        sessions.replaceAll((workerId, session) -> {
            if (!session.taskIds().contains(taskId)) {
                return session;
            }
            Set<String> taskIds = new HashSet<>(session.taskIds());
            taskIds.remove(taskId);
            return session.withTasks(taskIds);
        });
    }

    public StreamObserver<ServerMessage> stream(String workerId) {
        Session session = sessions.get(workerId);
        return session == null ? null : session.stream();
    }

    public record Session(Worker worker, StreamObserver<ServerMessage> stream, Set<String> taskIds) {
        public Session(Worker worker, StreamObserver<ServerMessage> stream) {
            this(worker, stream, Set.of());
        }

        public Session {
            taskIds = Set.copyOf(taskIds);
        }

        Session withTasks(Set<String> value) {
            return new Session(worker, stream, value);
        }
    }
}
