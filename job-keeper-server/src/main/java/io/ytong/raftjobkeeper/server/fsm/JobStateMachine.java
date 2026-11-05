package io.ytong.raftjobkeeper.server.fsm;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ytong.raftjobkeeper.common.command.Command;
import io.ytong.raftjobkeeper.server.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class JobStateMachine extends StateMachineAdapter {
    private static final Logger log = LoggerFactory.getLogger(JobStateMachine.class);
    private static final String SNAPSHOT_FILE = "job-store.json";

    private final JobStore store;
    private final ObjectMapper mapper;
    private volatile long currentTerm;

    public JobStateMachine(JobStore store, ObjectMapper mapper) {
        this.store = store;
        this.mapper = mapper;
    }

    @Override
    public void onLeaderStart(long term) {
        currentTerm = term;
    }

    @Override
    public void onLeaderStop(Status status) {
        currentTerm = 0;
    }

    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public void onApply(com.alipay.sofa.jraft.Iterator iterator) {
        while (iterator.hasNext()) {
            Closure done = iterator.done();
            try {
                ByteBuffer data = iterator.getData();
                byte[] bytes = new byte[data.remaining()];
                data.get(bytes);
                Command command = mapper.readValue(bytes, Command.class);
                var result = store.apply(command);
                if (done instanceof CommandClosure commandClosure) {
                    commandClosure.complete(result);
                }
                if (done != null) {
                    done.run(new Status());
                }
            } catch (Exception exception) {
                log.error("Failed to apply committed command", exception);
                if (done instanceof CommandClosure commandClosure) {
                    commandClosure.fail(exception);
                }
                if (done != null) {
                    done.run(new Status(RaftError.EINVAL, exception.getMessage()));
                }
            }
            iterator.next();
        }
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        try {
            Path snapshot = Path.of(writer.getPath(), SNAPSHOT_FILE);
            mapper.writeValue(snapshot.toFile(), store.snapshot());
            if (!writer.addFile(SNAPSHOT_FILE)) {
                throw new IOException("Unable to register job store snapshot file");
            }
            done.run(new Status());
        } catch (Exception exception) {
            log.error("Failed to save job store snapshot", exception);
            done.run(new Status(RaftError.EIO, exception.getMessage()));
        }
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        try {
            Path snapshot = Path.of(reader.getPath(), SNAPSHOT_FILE);
            JobStore.Snapshot state = mapper.readValue(snapshot.toFile(), JobStore.Snapshot.class);
            store.restore(state);
            return true;
        } catch (Exception exception) {
            log.error("Failed to load job store snapshot", exception);
            return false;
        }
    }

    public static final class CommandClosure implements Closure {
        private final CompletableFuture<com.fasterxml.jackson.databind.JsonNode> future;
        private volatile com.fasterxml.jackson.databind.JsonNode result;
        private volatile Throwable failure;

        public CommandClosure(CompletableFuture<com.fasterxml.jackson.databind.JsonNode> future) {
            this.future = future;
        }

        void complete(com.fasterxml.jackson.databind.JsonNode value) {
            this.result = value;
        }

        void fail(Throwable value) {
            this.failure = value;
        }

        @Override
        public void run(Status status) {
            if (!status.isOk()) {
                future.completeExceptionally(failure == null
                        ? new IllegalStateException(status.getErrorMsg()) : failure);
            } else {
                future.complete(result);
            }
        }
    }
}
