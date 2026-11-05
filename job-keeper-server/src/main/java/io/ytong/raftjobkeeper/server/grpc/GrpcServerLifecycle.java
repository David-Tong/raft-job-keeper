package io.ytong.raftjobkeeper.server.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.ytong.raftjobkeeper.server.api.WorkerGrpcService;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class GrpcServerLifecycle {
    private final Server server;

    public GrpcServerLifecycle(WorkerGrpcService service, @Value("${grpc.port:9090}") int port)
            throws IOException {
        server = ServerBuilder.forPort(port).addService(service).build().start();
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (!server.isShutdown()) {
            server.shutdown();
            if (!server.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        }
    }
}
