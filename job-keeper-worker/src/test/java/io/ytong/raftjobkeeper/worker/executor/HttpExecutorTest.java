package io.ytong.raftjobkeeper.worker.executor;

import com.sun.net.httpserver.HttpServer;
import io.ytong.raftjobkeeper.common.grpc.TaskAssignment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class HttpExecutorTest {
    private HttpServer server;
    private String url;
    private final AtomicReference<String> method = new AtomicReference<>();
    private final AtomicReference<String> body = new AtomicReference<>();
    private final AtomicReference<String> header = new AtomicReference<>();

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            method.set(exchange.getRequestMethod());
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            header.set(exchange.getRequestHeaders().getFirst("X-Test"));
            int status = exchange.getRequestURI().getPath().equals("/error") ? 503 : 200;
            byte[] response = "response".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, response.length);
            try (var output = exchange.getResponseBody()) {
                output.write(response);
            }
        });
        server.start();
        url = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
    }

    @Test
    void defaultsToPostWithBodyAndHeaders() throws Exception {
        var task = TaskAssignment.newBuilder().setCommand(url).setTimeoutMillis(5000)
                .addArgs("hello").addArgs("world").putEnv("header.X-Test", "value").build();
        assertEquals(new TaskResult("SUCCEEDED", "200 response"), new HttpExecutor().execute(task));
        assertEquals("POST", method.get());
        assertEquals("hello world", body.get());
        assertEquals("value", header.get());
    }

    @Test
    void getHasNoBody() throws Exception {
        var task = TaskAssignment.newBuilder().setCommand(url).setTimeoutMillis(5000)
                .addArgs("get").addArgs("ignored").build();
        assertEquals("SUCCEEDED", new HttpExecutor().execute(task).status());
        assertEquals("GET", method.get());
        assertEquals("", body.get());
    }

    @Test
    void nonSuccessResponseFailsTask() throws Exception {
        var task = TaskAssignment.newBuilder().setCommand(url + "/error").setTimeoutMillis(5000).build();
        assertEquals(new TaskResult("FAILED", "503 response"), new HttpExecutor().execute(task));
    }
}
