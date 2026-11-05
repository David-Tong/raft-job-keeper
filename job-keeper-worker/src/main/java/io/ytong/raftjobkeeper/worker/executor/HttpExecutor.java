package io.ytong.raftjobkeeper.worker.executor;

import io.ytong.raftjobkeeper.common.grpc.TaskAssignment;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Component
public class HttpExecutor implements Executor {
    private static final int MAX_OUTPUT_LENGTH = 64 * 1024;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @Override
    public String type() {
        return "http";
    }

    @Override
    public TaskResult execute(TaskAssignment task) throws Exception {
        URI uri = URI.create(task.getCommand());
        String method = "POST";
        List<String> arguments = new ArrayList<>(task.getArgsList());
        if (!arguments.isEmpty() && isHttpMethod(arguments.get(0))) {
            method = arguments.remove(0).toUpperCase(Locale.ROOT);
        }
        String body = String.join(" ", arguments);
        HttpRequest.Builder request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMillis(Math.max(1, task.getTimeoutMillis())));
        task.getEnvMap().forEach((key, value) -> {
            if (key.startsWith("header.") && key.length() > "header.".length()) {
                request.header(key.substring("header.".length()), value);
            }
        });
        if ("GET".equals(method) || "DELETE".equals(method)) {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            request.method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        try {
            HttpResponse<String> response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
            String output = response.statusCode() + " " + response.body();
            if (output.length() > MAX_OUTPUT_LENGTH) {
                output = output.substring(0, MAX_OUTPUT_LENGTH) + "\n[output truncated]";
            }
            return new TaskResult(response.statusCode() >= 200 && response.statusCode() < 300
                    ? "SUCCEEDED" : "FAILED", output);
        } catch (java.net.http.HttpTimeoutException exception) {
            return new TaskResult("TIMEOUT", "HTTP request timed out");
        }
    }

    private static boolean isHttpMethod(String value) {
        return List.of("GET", "POST", "PUT", "PATCH", "DELETE").contains(value.toUpperCase(Locale.ROOT));
    }
}
