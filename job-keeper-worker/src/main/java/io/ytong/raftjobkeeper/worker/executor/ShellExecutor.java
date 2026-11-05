package io.ytong.raftjobkeeper.worker.executor;

import io.ytong.raftjobkeeper.common.grpc.TaskAssignment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class ShellExecutor implements Executor {
    private static final int MAX_OUTPUT_BYTES = 64 * 1024;

    @Override
    public String type() {
        return "shell";
    }

    @Override
    public TaskResult execute(TaskAssignment task) throws Exception {
        if (!"shell".equalsIgnoreCase(task.getExecutor())) {
            throw new IllegalArgumentException("Unsupported executor: " + task.getExecutor());
        }
        if (task.getCommand().isBlank()) {
            throw new IllegalArgumentException("Shell task command is required");
        }
        List<String> command = new ArrayList<>();
        command.add(task.getCommand());
        command.addAll(task.getArgsList());
        Path outputFile = Files.createTempFile("raft-job-" + task.getTaskId(), ".log");
        try {
            ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true)
                    .redirectOutput(outputFile.toFile());
            builder.environment().putAll(task.getEnvMap());
            Process process = builder.start();
            long timeout = Math.max(1, task.getTimeoutMillis());
            boolean finished;
            try {
                finished = process.waitFor(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException exception) {
                process.destroyForcibly();
                process.waitFor();
                Thread.currentThread().interrupt();
                throw exception;
            }
            if (!finished) {
                process.destroyForcibly();
                process.waitFor();
                return new TaskResult("TIMEOUT", readOutput(outputFile));
            }
            String output = readOutput(outputFile);
            return new TaskResult(process.exitValue() == 0 ? "SUCCEEDED" : "FAILED", output);
        } finally {
            Files.deleteIfExists(outputFile);
        }
    }

    private static String readOutput(Path path) throws Exception {
        byte[] output = Files.readAllBytes(path);
        if (output.length > MAX_OUTPUT_BYTES) {
            byte[] truncated = new byte[MAX_OUTPUT_BYTES];
            System.arraycopy(output, output.length - MAX_OUTPUT_BYTES, truncated, 0, MAX_OUTPUT_BYTES);
            return new String(truncated, StandardCharsets.UTF_8) + "\n[output truncated]";
        }
        return new String(output, StandardCharsets.UTF_8);
    }
}
