package io.ytong.raftjobkeeper.worker.executor;

import io.ytong.raftjobkeeper.common.grpc.TaskAssignment;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ShellExecutorTest {
    private final ShellExecutor executor = new ShellExecutor();

    @Test
    void rejectsWrongExecutorAndBlankCommand() {
        assertThrows(IllegalArgumentException.class, () -> executor.execute(
                TaskAssignment.newBuilder().setExecutor("http").setCommand("echo").build()));
        assertThrows(IllegalArgumentException.class, () -> executor.execute(
                TaskAssignment.newBuilder().setExecutor("shell").setCommand(" ").build()));
    }

    @Test
    void capturesOutputAndEnvironment() throws Exception {
        var result = executor.execute(task("success").putEnv("JOB_KEEPER_TEST", "hello").build());
        assertEquals("SUCCEEDED", result.status());
        assertTrue(result.output().contains("hello"));
        assertTrue(result.output().contains("stderr"));
    }

    @Test
    void nonzeroExitFailsTask() throws Exception {
        assertEquals("FAILED", executor.execute(task("fail").build()).status());
    }

    @Test
    void killsTaskWhenTimeoutExpires() throws Exception {
        assertEquals("TIMEOUT", executor.execute(task("sleep").setTimeoutMillis(100).build()).status());
    }

    private TaskAssignment.Builder task(String mode) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classes = Path.of(ChildProcess.class.getProtectionDomain().getCodeSource()
                .getLocation().toURI()).toString();
        return TaskAssignment.newBuilder().setTaskId("test").setExecutor("shell")
                .setCommand(java).addArgs("-cp").addArgs(classes).addArgs(ChildProcess.class.getName())
                .addArgs(mode).setTimeoutMillis(10000);
    }

    public static class ChildProcess {
        public static void main(String[] args) throws Exception {
            switch (args[0]) {
                case "success" -> {
                    System.out.println(System.getenv("JOB_KEEPER_TEST"));
                    System.err.println("stderr");
                }
                case "fail" -> System.exit(3);
                case "sleep" -> Thread.sleep(30000);
                default -> throw new IllegalArgumentException(args[0]);
            }
        }
    }
}
