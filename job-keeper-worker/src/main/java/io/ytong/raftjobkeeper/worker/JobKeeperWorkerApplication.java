package io.ytong.raftjobkeeper.worker;

import io.ytong.raftjobkeeper.worker.runtime.WorkerRuntime;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class JobKeeperWorkerApplication implements CommandLineRunner {
    private final WorkerRuntime runtime;

    public JobKeeperWorkerApplication(WorkerRuntime runtime) {
        this.runtime = runtime;
    }

    public static void main(String[] args) {
        SpringApplication.run(JobKeeperWorkerApplication.class, args);
    }

    @Override
    public void run(String... args) {
        runtime.start();
    }

    @PreDestroy
    public void stop() {
        runtime.stop();
    }
}
