package io.ytong.raftjobkeeper.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class JobKeeperServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(JobKeeperServerApplication.class, args);
    }
}
