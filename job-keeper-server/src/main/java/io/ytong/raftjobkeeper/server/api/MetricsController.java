package io.ytong.raftjobkeeper.server.api;

import io.ytong.raftjobkeeper.common.model.TaskStatus;
import io.ytong.raftjobkeeper.server.store.JobStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MetricsController {
    private final JobStore store;

    public MetricsController(JobStore store) {
        this.store = store;
    }

    @GetMapping(value = "/metrics", produces = "text/plain; version=0.0.4")
    public String metrics() {
        StringBuilder output = new StringBuilder();
        var tasks = store.listTasks();
        output.append("# HELP jobs_total Number of non-deleted jobs\n")
                .append("# TYPE jobs_total gauge\n")
                .append("jobs_total ").append(store.listJobs().size()).append('\n')
                .append("# HELP tasks_by_status Number of tasks by status\n")
                .append("# TYPE tasks_by_status gauge\n");
        for (TaskStatus status : TaskStatus.values()) {
            long count = tasks.stream().filter(task -> task.status() == status).count();
            output.append("tasks_by_status{status=\"").append(status.name().toLowerCase())
                    .append("\"} ").append(count).append('\n');
        }
        output.append("# HELP worker_online Number of online workers in the state machine\n")
                .append("# TYPE worker_online gauge\n")
                .append("worker_online ").append(store.onlineWorkers().size()).append('\n');
        return output.toString();
    }
}
