package io.ytong.raftjobkeeper.server.api;

import io.ytong.raftjobkeeper.server.raft.NotLeaderException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(NotLeaderException.class)
    public ResponseEntity<Map<String, String>> notLeader(NotLeaderException exception, HttpServletRequest request) {
        HttpHeaders headers = new HttpHeaders();
        if (exception.leader() != null && !exception.leader().isBlank()) {
            String query = request.getQueryString();
            headers.setLocation(URI.create("http://" + exception.leader() + request.getRequestURI()
                    + (query == null ? "" : "?" + query)));
        }
        return new ResponseEntity<>(Map.of("error", "NOT_LEADER",
                "leader", exception.leader() == null ? "" : exception.leader()), headers,
                HttpStatus.TEMPORARY_REDIRECT);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> invalidRequest(IllegalArgumentException exception) {
        return ResponseEntity.badRequest().body(Map.of("error", exception.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> conflict(IllegalStateException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", exception.getMessage()));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, String>> status(ResponseStatusException exception) {
        return ResponseEntity.status(exception.getStatusCode())
                .body(Map.of("error", exception.getReason() == null ? "Request failed" : exception.getReason()));
    }
}
