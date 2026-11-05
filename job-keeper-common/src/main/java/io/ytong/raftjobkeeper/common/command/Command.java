package io.ytong.raftjobkeeper.common.command;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public record Command(CommandType type, String requestId, Instant now, JsonNode body) {
}
