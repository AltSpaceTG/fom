package io.fom.log;

import java.util.Map;
import java.util.Objects;

/**
 * Snapshot view of a {@link LogBackend}'s current state — for {@code introspect()}
 * exposure.
 */
public record LogBackendReport(String logId,
                               int length,
                               String currentLeader,
                               Map<String, Integer> eventCountsByType,
                               long lastTimestampMillis) {

    public LogBackendReport {
        Objects.requireNonNull(logId, "logId");
        Objects.requireNonNull(eventCountsByType, "eventCountsByType");
        eventCountsByType = Map.copyOf(eventCountsByType);
    }
}
