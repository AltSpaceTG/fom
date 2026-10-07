package io.fom.log;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A {@link LogBackend}'s current state, as returned by {@code introspect()}.
 *
 * @param maxTimestampMillis the highest event timestamp in the log (0 while empty); not
 *                           necessarily the newest event's, since wall clocks can step back
 */
public record LogBackendReport(String logId,
                               int length,
                               String currentLeader,
                               Map<String, Integer> eventCounts,
                               long maxTimestampMillis) {

    public LogBackendReport {
        Objects.requireNonNull(logId, "logId");
        Objects.requireNonNull(eventCounts, "eventCounts");
        // Sorted, so reports and `fom-log inspect` list the counts in a stable order.
        eventCounts = Collections.unmodifiableMap(new TreeMap<>(eventCounts));
    }
}
