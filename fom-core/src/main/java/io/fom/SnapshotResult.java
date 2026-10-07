package io.fom;

import java.util.Objects;

/**
 * Outcome of {@code Engine.snapshot()}.
 *
 * @param newLogId         identifier of the freshly-rotated log file/table
 * @param archivedLogId    identifier of the previous log (now {@code *.archived})
 * @param checkpointClock  the marker's {@code checkpointClock} payload: the clock of the
 *                         last event of the log as it was before this compaction
 * @param eventsCopied     number of events in the compacted log — every event
 *                         written, including the leader and snapshot markers
 */
public record SnapshotResult(String newLogId,
                             String archivedLogId,
                             long checkpointClock,
                             int eventsCopied) {

    public SnapshotResult {
        Objects.requireNonNull(newLogId, "newLogId");
        Objects.requireNonNull(archivedLogId, "archivedLogId");
        if (checkpointClock < 0) {
            throw new IllegalArgumentException("checkpointClock must be >= 0, was " + checkpointClock);
        }
        if (eventsCopied < 0) {
            throw new IllegalArgumentException("eventsCopied must be >= 0, was " + eventsCopied);
        }
    }
}
