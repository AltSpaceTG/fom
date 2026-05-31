package io.fom;

import java.util.Objects;

/**
 * Outcome of {@code Engine.snapshot()}.
 *
 * @param newLogId         identifier of the freshly-rotated log file/table
 * @param archivedLogId    identifier of the previous log (now {@code *.archived})
 * @param checkpointClock  the clock written into the {@code LogSnapshot} marker
 * @param eventsCopied     number of {@code LogChangeGraph} + {@code LogInitialized}
 *                         events transcribed into the new log
 */
public record SnapshotResult(String newLogId,
                             String archivedLogId,
                             int checkpointClock,
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
