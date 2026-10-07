package io.fom;

import java.io.Serializable;
import java.util.Objects;

/**
 * One version of a process's state: the process name and the clock of the
 * {@code LogInitialized} that committed it.
 *
 * <p>Clocks only grow and survive snapshots, so a Sid never names another state.
 * A clock is not a log position (they differ after a compaction): don't pass it to
 * {@code LogBackend.get}.</p>
 */
public record Sid(String processName, long clock) implements Serializable {

    public Sid {
        Objects.requireNonNull(processName, "processName");
        if (processName.isEmpty()) {
            throw new IllegalArgumentException("processName must not be empty");
        }
        if (clock < 0) {
            throw new IllegalArgumentException("clock must be >= 0, was " + clock);
        }
    }
}
