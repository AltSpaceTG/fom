package io.fom;

import java.io.Serializable;
import java.util.Objects;

/**
 * Stable identifier of a specific version of a process state.
 * Pair of {@code (processName, clock)} where {@code clock} is the
 * log position at which this state was committed.
 *
 * <p>Two states of the same process with different {@code clock} values are
 * different {@code Sid}s. Sid uniquely identifies which serving instance
 * (live or {@code Dead}) a query was routed to.</p>
 */
public record Sid(String processName, int clock) implements Serializable {

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
