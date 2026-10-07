package io.fom.log;

import java.util.Objects;

/**
 * {@code processName} is no longer paused: it was resumed, or removed from the
 * graph while paused. Cancels the preceding {@link LogPaused}.
 */
public record LogResumed(long clock, long timestamp, short formatVersion, String processName)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogResumed {
        Objects.requireNonNull(processName, "processName");
        if (processName.isEmpty()) {
            throw new IllegalArgumentException("processName must not be empty");
        }
    }

    public LogResumed(long clock, long timestamp, String processName) {
        this(clock, timestamp, CURRENT_FORMAT, processName);
    }
}
