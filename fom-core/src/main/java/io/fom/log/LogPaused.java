package io.fom.log;

import java.util.Objects;

/**
 * {@code processName} is paused: stopped, with its persisted state kept.
 * {@code stale} means a dependency changed (or it was triggered) while paused, so it
 * must re-init when resumed. {@code forDependency} means the engine paused it at
 * startup only because a dependency was paused.
 *
 * <p>The latest {@code LogPaused}/{@link LogResumed} of a process decides whether
 * it comes up paused after a restart.</p>
 */
public record LogPaused(long clock, long timestamp, short formatVersion, String processName, boolean stale,
                        boolean forDependency)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogPaused {
        Objects.requireNonNull(processName, "processName");
        if (processName.isEmpty()) {
            throw new IllegalArgumentException("processName must not be empty");
        }
    }

    /** A pause asked for by the operator. */
    public LogPaused(long clock, long timestamp, String processName, boolean stale) {
        this(clock, timestamp, CURRENT_FORMAT, processName, stale, false);
    }

    public LogPaused(long clock, long timestamp, String processName, boolean stale, boolean forDependency) {
        this(clock, timestamp, CURRENT_FORMAT, processName, stale, forDependency);
    }
}
