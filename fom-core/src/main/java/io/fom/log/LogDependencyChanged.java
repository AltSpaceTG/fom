package io.fom.log;

import io.fom.Sid;

import java.util.Objects;

/**
 * Reactive invalidation marker: the dependency {@code depName} of {@code sid}
 * changed its Sid from {@code oldDepClock} to {@code newDepClock}. Recorded
 * for tracing and dedup-window collapsing.
 *
 * <p>Extended event — older readers may skip with a warning.</p>
 */
public record LogDependencyChanged(int clock,
                                   long timestamp,
                                   short formatVersion,
                                   Sid sid,
                                   String depName,
                                   int oldDepClock,
                                   int newDepClock)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogDependencyChanged {
        Objects.requireNonNull(sid, "sid");
        Objects.requireNonNull(depName, "depName");
        if (depName.isEmpty()) {
            throw new IllegalArgumentException("depName must not be empty");
        }
        if (oldDepClock < 0) {
            throw new IllegalArgumentException("oldDepClock must be >= 0, was " + oldDepClock);
        }
        if (newDepClock < 0) {
            throw new IllegalArgumentException("newDepClock must be >= 0, was " + newDepClock);
        }
    }

    public LogDependencyChanged(int clock, long timestamp,
                                Sid sid, String depName, int oldDepClock, int newDepClock) {
        this(clock, timestamp, CURRENT_FORMAT, sid, depName, oldDepClock, newDepClock);
    }
}
