package io.fom.log;

import io.fom.Sid;

import java.util.Objects;

/**
 * The reactive dependency {@code depName} of {@code sid} moved from the Sid at
 * {@code oldDepClock} to the one at {@code newDepClock}, so {@code sid} must re-init.
 *
 * <p>Extended event: older readers may skip it with a warning.</p>
 */
public record LogDependencyChanged(long clock,
                                   long timestamp,
                                   short formatVersion,
                                   Sid sid,
                                   String depName,
                                   long oldDepClock,
                                   long newDepClock)
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

    public LogDependencyChanged(long clock, long timestamp,
                                Sid sid, String depName, long oldDepClock, long newDepClock) {
        this(clock, timestamp, CURRENT_FORMAT, sid, depName, oldDepClock, newDepClock);
    }
}
