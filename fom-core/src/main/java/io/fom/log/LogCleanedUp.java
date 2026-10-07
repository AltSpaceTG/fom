package io.fom.log;

import io.fom.Sid;

import java.util.Objects;

/**
 * {@code Process.cleanUp()} finished for {@code sid}: {@code ok} is false if it
 * threw or ran past the cleanup timeout.
 *
 * <p>Extended event: older readers may skip it with a warning.</p>
 */
public record LogCleanedUp(long clock, long timestamp, short formatVersion, Sid sid, boolean ok)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogCleanedUp {
        Objects.requireNonNull(sid, "sid");
    }

    public LogCleanedUp(long clock, long timestamp, Sid sid, boolean ok) {
        this(clock, timestamp, CURRENT_FORMAT, sid, ok);
    }
}
