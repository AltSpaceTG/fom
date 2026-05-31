package io.fom.log;

import io.fom.Sid;

import java.util.Objects;

/**
 * {@code Process.cleanUp()} completed (or timed out) for the given Sid.
 * {@code ok=true} means the cleanup callback returned normally;
 * {@code ok=false} means it threw or exceeded the cleanup timeout.
 *
 * <p>Extended event — older readers may skip with a warning.</p>
 */
public record LogCleanedUp(int clock, long timestamp, short formatVersion, Sid sid, boolean ok)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogCleanedUp {
        Objects.requireNonNull(sid, "sid");
    }

    public LogCleanedUp(int clock, long timestamp, Sid sid, boolean ok) {
        this(clock, timestamp, CURRENT_FORMAT, sid, ok);
    }
}
