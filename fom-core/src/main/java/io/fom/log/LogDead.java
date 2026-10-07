package io.fom.log;

import io.fom.Sid;

import java.util.Objects;

/** {@code sid} was retired; its state must not be loaded again. */
public record LogDead(long clock, long timestamp, short formatVersion, Sid sid)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogDead {
        Objects.requireNonNull(sid, "sid");
    }

    public LogDead(long clock, long timestamp, Sid sid) {
        this(clock, timestamp, CURRENT_FORMAT, sid);
    }
}
