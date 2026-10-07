package io.fom.log;

import io.fom.Sid;

import java.util.Objects;

/** {@code sid} finished {@code load} and is serving queries. */
public record LogLoaded(long clock, long timestamp, short formatVersion, Sid sid)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogLoaded {
        Objects.requireNonNull(sid, "sid");
    }

    public LogLoaded(long clock, long timestamp, Sid sid) {
        this(clock, timestamp, CURRENT_FORMAT, sid);
    }
}
