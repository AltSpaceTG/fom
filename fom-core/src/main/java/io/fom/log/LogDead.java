package io.fom.log;

import io.fom.Sid;

import java.util.Objects;

/**
 * The given Sid has been retired. Consumers must re-route to the new
 * (live) Sid of the same process.
 */
public record LogDead(int clock, long timestamp, short formatVersion, Sid sid)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogDead {
        Objects.requireNonNull(sid, "sid");
    }

    public LogDead(int clock, long timestamp, Sid sid) {
        this(clock, timestamp, CURRENT_FORMAT, sid);
    }
}
