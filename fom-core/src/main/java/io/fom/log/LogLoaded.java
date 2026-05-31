package io.fom.log;

import io.fom.Sid;

import java.util.Objects;

/**
 * Load phase of the given Sid is complete. The process is now ready to serve
 * queries.
 */
public record LogLoaded(int clock, long timestamp, short formatVersion, Sid sid)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogLoaded {
        Objects.requireNonNull(sid, "sid");
    }

    public LogLoaded(int clock, long timestamp, Sid sid) {
        this(clock, timestamp, CURRENT_FORMAT, sid);
    }
}
