package io.fom.log;

import java.util.Objects;

/**
 * {@code instanceId} claimed the log. Only the instance named by the latest
 * {@code LogLeader} may append.
 */
public record LogLeader(long clock, long timestamp, short formatVersion, String instanceId)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogLeader {
        Objects.requireNonNull(instanceId, "instanceId");
        if (instanceId.isEmpty()) {
            throw new IllegalArgumentException("instanceId must not be empty");
        }
    }

    public LogLeader(long clock, long timestamp, String instanceId) {
        this(clock, timestamp, CURRENT_FORMAT, instanceId);
    }
}
