package io.fom.log;

import java.util.Objects;

/**
 * Marks a successful leadership claim by the given JVM instance.
 * Written by the first successful append from a new leader.
 * Protects the log from concurrent writers.
 */
public record LogLeader(int clock, long timestamp, short formatVersion, String instanceId)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogLeader {
        Objects.requireNonNull(instanceId, "instanceId");
        if (instanceId.isEmpty()) {
            throw new IllegalArgumentException("instanceId must not be empty");
        }
    }

    public LogLeader(int clock, long timestamp, String instanceId) {
        this(clock, timestamp, CURRENT_FORMAT, instanceId);
    }
}
