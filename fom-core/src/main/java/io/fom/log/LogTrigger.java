package io.fom.log;

import java.util.List;
import java.util.Objects;

/**
 * A re-init was requested for {@code processNames} by {@code engine.trigger(...)}
 * or a watcher tick. Written before the re-init, so a restart can replay requests
 * that never got a newer {@code LogInitialized}. Trigger values are not recorded.
 */
public record LogTrigger(long clock, long timestamp, short formatVersion, List<String> processNames)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 2;

    public LogTrigger {
        processNames = List.copyOf(Objects.requireNonNull(processNames, "processNames"));
        if (processNames.isEmpty()) {
            throw new IllegalArgumentException("LogTrigger must name at least one process");
        }
    }

    public LogTrigger(long clock, long timestamp, List<String> processNames) {
        this(clock, timestamp, CURRENT_FORMAT, processNames);
    }
}
