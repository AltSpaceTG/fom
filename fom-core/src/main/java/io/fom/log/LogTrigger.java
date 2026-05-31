package io.fom.log;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * External re-init signal — emitted by a watcher tick or by a manual
 * {@code engine.trigger(...)} call.
 *
 * <p>The {@code values} map binds {@code processName -> serializedTriggerValue}.
 * A single event may carry multiple triggers atomically.</p>
 */
public record LogTrigger(int clock, long timestamp, short formatVersion, Map<String, byte[]> values)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogTrigger {
        Objects.requireNonNull(values, "values");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("LogTrigger must carry at least one entry");
        }
        values = deepCopy(values);
    }

    public LogTrigger(int clock, long timestamp, Map<String, byte[]> values) {
        this(clock, timestamp, CURRENT_FORMAT, values);
    }

    private static Map<String, byte[]> deepCopy(Map<String, byte[]> src) {
        var copy = new TreeMap<String, byte[]>();
        for (var e : src.entrySet()) {
            Objects.requireNonNull(e.getKey(), "trigger processName");
            Objects.requireNonNull(e.getValue(), () -> "trigger value for " + e.getKey());
            copy.put(e.getKey(), e.getValue().clone());
        }
        return java.util.Collections.unmodifiableMap(copy);
    }
}
