package io.fom.log;

import io.fom.Sid;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Init phase complete for a process. The resulting {@code Sid} is derived
 * from {@code processName + clock} — the event itself defines its Sid by
 * virtue of being appended.
 *
 * <p>{@code properties} is the {@code ProcessProperties} payload — the byte
 * map that a future {@code ProcessLoader} will consume on JVM restart.
 * It is deep-copied on construction.</p>
 */
public record LogInitialized(int clock,
                             long timestamp,
                             short formatVersion,
                             String processName,
                             Map<String, byte[]> properties)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogInitialized {
        Objects.requireNonNull(processName, "processName");
        if (processName.isEmpty()) {
            throw new IllegalArgumentException("processName must not be empty");
        }
        Objects.requireNonNull(properties, "properties");
        properties = deepCopy(properties);
    }

    public LogInitialized(int clock, long timestamp, String processName, Map<String, byte[]> properties) {
        this(clock, timestamp, CURRENT_FORMAT, processName, properties);
    }

    public Sid sid() {
        return new Sid(processName, clock);
    }

    private static Map<String, byte[]> deepCopy(Map<String, byte[]> src) {
        var copy = new TreeMap<String, byte[]>();
        for (var e : src.entrySet()) {
            Objects.requireNonNull(e.getKey(), "property key");
            Objects.requireNonNull(e.getValue(), () -> "property value for key " + e.getKey());
            copy.put(e.getKey(), e.getValue().clone());
        }
        return java.util.Collections.unmodifiableMap(copy);
    }
}
