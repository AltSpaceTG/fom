package io.fom.log;

import io.fom.Sid;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A process finished {@code init}. Its {@code Sid} is {@code processName} plus
 * this event's clock.
 *
 * <p>{@code properties} is what {@code ProcessLoader.load} gets after a restart;
 * it is deep-copied on construction.</p>
 *
 * <p>{@code replaces} is the still-serving version this init is meant to
 * replace, or {@code null} for a cold init. Until a {@link LogLoaded} of this
 * {@code Sid} promotes it, the replaced version stays the live one.</p>
 */
public record LogInitialized(long clock,
                             long timestamp,
                             short formatVersion,
                             String processName,
                             Map<String, byte[]> properties,
                             Sid replaces)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogInitialized {
        Objects.requireNonNull(processName, "processName");
        if (processName.isEmpty()) {
            throw new IllegalArgumentException("processName must not be empty");
        }
        Objects.requireNonNull(properties, "properties");
        properties = deepCopy(properties);
        if (replaces != null && !replaces.processName().equals(processName)) {
            throw new IllegalArgumentException("replaces " + replaces + " is not a version of " + processName);
        }
    }

    public LogInitialized(long clock, long timestamp, short formatVersion, String processName,
                          Map<String, byte[]> properties) {
        this(clock, timestamp, formatVersion, processName, properties, null);
    }

    public LogInitialized(long clock, long timestamp, String processName, Map<String, byte[]> properties) {
        this(clock, timestamp, CURRENT_FORMAT, processName, properties, null);
    }

    public LogInitialized(long clock, long timestamp, String processName, Map<String, byte[]> properties,
                          Sid replaces) {
        this(clock, timestamp, CURRENT_FORMAT, processName, properties, replaces);
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
        return Collections.unmodifiableMap(copy);
    }
}
