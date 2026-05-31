package io.fom;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable persistent wrapper over {@code Map<String, byte[]>}.
 * Each mutating method returns a new {@code Properties} instance.
 *
 * <p>Provides typed access via {@link TypedKey} on top of the raw
 * {@code byte[]} cells defined by {@code ProcessProperties}.</p>
 */
public final class Properties {

    private static final Properties EMPTY = new Properties(Map.of());

    private final Map<String, byte[]> data;

    private Properties(Map<String, byte[]> data) {
        this.data = data;
    }

    public static Properties empty() {
        return EMPTY;
    }

    public static Properties of(Map<String, byte[]> raw) {
        Objects.requireNonNull(raw, "raw");
        if (raw.isEmpty()) return EMPTY;
        var copy = new LinkedHashMap<String, byte[]>(raw.size());
        for (var e : raw.entrySet()) {
            Objects.requireNonNull(e.getKey(), "raw entry key");
            Objects.requireNonNull(e.getValue(), () -> "raw entry value for key " + e.getKey());
            copy.put(e.getKey(), e.getValue().clone());
        }
        return new Properties(Collections.unmodifiableMap(copy));
    }

    public <T> Properties put(TypedKey<T> key, T value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        return putRaw(key.name(), key.codec().encode(value));
    }

    public Properties putRaw(String key, byte[] value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        var next = new LinkedHashMap<>(data);
        next.put(key, value.clone());
        return new Properties(Collections.unmodifiableMap(next));
    }

    public <T> T get(TypedKey<T> key) {
        Objects.requireNonNull(key, "key");
        byte[] bytes = data.get(key.name());
        if (bytes == null) {
            throw new NoSuchPropertyException(key.name());
        }
        return key.codec().decode(bytes);
    }

    public <T> Optional<T> getOpt(TypedKey<T> key) {
        Objects.requireNonNull(key, "key");
        byte[] bytes = data.get(key.name());
        return bytes == null ? Optional.empty() : Optional.of(key.codec().decode(bytes));
    }

    public byte[] getRaw(String key) {
        Objects.requireNonNull(key, "key");
        byte[] bytes = data.get(key);
        if (bytes == null) {
            throw new NoSuchPropertyException(key);
        }
        return bytes.clone();
    }

    public Optional<byte[]> getRawOpt(String key) {
        Objects.requireNonNull(key, "key");
        byte[] bytes = data.get(key);
        return bytes == null ? Optional.empty() : Optional.of(bytes.clone());
    }

    public boolean contains(String key) {
        Objects.requireNonNull(key, "key");
        return data.containsKey(key);
    }

    public int size() {
        return data.size();
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * Returns an unmodifiable view of the underlying map.
     * The returned map shares storage with this Properties instance and
     * must not be mutated; the byte arrays inside are also shared and
     * must be treated as read-only by callers.
     */
    public Map<String, byte[]> asRaw() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Properties other)) return false;
        if (data.size() != other.data.size()) return false;
        for (var e : data.entrySet()) {
            byte[] mine = e.getValue();
            byte[] theirs = other.data.get(e.getKey());
            if (theirs == null || !java.util.Arrays.equals(mine, theirs)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (var e : data.entrySet()) {
            h += e.getKey().hashCode() ^ java.util.Arrays.hashCode(e.getValue());
        }
        return h;
    }

    @Override
    public String toString() {
        return "Properties{size=" + data.size() + ", keys=" + data.keySet() + "}";
    }
}
