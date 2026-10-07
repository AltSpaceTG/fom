package io.fom;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * An immutable {@code Map<String, byte[]>} of a process's persisted cells, with typed
 * access through {@link TypedKey}. {@code put} returns a new instance; use
 * {@link #builder()} for many cells.
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

    /** A mutable builder; unlike {@link #put}, it does not copy the map on every cell. */
    public static Builder builder() {
        return new Builder(new LinkedHashMap<>());
    }

    /** A builder starting from these cells. */
    public Builder toBuilder() {
        return new Builder(new LinkedHashMap<>(data));
    }

    /** Collects cells, then {@link #build()}s an immutable {@link Properties}. Not thread-safe. */
    public static final class Builder {
        private LinkedHashMap<String, byte[]> cells;

        private Builder(LinkedHashMap<String, byte[]> cells) {
            this.cells = cells;
        }

        public <T> Builder put(TypedKey<T> key, T value) {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            return putRaw(key.name(), key.codec().encode(value));
        }

        public Builder putRaw(String key, byte[] value) {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            requireOpen().put(key, value.clone());
            return this;
        }

        /** The cells collected so far; the builder cannot be used afterwards. */
        public Properties build() {
            var built = requireOpen();
            cells = null; // hand the map over without copying it
            return built.isEmpty() ? EMPTY : new Properties(Collections.unmodifiableMap(built));
        }

        private LinkedHashMap<String, byte[]> requireOpen() {
            if (cells == null) throw new IllegalStateException("build() was already called");
            return cells;
        }
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

    public <T> Optional<T> find(TypedKey<T> key) {
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

    public Optional<byte[]> findRaw(String key) {
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

    /** The underlying map, unmodifiable. Its byte arrays are shared: don't change them. */
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
            if (theirs == null || !Arrays.equals(mine, theirs)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (var e : data.entrySet()) {
            h += e.getKey().hashCode() ^ Arrays.hashCode(e.getValue());
        }
        return h;
    }

    @Override
    public String toString() {
        return "Properties{size=" + data.size() + ", keys=" + data.keySet() + "}";
    }
}
