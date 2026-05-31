package io.fom.fury;

import io.fom.Graph;
import io.fom.serde.SerDe;
import io.fom.serde.SerDeException;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link SerDe} backed by Apache Fury — compact, fast, schema-evolution
 * friendly. Configured in {@code Compatible} mode so adding a new nullable
 * field to a record does not break readers built against the older schema.
 *
 * <p>Construct one instance per Engine and share it: the underlying
 * {@link ThreadSafeFury} caches reflection metadata and pre-builds codecs
 * lazily on first encounter of a class.</p>
 */
public final class FurySerDe implements SerDe {

    private final ThreadSafeFury fury;

    public FurySerDe() {
        this(false);
    }

    /**
     * @param requireClassRegistration when {@code true}, Fury rejects any class
     *        not explicitly registered — its strongest defence against
     *        deserialisation attacks when log bytes may be tampered with. You
     *        must then register every type that flows through trigger values,
     *        process params and the graph. Leave {@code false} (the default) for
     *        the permissive, zero-configuration behaviour suited to trusted logs.
     */
    public FurySerDe(boolean requireClassRegistration) {
        this.fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(true)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .requireClassRegistration(requireClassRegistration)
                .buildThreadSafeFury();
    }

    @Override
    public byte[] serializeTriggerValue(String processName, Object value) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(value, "value");
        try {
            return fury.serialize(value);
        } catch (Throwable t) {
            throw new SerDeException("Fury serialise trigger for " + processName + " failed", t);
        }
    }

    @Override
    public Object loadTriggerValue(String processName, byte[] bytes) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(bytes, "bytes");
        try {
            return fury.deserialize(bytes);
        } catch (Throwable t) {
            throw new SerDeException("Fury deserialise trigger for " + processName + " failed", t);
        }
    }

    @Override
    public byte[] serializeProcessProperties(Map<String, byte[]> properties) {
        Objects.requireNonNull(properties, "properties");
        var copy = new HashMap<String, byte[]>(properties.size());
        for (var e : properties.entrySet()) {
            Objects.requireNonNull(e.getKey(), "property key");
            Objects.requireNonNull(e.getValue(), () -> "property value for " + e.getKey());
            copy.put(e.getKey(), e.getValue());
        }
        try {
            return fury.serialize(copy);
        } catch (Throwable t) {
            throw new SerDeException("Fury serialise properties failed", t);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, byte[]> loadProcessProperties(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            Object obj = fury.deserialize(bytes);
            if (!(obj instanceof Map<?, ?> raw)) {
                throw new SerDeException("Expected Map for ProcessProperties, got " + obj.getClass().getName());
            }
            var out = new LinkedHashMap<String, byte[]>(raw.size());
            for (var e : raw.entrySet()) {
                if (!(e.getKey() instanceof String key)) {
                    throw new SerDeException("Non-String key in ProcessProperties: " + e.getKey());
                }
                if (!(e.getValue() instanceof byte[] val)) {
                    throw new SerDeException("Non-byte[] value for key " + key);
                }
                out.put(key, val);
            }
            return out;
        } catch (SerDeException e) {
            throw e;
        } catch (Throwable t) {
            throw new SerDeException("Fury deserialise properties failed", t);
        }
    }

    @Override
    public byte[] serializeProcessGraph(Graph graph) {
        Objects.requireNonNull(graph, "graph");
        try {
            return fury.serialize(graph);
        } catch (Throwable t) {
            throw new SerDeException("Fury serialise graph failed", t);
        }
    }

    @Override
    public Graph loadProcessGraph(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            Object obj = fury.deserialize(bytes);
            if (!(obj instanceof Graph g)) {
                throw new SerDeException("Expected Graph, got " + obj.getClass().getName());
            }
            return g;
        } catch (SerDeException e) {
            throw e;
        } catch (Throwable t) {
            throw new SerDeException("Fury deserialise graph failed", t);
        }
    }
}
