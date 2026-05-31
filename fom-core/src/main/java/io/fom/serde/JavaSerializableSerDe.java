package io.fom.serde;

import io.fom.Graph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Fallback {@link SerDe} backed by {@link ObjectOutputStream} /
 * {@link ObjectInputStream}.
 *
 * <p><strong>Security:</strong> Java deserialisation is a classic
 * remote-code-execution sink. Because this SerDe must accept arbitrary
 * user-supplied {@link Serializable} trigger/graph values, it cannot
 * allowlist classes by package; instead every {@code readObject()} runs
 * under an {@link ObjectInputFilter}. The default
 * ({@link ObjectInputFilters#resourceLimits()}) caps depth/refs/bytes/array
 * size to blunt deserialisation bombs but still admits unknown user classes.
 * Production deployments handling untrusted log bytes should either use
 * {@code FurySerDe} from {@code fom-fury} or pass a strict allowlist via
 * {@link #JavaSerializableSerDe(ObjectInputFilter)} (or set the JVM-wide
 * {@code jdk.serialFilter}).</p>
 *
 * <p>All inputs must implement {@link Serializable}. {@code ProcessProperties}
 * are serialised as a regular {@code Map<String, byte[]>}.</p>
 */
public final class JavaSerializableSerDe implements SerDe {

    private final ObjectInputFilter inputFilter;

    /** Uses {@link ObjectInputFilters#resourceLimits()} as the deserialisation filter. */
    public JavaSerializableSerDe() {
        this(ObjectInputFilters.resourceLimits());
    }

    /**
     * @param inputFilter the {@link ObjectInputFilter} applied to every
     *        {@code readObject()}; supply a strict allowlist for production use.
     */
    public JavaSerializableSerDe(ObjectInputFilter inputFilter) {
        this.inputFilter = Objects.requireNonNull(inputFilter, "inputFilter");
    }

    @Override
    public byte[] serializeTriggerValue(String processName, Object value) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(value, "value");
        if (!(value instanceof Serializable)) {
            throw new SerDeException(
                    "Trigger value for " + processName + " is not Serializable: " + value.getClass().getName());
        }
        return writeObject(value);
    }

    @Override
    public Object loadTriggerValue(String processName, byte[] bytes) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(bytes, "bytes");
        return readObject(bytes);
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
        return writeObject(copy);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, byte[]> loadProcessProperties(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        Object obj = readObject(bytes);
        if (!(obj instanceof Map<?, ?> raw)) {
            throw new SerDeException("Expected Map for ProcessProperties, got " + obj.getClass().getName());
        }
        var out = new LinkedHashMap<String, byte[]>(raw.size());
        for (var e : raw.entrySet()) {
            if (!(e.getKey() instanceof String key)) {
                throw new SerDeException("Non-String key in ProcessProperties: " + e.getKey());
            }
            if (!(e.getValue() instanceof byte[] val)) {
                throw new SerDeException("Non-byte[] value in ProcessProperties for key " + key);
            }
            out.put(key, val);
        }
        return out;
    }

    @Override
    public byte[] serializeProcessGraph(Graph graph) {
        Objects.requireNonNull(graph, "graph");
        return writeObject(graph);
    }

    @Override
    public Graph loadProcessGraph(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        Object obj = readObject(bytes);
        if (!(obj instanceof Graph g)) {
            throw new SerDeException("Expected Graph, got " + obj.getClass().getName());
        }
        return g;
    }

    private static byte[] writeObject(Object value) {
        try (var baos = new ByteArrayOutputStream();
             var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new SerDeException("Java serialisation failed", e);
        }
    }

    private Object readObject(byte[] bytes) {
        try (var bais = new ByteArrayInputStream(bytes);
             var ois = new ObjectInputStream(bais)) {
            ois.setObjectInputFilter(inputFilter);
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SerDeException("Java deserialisation failed", e);
        }
    }
}
