package io.fom.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

/**
 * {@link SerDe} on plain Java serialisation.
 *
 * <p><strong>Security:</strong> Java deserialisation is a classic remote-code-execution
 * sink, and params are arbitrary user classes, so no package allowlist fits. Every
 * {@code readObject()} runs under an {@link ObjectInputFilter}; the default,
 * {@link ObjectInputFilters#resourceLimits()}, only caps size and depth and still admits
 * any class. If the log bytes may be untrusted, pass a strict allowlist to
 * {@link #JavaSerializableSerDe(ObjectInputFilter)}, set {@code jdk.serialFilter}, or use
 * {@code FurySerDe.strict(...)} from {@code fom-fury}.</p>
 */
public final class JavaSerializableSerDe implements SerDe {

    private final ObjectInputFilter inputFilter;

    /** Uses {@link ObjectInputFilters#resourceLimits()} as the deserialisation filter. */
    public JavaSerializableSerDe() {
        this(ObjectInputFilters.resourceLimits());
    }

    /** @param inputFilter applied to every {@code readObject()}; use a strict allowlist in production */
    public JavaSerializableSerDe(ObjectInputFilter inputFilter) {
        this.inputFilter = Objects.requireNonNull(inputFilter, "inputFilter");
    }

    @Override
    public byte[] serializeParam(String processName, Serializable param) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(param, "param");
        try (var baos = new ByteArrayOutputStream();
             var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(param);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new SerDeException("Java serialisation of the param of " + processName + " failed", e);
        }
    }

    @Override
    public Object loadParam(String processName, byte[] bytes) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(bytes, "bytes");
        try (var bais = new ByteArrayInputStream(bytes);
             var ois = new ObjectInputStream(bais)) {
            ois.setObjectInputFilter(inputFilter);
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SerDeException("Java deserialisation of the param of " + processName + " failed", e);
        }
    }
}
