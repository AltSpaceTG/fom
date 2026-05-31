package io.fom.serde;

import java.io.ObjectInputFilter;

/**
 * Factory for hardened {@link ObjectInputFilter}s used wherever the engine
 * reads Java-serialised bytes back into objects.
 *
 * <p>Unfiltered {@link java.io.ObjectInputStream#readObject()} is the classic
 * gadget-chain remote-code-execution sink: an attacker who can influence the
 * bytes (a tampered log file, a writable row in the log table, a file handed
 * to the CLI) can drive deserialisation through whatever dangerous classes
 * happen to be on the application classpath. These filters blunt that.</p>
 */
public final class ObjectInputFilters {

    private ObjectInputFilters() {
    }

    /** Conservative caps to blunt deserialisation bombs (oversized graphs/arrays). */
    private static final long MAX_STREAM_BYTES = 64L * 1024 * 1024; // 64 MiB
    private static final long MAX_DEPTH = 64;
    private static final long MAX_REFERENCES = 1_000_000;
    private static final long MAX_ARRAY_LENGTH = 10_000_000;

    /**
     * Allowlist filter for the engine's own log payloads. The log only ever
     * serialises {@code io.fom.*} event records plus JDK types (String, Number,
     * collections, {@code byte[]}); any other class — i.e. every gadget-chain
     * class that might sit on the application classpath — is rejected. Resource
     * caps are enforced as well.
     */
    public static ObjectInputFilter logPayload() {
        return info -> {
            ObjectInputFilter.Status limit = withinLimits(info);
            if (limit != ObjectInputFilter.Status.UNDECIDED) {
                return limit;
            }
            Class<?> clazz = info.serialClass();
            if (clazz == null) {
                return ObjectInputFilter.Status.UNDECIDED;
            }
            Class<?> base = clazz;
            while (base.isArray()) {
                base = base.getComponentType();
            }
            if (base.isPrimitive()) {
                return ObjectInputFilter.Status.ALLOWED;
            }
            String name = base.getName();
            if (name.startsWith("io.fom.") || name.startsWith("java.")) {
                return ObjectInputFilter.Status.ALLOWED;
            }
            return ObjectInputFilter.Status.REJECTED;
        };
    }

    /**
     * Resource-cap-only filter for the general-purpose
     * {@code JavaSerializableSerDe}, which by contract must accept arbitrary
     * user-supplied {@link java.io.Serializable} trigger/graph values and so
     * cannot allowlist classes. It still enforces depth/reference/byte/array
     * caps. Production deployments should supply a stricter allowlist via
     * {@code new JavaSerializableSerDe(filter)} or use a binary SerDe.
     */
    public static ObjectInputFilter resourceLimits() {
        return ObjectInputFilters::withinLimits;
    }

    private static ObjectInputFilter.Status withinLimits(ObjectInputFilter.FilterInfo info) {
        if (info.depth() > MAX_DEPTH
                || info.references() > MAX_REFERENCES
                || info.streamBytes() > MAX_STREAM_BYTES
                || info.arrayLength() > MAX_ARRAY_LENGTH) {
            return ObjectInputFilter.Status.REJECTED;
        }
        return ObjectInputFilter.Status.UNDECIDED;
    }
}
