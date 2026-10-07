package io.fom.serde;

import java.io.ObjectInputFilter;
import java.io.ObjectInputFilter.FilterInfo;
import java.io.ObjectInputFilter.Status;
import java.util.Locale;

/**
 * {@link ObjectInputFilter}s for every place the engine reads Java-serialised bytes.
 *
 * <p>An unfiltered {@code readObject()} is the classic gadget-chain remote-code-execution
 * sink: whoever can change the bytes (a tampered log file, a writable row of the log
 * table, a file handed to the CLI) can drive whatever dangerous classes sit on the
 * classpath.</p>
 */
public final class ObjectInputFilters {

    private ObjectInputFilters() {
    }

    private static final long MAX_STREAM_BYTES = 64L * 1024 * 1024;
    private static final long MAX_DEPTH = 64;
    private static final long MAX_REFERENCES = 1_000_000;
    private static final long MAX_ARRAY_LENGTH = 10_000_000;

    /** The limits {@link #logPayload()} enforces, for error messages. */
    public static String logPayloadLimits() {
        return "at most " + (MAX_STREAM_BYTES / (1024 * 1024)) + " MiB per event, arrays of at most "
                + grouped(MAX_ARRAY_LENGTH) + " elements (e.g. a byte[] property), "
                + grouped(MAX_REFERENCES) + " object references, nesting depth " + MAX_DEPTH;
    }

    /**
     * Allowlist for the engine's own log payloads: {@code io.fom.*} events plus JDK types
     * (strings, numbers, collections, {@code byte[]}). Every other class is rejected, and the
     * resource caps apply too.
     */
    public static ObjectInputFilter logPayload() {
        return info -> {
            Status limit = withinLimits(info);
            if (limit != Status.UNDECIDED) {
                return limit;
            }
            Class<?> clazz = info.serialClass();
            if (clazz == null) {
                return Status.UNDECIDED;
            }
            Class<?> base = clazz;
            while (base.isArray()) {
                base = base.getComponentType();
            }
            if (base.isPrimitive()) {
                return Status.ALLOWED;
            }
            String name = base.getName();
            if (name.startsWith("io.fom.") || name.startsWith("java.")) {
                return Status.ALLOWED;
            }
            return Status.REJECTED;
        };
    }

    /**
     * Resource caps only (depth, references, bytes, array length), for
     * {@link JavaSerializableSerDe}: params are arbitrary user classes, so it cannot
     * allowlist. In production pass a stricter filter to
     * {@code new JavaSerializableSerDe(filter)} or use another SerDe.
     */
    public static ObjectInputFilter resourceLimits() {
        return ObjectInputFilters::withinLimits;
    }

    private static Status withinLimits(FilterInfo info) {
        if (info.depth() > MAX_DEPTH
                || info.references() > MAX_REFERENCES
                || info.streamBytes() > MAX_STREAM_BYTES
                || info.arrayLength() > MAX_ARRAY_LENGTH) {
            return Status.REJECTED;
        }
        return Status.UNDECIDED;
    }

    private static String grouped(long n) {
        return String.format(Locale.ROOT, "%,d", n);
    }
}
