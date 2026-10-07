package io.fom.fury;

import io.fom.serde.SerDe;
import io.fom.serde.SerDeException;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * {@link SerDe} backed by Apache Fury. Runs in {@code Compatible} mode, so adding a
 * nullable field to a record does not break readers of older logs.
 *
 * <p>Share one instance per engine: Fury caches codecs per class.</p>
 *
 * <h2>Strict mode</h2>
 *
 * <p>{@link #strict(Class[])} (or {@code new FurySerDe(true, classes)}) refuses any class
 * that is not registered, when writing and when reading. Fury registers JDK collections,
 * boxed primitives, strings and {@code byte[]} itself; register every process param type
 * ({@code GraphBuilder.addWithParam}) and any non-JDK type nested in it.</p>
 *
 * <p>Registration assigns ids in order, so every JVM reading a log must register the same
 * classes in the same order. Append new classes at the end; never reorder.</p>
 */
public final class FurySerDe implements SerDe {

    private final ThreadSafeFury fury;

    /** Permissive mode: no class registration; for trusted logs only. */
    public FurySerDe() {
        this(false);
    }

    /**
     * @param requireClassRegistration {@code true} rejects every unregistered class, the
     *        defence against tampered log bytes; register param types through
     *        {@link #FurySerDe(boolean, Collection)} or {@link #strict(Class[])}.
     */
    public FurySerDe(boolean requireClassRegistration) {
        this(requireClassRegistration, List.of());
    }

    /**
     * @param requireClassRegistration see {@link #FurySerDe(boolean)}
     * @param classesToRegister param classes to register, in a stable order
     */
    public FurySerDe(boolean requireClassRegistration, Collection<? extends Class<?>> classesToRegister) {
        Objects.requireNonNull(classesToRegister, "classesToRegister");
        this.fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(true)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .requireClassRegistration(requireClassRegistration)
                .buildThreadSafeFury();
        var ordered = new LinkedHashSet<Class<?>>();
        for (Class<?> c : classesToRegister) {
            ordered.add(Objects.requireNonNull(c, "class to register"));
        }
        for (Class<?> c : ordered) {
            fury.register(c);
        }
    }

    /** Strict (registration-required) SerDe with {@code paramClasses} registered, in the given order. */
    public static FurySerDe strict(Class<?>... paramClasses) {
        Objects.requireNonNull(paramClasses, "paramClasses");
        return new FurySerDe(true, List.of(paramClasses));
    }

    @Override
    public byte[] serializeParam(String processName, Serializable param) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(param, "param");
        try {
            return fury.serialize(param);
        } catch (Throwable t) {
            throw new SerDeException("Fury serialise param for " + processName + " failed", t);
        }
    }

    @Override
    public Object loadParam(String processName, byte[] bytes) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(bytes, "bytes");
        try {
            return fury.deserialize(bytes);
        } catch (Throwable t) {
            throw new SerDeException("Fury deserialise param for " + processName + " failed", t);
        }
    }
}
