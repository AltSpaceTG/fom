package io.fom;

import java.io.Serializable;
import java.util.Objects;

/**
 * A typed, refactor-safe handle for a process name.
 *
 * <p>A process is identified across JVM restarts by its <em>name</em> — that
 * string is what the log persists (in {@link Sid}, {@code LogInitialized},
 * {@code LogDead}, …), so the durable identity is always {@link #name()}.
 * {@code ProcessRef} does not change that wire format; it is purely a
 * compile-time convenience so callers pass a shared constant instead of
 * scattering bare string literals.</p>
 *
 * <p>Define one constant per process and reference it everywhere — in
 * {@link GraphBuilder#add(ProcessRef, SerializableSupplier, SerializableSupplier, ProcessRef...)},
 * in dependency declarations, in {@code engine.queryProcess(ref, …)} /
 * {@code engine.trigger(ref, …)}, and in {@code ctx.query(ref, …)}:</p>
 *
 * <pre>{@code
 * final class InventoryInit implements ProcessInitializer, ProcessLoader {
 *     static final ProcessRef REF = ProcessRef.of("Inventory");
 *     ...
 * }
 * }</pre>
 *
 * <p>Every API that accepts a {@code ProcessRef} also has a {@code String}
 * overload, so the two styles interoperate freely.</p>
 */
public record ProcessRef(String name) implements Serializable {

    public ProcessRef {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("ProcessRef name must not be empty");
        }
    }

    /** Convenience factory: {@code ProcessRef.of("Inventory")}. */
    public static ProcessRef of(String name) {
        return new ProcessRef(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
