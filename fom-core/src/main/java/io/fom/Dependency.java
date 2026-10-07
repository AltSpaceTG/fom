package io.fom;

import java.io.Serializable;
import java.util.Objects;

/**
 * An edge of the process graph: whether a new {@link Sid} of the dependency
 * re-initialises the consumer.
 *
 * <p>{@link Reactive} (the default) re-initialises the consumer. {@link Stable}
 * leaves it alone; the consumer can still {@code ctx.query()} the dependency's
 * current state when it needs it.</p>
 */
public sealed interface Dependency extends Serializable permits Dependency.Reactive, Dependency.Stable {

    String name();

    record Reactive(String name) implements Dependency {
        public Reactive {
            requireName(name);
        }
    }

    record Stable(String name) implements Dependency {
        public Stable {
            requireName(name);
        }
    }

    static Dependency reactive(String name) {
        return new Reactive(name);
    }

    static Dependency stable(String name) {
        return new Stable(name);
    }

    static Dependency reactive(ProcessRef ref) {
        Objects.requireNonNull(ref, "ref");
        return new Reactive(ref.name());
    }

    static Dependency stable(ProcessRef ref) {
        Objects.requireNonNull(ref, "ref");
        return new Stable(ref.name());
    }

    private static void requireName(String name) {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("dependency name must not be empty or blank");
        }
    }
}
