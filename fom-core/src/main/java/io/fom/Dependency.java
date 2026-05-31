package io.fom;

import java.io.Serializable;
import java.util.Objects;

/**
 * Declaration of an edge in the process graph.
 * Determines whether a change of the dependency's {@link Sid}
 * cascades a re-init into the consumer.
 *
 * <p>{@link Reactive} (default): a change of the dependency's Sid
 * forces the consumer to re-init.</p>
 *
 * <p>{@link Stable}: a change of the dependency's Sid does <em>not</em>
 * invalidate the consumer. The consumer may re-read the current state
 * of the dependency via {@code ctx.query()} on demand.</p>
 */
public sealed interface Dependency extends Serializable permits Dependency.Reactive, Dependency.Stable {

    String name();

    record Reactive(String name) implements Dependency {
        public Reactive {
            Objects.requireNonNull(name, "name");
            if (name.isEmpty()) {
                throw new IllegalArgumentException("dependency name must not be empty");
            }
        }
    }

    record Stable(String name) implements Dependency {
        public Stable {
            Objects.requireNonNull(name, "name");
            if (name.isEmpty()) {
                throw new IllegalArgumentException("dependency name must not be empty");
            }
        }
    }

    static Dependency reactive(String name) {
        return new Reactive(name);
    }

    static Dependency stable(String name) {
        return new Stable(name);
    }
}
