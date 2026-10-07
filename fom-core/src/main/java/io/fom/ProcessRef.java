package io.fom;

import java.io.Serializable;
import java.util.Objects;

/**
 * A typed handle for a process name, so callers share a constant instead of
 * repeating string literals. The log still stores only the name.
 *
 * <pre>{@code
 * final class StationsInit implements ProcessInitializer, ProcessLoader {
 *     static final ProcessRef REF = ProcessRef.of("Stations");
 *     ...
 * }
 * }</pre>
 *
 * <p>Every API that takes a {@code ProcessRef} ({@link GraphBuilder}, {@link Dependency},
 * {@code engine.queryProcess}, {@code engine.trigger}, {@code ctx.query}) also takes the
 * plain name.</p>
 */
public record ProcessRef(String name) implements Serializable {

    public ProcessRef {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("ProcessRef name must not be empty or blank");
        }
    }

    public static ProcessRef of(String name) {
        return new ProcessRef(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
