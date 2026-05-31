package io.fom;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Serialisable declaration of a single graph node — what the engine writes
 * into a {@code LogChangeGraph} event and what the loader rehydrates after a
 * JVM restart.
 *
 * <p>{@code initFactory} and {@code loadFactory} are {@link SerializableSupplier}s
 * so that the same factory references can be revived from the log without
 * the engine knowing the concrete classes at startup.</p>
 *
 * <p>{@code param} is nullable. When non-null it MUST be {@link Serializable}
 * and immutable. Two nodes are considered equal for graph diff
 * purposes when their {@code name}, {@code param} (via {@code equals}) and
 * the set of {@code dependencies} match.</p>
 */
public record ProcessNode(String name,
                          List<Dependency> dependencies,
                          Serializable param,
                          SerializableSupplier<?> initFactory,
                          SerializableSupplier<?> loadFactory) implements Serializable {

    public ProcessNode {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("ProcessNode name must not be empty");
        }
        Objects.requireNonNull(dependencies, "dependencies");
        Objects.requireNonNull(initFactory, "initFactory");
        Objects.requireNonNull(loadFactory, "loadFactory");
        dependencies = List.copyOf(dependencies);

        long distinct = dependencies.stream().map(Dependency::name).distinct().count();
        if (distinct != dependencies.size()) {
            throw new IllegalArgumentException(
                    "ProcessNode '" + name + "' has duplicate dependency names");
        }
    }
}
