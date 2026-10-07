package io.fom;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * One graph node: its name, dependencies, optional {@code param}, and the
 * factories of its initializer and loader.
 *
 * <p>The factories never leave this JVM and are not logged, so they may capture
 * anything (a DI container, a data source). A non-null {@code param} must be
 * immutable and implement {@code equals}: it is written to the log through the
 * {@code SerDe}, and a restart or a graph swap compares it to tell whether the node
 * changed.</p>
 *
 * <p>{@code reinitStrategy} overrides {@link EngineConfig#reinitStrategy()} for this node, or is
 * {@code null} to follow it. It is an operational choice, not part of the definition: changing it
 * alone does not make a graph swap re-initialise the node.</p>
 */
public record ProcessNode(String name,
                          List<Dependency> dependencies,
                          Serializable param,
                          Supplier<?> initFactory,
                          Supplier<?> loadFactory,
                          ReinitStrategy reinitStrategy) {

    public ProcessNode {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("ProcessNode name must not be empty or blank");
        }
        Objects.requireNonNull(dependencies, "dependencies");
        Objects.requireNonNull(initFactory, "initFactory");
        Objects.requireNonNull(loadFactory, "loadFactory");
        dependencies = List.copyOf(dependencies);

        var seen = new HashSet<String>();
        for (Dependency dependency : dependencies) {
            if (!seen.add(dependency.name())) {
                throw new IllegalArgumentException("ProcessNode '" + name + "' names dependency '"
                        + dependency.name() + "' more than once");
            }
        }
    }

    public ProcessNode(String name, List<Dependency> dependencies, Serializable param,
                       Supplier<?> initFactory, Supplier<?> loadFactory) {
        this(name, dependencies, param, initFactory, loadFactory, null);
    }

    /** This node with another re-init strategy ({@code null}: the engine's). */
    public ProcessNode withReinitStrategy(ReinitStrategy strategy) {
        return new ProcessNode(name, dependencies, param, initFactory, loadFactory, strategy);
    }
}
