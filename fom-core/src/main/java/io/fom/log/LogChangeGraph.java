package io.fom.log;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * A new process graph was committed. Only the graph's <em>structure</em> is
 * recorded — per node its name, dependencies and serialized {@code param} —
 * which is what a restart needs to tell whether a node's definition changed
 * since its state was persisted. Factories and routes are not recorded: they
 * live in code, and after a restart the application installs its graph again.
 */
public record LogChangeGraph(long clock, long timestamp, short formatVersion, List<Node> nodes)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 2;

    public LogChangeGraph {
        nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes"));
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
    }

    public LogChangeGraph(long clock, long timestamp, List<Node> nodes) {
        this(clock, timestamp, CURRENT_FORMAT, nodes);
    }

    /**
     * One node's definition.
     *
     * @param param the node's {@code param} as serialized by the engine's
     *              {@code SerDe}, or {@code null} if the node has none
     */
    public record Node(String name,
                       List<String> reactiveDependencies,
                       List<String> stableDependencies,
                       byte[] param) implements Serializable {

        public Node {
            Objects.requireNonNull(name, "name");
            reactiveDependencies = List.copyOf(Objects.requireNonNull(reactiveDependencies, "reactiveDependencies"));
            stableDependencies = List.copyOf(Objects.requireNonNull(stableDependencies, "stableDependencies"));
        }
    }
}
