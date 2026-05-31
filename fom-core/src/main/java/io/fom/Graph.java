package io.fom;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable snapshot of a process topology — the single value handed to
 * {@code Engine.newGraph(...)}.
 *
 * <p>{@code nodes} is the flat map of every {@link ProcessNode} reachable
 * from {@code top}. {@code typeRouting} declares which query classes the
 * top-level dispatcher routes to which process.</p>
 *
 * <p>The compact constructor fully validates the graph and fails fast:</p>
 * <ul>
 *   <li>{@code top.name()} must appear in {@code nodes}</li>
 *   <li>every {@link Dependency#name()} referenced by any node must exist in {@code nodes}</li>
 *   <li>the dependency graph must be acyclic (topological sort)</li>
 *   <li>every {@link QueryRoute.Static} target must exist in {@code nodes}
 *       (Dynamic routes are validated at dispatch time)</li>
 *   <li>{@code nodes} and {@code typeRouting} are defensively copied</li>
 * </ul>
 */
public record Graph(ProcessNode top,
                    Map<String, ProcessNode> nodes,
                    Map<Class<?>, QueryRoute> typeRouting) implements Serializable {

    public Graph {
        Objects.requireNonNull(top, "top");
        Objects.requireNonNull(nodes, "nodes");
        Objects.requireNonNull(typeRouting, "typeRouting");
        if (!nodes.containsKey(top.name())) {
            throw new IllegalArgumentException(
                    "top '" + top.name() + "' is not present in nodes map");
        }
        for (var entry : nodes.entrySet()) {
            ProcessNode node = entry.getValue();
            if (!entry.getKey().equals(node.name())) {
                throw new IllegalArgumentException(
                        "nodes key '" + entry.getKey() + "' does not match node name '" + node.name() + "'");
            }
        }
        for (ProcessNode node : nodes.values()) {
            for (Dependency dep : node.dependencies()) {
                if (!nodes.containsKey(dep.name())) {
                    throw new IllegalArgumentException(
                            "node '" + node.name() + "' depends on missing '" + dep.name() + "'");
                }
            }
        }
        verifyAcyclic(nodes);
        for (var entry : typeRouting.entrySet()) {
            if (entry.getValue() instanceof QueryRoute.Static s) {
                if (!nodes.containsKey(s.processName())) {
                    throw new IllegalArgumentException(
                            "type routing for " + entry.getKey().getName()
                                    + " → '" + s.processName() + "' which is not in nodes");
                }
            }
        }
        nodes = Map.copyOf(nodes);
        typeRouting = Map.copyOf(typeRouting);
    }

    /**
     * Topological order of {@code nodes}: dependencies precede their consumers.
     * Used by the engine to spawn FSMs leaf-first so a consumer's {@code init}
     * can issue a {@code ctx.query} against an already-{@code Serving} dependency.
     */
    public List<ProcessNode> topologicalOrder() {
        Map<String, Integer> colour = new HashMap<>();
        List<ProcessNode> order = new ArrayList<>(nodes.size());
        for (String name : nodes.keySet()) {
            if (colour.getOrDefault(name, 0) == 0) {
                topoVisit(name, colour, order);
            }
        }
        return order;
    }

    private void topoVisit(String name, Map<String, Integer> colour, List<ProcessNode> order) {
        colour.put(name, 1);
        ProcessNode node = nodes.get(name);
        for (Dependency dep : node.dependencies()) {
            int c = colour.getOrDefault(dep.name(), 0);
            if (c == 0) {
                topoVisit(dep.name(), colour, order);
            }
        }
        colour.put(name, 2);
        order.add(node);
    }

    private static void verifyAcyclic(Map<String, ProcessNode> nodes) {
        Map<String, Integer> colour = new HashMap<>();
        for (String name : nodes.keySet()) {
            if (colour.getOrDefault(name, 0) == 0) {
                dfsCycle(name, nodes, colour, new ArrayList<>(), new HashSet<>());
            }
        }
    }

    private static void dfsCycle(String name,
                                 Map<String, ProcessNode> nodes,
                                 Map<String, Integer> colour,
                                 List<String> path,
                                 Set<String> onPath) {
        colour.put(name, 1);
        path.add(name);
        onPath.add(name);
        for (Dependency dep : nodes.get(name).dependencies()) {
            int c = colour.getOrDefault(dep.name(), 0);
            if (c == 1 && onPath.contains(dep.name())) {
                int from = path.indexOf(dep.name());
                List<String> cycle = new ArrayList<>(path.subList(from, path.size()));
                cycle.add(dep.name());
                throw new IllegalArgumentException("Cycle detected: " + String.join(" -> ", cycle));
            }
            if (c == 0) {
                dfsCycle(dep.name(), nodes, colour, path, onPath);
            }
        }
        colour.put(name, 2);
        path.remove(path.size() - 1);
        onPath.remove(name);
    }
}
