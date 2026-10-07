package io.fom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SequencedSet;
import java.util.Set;

/**
 * An immutable process topology, the value handed to {@code Engine.newGraph(...)}:
 * every node by name, and which query classes route to which process.
 *
 * <p>The constructor rejects a {@code top} missing from {@code nodes}, a dependency
 * on a missing node, a cycle, and a static route to a missing node. Dynamic routes
 * are checked when a query is dispatched.</p>
 */
public record Graph(ProcessNode top,
                    Map<String, ProcessNode> nodes,
                    Map<Class<?>, QueryRoute> typeRouting) {

    public Graph {
        Objects.requireNonNull(top, "top");
        Objects.requireNonNull(nodes, "nodes");
        Objects.requireNonNull(typeRouting, "typeRouting");
        // Validate the copy, so a map changed concurrently can't slip past the checks. Not Map.copyOf:
        // its order varies per JVM, and the node order decides the start order of independent nodes.
        nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
        typeRouting = Map.copyOf(typeRouting);
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
            GraphBuilder.requireRoutable(entry.getKey());
            if (entry.getValue() instanceof QueryRoute.Static s && !nodes.containsKey(s.processName())) {
                throw new IllegalArgumentException(
                        "type routing for " + entry.getKey().getName()
                                + " → '" + s.processName() + "' which is not in nodes");
            }
        }
    }

    /**
     * This graph without the nodes in {@code names}. Static routes to removed
     * nodes are dropped; if {@code top} is removed, the last remaining node in
     * topological order becomes the new top.
     *
     * @throws IllegalArgumentException if a remaining node depends on a removed
     *         one, or if nothing would remain
     */
    public Graph without(Set<String> names) {
        Objects.requireNonNull(names, "names");
        Map<String, ProcessNode> kept = new LinkedHashMap<>(nodes);
        kept.keySet().removeAll(names);
        if (kept.isEmpty()) {
            throw new IllegalArgumentException("Removing " + names + " would leave an empty graph");
        }
        for (ProcessNode node : kept.values()) {
            for (Dependency dep : node.dependencies()) {
                if (names.contains(dep.name())) {
                    throw new IllegalArgumentException(
                            "Cannot remove '" + dep.name() + "': '" + node.name() + "' depends on it");
                }
            }
        }
        Map<Class<?>, QueryRoute> routes = new LinkedHashMap<>();
        typeRouting.forEach((type, route) -> {
            if (!(route instanceof QueryRoute.Static s && names.contains(s.processName()))) {
                routes.put(type, route);
            }
        });
        ProcessNode newTop = top;
        if (!kept.containsKey(top.name())) {
            List<ProcessNode> order = topologicalOrder();
            for (int i = order.size() - 1; i >= 0; i--) {
                if (kept.containsKey(order.get(i).name())) {
                    newTop = order.get(i);
                    break;
                }
            }
        }
        return new Graph(newTop, kept, routes);
    }

    /**
     * Dependencies before their consumers, otherwise in insertion order. The engine
     * starts nodes in this order, so a consumer's {@code init} finds its dependencies serving.
     */
    public List<ProcessNode> topologicalOrder() {
        Set<String> visited = new HashSet<>();
        List<ProcessNode> order = new ArrayList<>(nodes.size());
        for (String name : nodes.keySet()) {
            visit(name, visited, order);
        }
        return order;
    }

    private void visit(String name, Set<String> visited, List<ProcessNode> order) {
        if (!visited.add(name)) return;
        ProcessNode node = nodes.get(name);
        for (Dependency dep : node.dependencies()) {
            visit(dep.name(), visited, order);
        }
        order.add(node);
    }

    private static void verifyAcyclic(Map<String, ProcessNode> nodes) {
        Set<String> done = new HashSet<>();
        for (String name : nodes.keySet()) {
            findCycle(name, nodes, new LinkedHashSet<>(), done);
        }
    }

    /** Depth-first; {@code path} holds the nodes being visited, so meeting one of them again is a cycle. */
    private static void findCycle(String name, Map<String, ProcessNode> nodes,
                                  SequencedSet<String> path, Set<String> done) {
        if (done.contains(name)) return;
        if (path.contains(name)) {
            List<String> onPath = new ArrayList<>(path);
            List<String> cycle = new ArrayList<>(onPath.subList(onPath.indexOf(name), onPath.size()));
            cycle.add(name);
            throw new IllegalArgumentException("Cycle detected: " + String.join(" -> ", cycle));
        }
        path.add(name);
        for (Dependency dep : nodes.get(name).dependencies()) {
            findCycle(dep.name(), nodes, path, done);
        }
        path.removeLast();
        done.add(name);
    }
}
