package io.fom;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Difference between two {@link Graph}s. Computed by {@link #compute(Graph, Graph)}
 * and consumed by {@code GraphMachine.applyGraphChange} to decide which FSMs
 * to spawn, shut down, or leave untouched.
 *
 * <p>Node equivalence per TZ §6.4:</p>
 * <ul>
 *   <li>same {@code name}</li>
 *   <li>same set of dependency {@code name + kind} (order doesn't matter)</li>
 *   <li>same {@code param} (via {@code Objects.equals})</li>
 * </ul>
 *
 * <p>Factory identity ({@code initFactory}, {@code loadFactory}) is intentionally
 * NOT part of equivalence — two graph builds in the same JVM produce different
 * lambda instances even when the user logically meant "the same node", and the
 * spec says graph identity is structural.</p>
 */
public record GraphDiff(Set<String> added,
                        Set<String> removed,
                        Set<String> changed,
                        Set<String> unchanged) {

    public GraphDiff {
        added = Set.copyOf(added);
        removed = Set.copyOf(removed);
        changed = Set.copyOf(changed);
        unchanged = Set.copyOf(unchanged);
    }

    public boolean hasChanges() {
        return !added.isEmpty() || !removed.isEmpty() || !changed.isEmpty();
    }

    public static GraphDiff compute(Graph prev, Graph next) {
        Objects.requireNonNull(prev, "prev");
        Objects.requireNonNull(next, "next");
        Set<String> prevNames = prev.nodes().keySet();
        Set<String> nextNames = next.nodes().keySet();

        Set<String> added = new HashSet<>(nextNames);
        added.removeAll(prevNames);

        Set<String> removed = new HashSet<>(prevNames);
        removed.removeAll(nextNames);

        Set<String> changed = new HashSet<>();
        Set<String> unchanged = new HashSet<>();
        for (String name : nextNames) {
            if (!prevNames.contains(name)) continue;
            ProcessNode prevNode = prev.nodes().get(name);
            ProcessNode nextNode = next.nodes().get(name);
            if (areEquivalent(prevNode, nextNode)) {
                unchanged.add(name);
            } else {
                changed.add(name);
            }
        }
        return new GraphDiff(added, removed, changed, unchanged);
    }

    /** Structural equivalence per TZ §6.4: name + deps (as set, with kind) + param. */
    public static boolean areEquivalent(ProcessNode a, ProcessNode b) {
        if (!a.name().equals(b.name())) return false;
        if (!Objects.equals(a.param(), b.param())) return false;
        Set<DepKey> aDeps = a.dependencies().stream()
                .map(d -> new DepKey(d.name(), d.getClass()))
                .collect(Collectors.toSet());
        Set<DepKey> bDeps = b.dependencies().stream()
                .map(d -> new DepKey(d.name(), d.getClass()))
                .collect(Collectors.toSet());
        return aDeps.equals(bDeps);
    }

    /** Map.Entry-like dependency key used by {@link #areEquivalent}. */
    private record DepKey(String name, Class<? extends Dependency> kind) { }
}
