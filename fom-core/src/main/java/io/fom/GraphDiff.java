package io.fom;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Which nodes a graph swap adds, removes, changes or leaves alone.
 *
 * <p>A node is unchanged when its name, {@code param} ({@code equals}) and set of
 * dependencies (name and kind, in any order) match. The factories don't count: every
 * build makes new lambdas, even for the same node.</p>
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
            if (equivalent(prev.nodes().get(name), next.nodes().get(name))) {
                unchanged.add(name);
            } else {
                changed.add(name);
            }
        }
        return new GraphDiff(added, removed, changed, unchanged);
    }

    /** Same name, same {@code param} and the same dependencies in any order. */
    public static boolean equivalent(ProcessNode a, ProcessNode b) {
        return a.name().equals(b.name())
                && Objects.equals(a.param(), b.param())
                && dependencyKeys(a).equals(dependencyKeys(b));
    }

    private static Set<DependencyKey> dependencyKeys(ProcessNode node) {
        return node.dependencies().stream()
                .map(d -> new DependencyKey(d.name(), d.getClass()))
                .collect(Collectors.toSet());
    }

    private record DependencyKey(String name, Class<? extends Dependency> kind) { }
}
