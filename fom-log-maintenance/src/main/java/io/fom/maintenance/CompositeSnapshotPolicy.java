package io.fom.maintenance;

import io.fom.SnapshotContext;
import io.fom.SnapshotPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * OR-composition of multiple {@link SnapshotPolicy}s — each child policy
 * receives its own activation and operates independently. The first policy
 * whose trigger fires causes a snapshot; the others see the freshly-compacted
 * log on their next poll and stand down.
 *
 * <p>This wrapper only supports policies that themselves implement
 * {@link SnapshotPolicy#activate(SnapshotContext)} (i.e. user/extension policies).
 * Wrapping built-ins ({@link SnapshotPolicy.FixedInterval}, {@link SnapshotPolicy.Disabled})
 * has no effect because the engine special-cases those.</p>
 */
public final class CompositeSnapshotPolicy implements SnapshotPolicy {

    private final List<SnapshotPolicy> policies;

    public CompositeSnapshotPolicy(List<SnapshotPolicy> policies) {
        Objects.requireNonNull(policies, "policies");
        if (policies.isEmpty()) {
            throw new IllegalArgumentException("CompositeSnapshotPolicy needs at least one child policy");
        }
        this.policies = List.copyOf(policies);
    }

    public List<SnapshotPolicy> policies() {
        return policies;
    }

    @Override
    public AutoCloseable activate(SnapshotContext context) {
        Objects.requireNonNull(context, "context");
        List<AutoCloseable> handles = new ArrayList<>(policies.size());
        for (SnapshotPolicy p : policies) {
            AutoCloseable h = p.activate(context);
            if (h != null) handles.add(h);
        }
        return () -> {
            for (AutoCloseable h : handles) {
                try { h.close(); } catch (Exception ignored) { }
            }
        };
    }
}
