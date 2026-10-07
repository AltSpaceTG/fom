package io.fom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Several {@link SnapshotPolicy}s at once: each child runs on its own, and whichever
 * fires first takes the snapshot. A size-based child sees the shorter log on its next
 * poll and resets its baseline.
 *
 * <p>A {@link SnapshotPolicy.FixedInterval} child is scheduled here (a tick is skipped
 * while the previous snapshot still runs), {@link SnapshotPolicy.Disabled} is ignored,
 * and anything else, nested composites included, is {@link SnapshotPolicy#activate activated}.</p>
 */
public final class CompositeSnapshotPolicy implements SnapshotPolicy {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(CompositeSnapshotPolicy.class);

    private final List<SnapshotPolicy> policies;

    public CompositeSnapshotPolicy(List<SnapshotPolicy> policies) {
        Objects.requireNonNull(policies, "policies");
        if (policies.isEmpty()) {
            throw new IllegalArgumentException("CompositeSnapshotPolicy needs at least one child policy");
        }
        this.policies = List.copyOf(policies);
    }

    public CompositeSnapshotPolicy(SnapshotPolicy... policies) {
        this(List.of(Objects.requireNonNull(policies, "policies")));
    }

    public List<SnapshotPolicy> policies() {
        return policies;
    }

    @Override
    public AutoCloseable activate(SnapshotContext context) {
        Objects.requireNonNull(context, "context");
        List<AutoCloseable> handles = new ArrayList<>(policies.size());
        try {
            for (SnapshotPolicy p : policies) {
                AutoCloseable h = activateChild(p, context);
                if (h != null) handles.add(h);
            }
        } catch (RuntimeException e) {
            closeAll(handles);
            throw e;
        }
        return () -> closeAll(handles);
    }

    private static AutoCloseable activateChild(SnapshotPolicy p, SnapshotContext context) {
        if (p instanceof SnapshotPolicy.Disabled) return null;
        if (p instanceof SnapshotPolicy.FixedInterval fi) return scheduleFixedInterval(fi, context);
        return p.activate(context);
    }

    private static AutoCloseable scheduleFixedInterval(SnapshotPolicy.FixedInterval fi, SnapshotContext context) {
        long periodMs = Math.max(1L, fi.interval().toMillis());
        AtomicBoolean inFlight = new AtomicBoolean();
        AtomicBoolean closed = new AtomicBoolean();
        ScheduledFuture<?> task = context.scheduler().scheduleAtFixedRate(() -> {
            if (closed.get() || !inFlight.compareAndSet(false, true)) return;
            try {
                context.snapshot().whenComplete((res, err) -> {
                    try {
                        if (err != null) {
                            log.warn("CompositeSnapshotPolicy fixed-interval snapshot failed: {}", err.toString());
                        } else if (!closed.get() && fi.purgesArchives()) {
                            context.purgeArchives(fi.keepHistory());
                        }
                    } catch (Throwable t) {
                        log.warn("CompositeSnapshotPolicy archive purge failed: {}", t.toString());
                    } finally {
                        inFlight.set(false);
                    }
                });
            } catch (Throwable t) {
                inFlight.set(false);
                log.warn("CompositeSnapshotPolicy fixed-interval tick failed: {}", t.toString());
            }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
        return () -> {
            closed.set(true);
            task.cancel(false);
        };
    }

    private static void closeAll(List<AutoCloseable> handles) {
        for (AutoCloseable h : handles) {
            try { h.close(); } catch (Exception ignored) { }
        }
    }
}
