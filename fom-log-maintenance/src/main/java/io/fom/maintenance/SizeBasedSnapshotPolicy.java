package io.fom.maintenance;

import io.fom.SnapshotContext;
import io.fom.SnapshotPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Triggers {@code Engine.snapshot()} whenever the active log accumulates
 * more than {@code eventCountThreshold} events. Polled at {@code pollInterval};
 * the simplest non-invasive way to bound log growth without a per-append hook.
 *
 * <p>Pair with {@link io.fom.SnapshotPolicy.FixedInterval} via
 * {@link CompositeSnapshotPolicy} for "either size OR time" rotation.</p>
 */
public final class SizeBasedSnapshotPolicy implements SnapshotPolicy {

    private static final Logger log = LoggerFactory.getLogger(SizeBasedSnapshotPolicy.class);

    private final int eventCountThreshold;
    private final Duration pollInterval;
    private final int keepHistory;

    public SizeBasedSnapshotPolicy(int eventCountThreshold, Duration pollInterval, int keepHistory) {
        if (eventCountThreshold <= 0) {
            throw new IllegalArgumentException("eventCountThreshold must be > 0, was " + eventCountThreshold);
        }
        Objects.requireNonNull(pollInterval, "pollInterval");
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be > 0, was " + pollInterval);
        }
        if (keepHistory < 1) {
            throw new IllegalArgumentException("keepHistory must be >= 1, was " + keepHistory);
        }
        this.eventCountThreshold = eventCountThreshold;
        this.pollInterval = pollInterval;
        this.keepHistory = keepHistory;
    }

    public int eventCountThreshold() { return eventCountThreshold; }
    public Duration pollInterval() { return pollInterval; }
    public int keepHistory() { return keepHistory; }

    @Override
    public AutoCloseable activate(SnapshotContext context) {
        Objects.requireNonNull(context, "context");
        long period = pollInterval.toMillis();
        ScheduledFuture<?> task = context.scheduler().scheduleAtFixedRate(() -> {
            try {
                int length = context.logBackend().length();
                if (length >= eventCountThreshold) {
                    context.snapshot().toCompletableFuture().get();
                    context.purgeArchives(keepHistory);
                }
            } catch (Throwable t) {
                log.warn("SizeBasedSnapshotPolicy poll failed: {}", t.toString());
            }
        }, period, period, TimeUnit.MILLISECONDS);
        return () -> task.cancel(false);
    }
}
