package io.fom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Snapshots once the log has <em>grown</em> by {@code eventCountThreshold} events
 * since this policy's last snapshot (or since activation), checked every
 * {@code pollInterval}.
 *
 * <p>It counts growth, not length, because a compacted log is never empty: a
 * length threshold at or below that floor would snapshot on every poll. If someone
 * else compacts the log, the baseline drops to the new length.</p>
 *
 * <p>Archives are kept unless {@code keepHistory} is finite. Combine with
 * {@link SnapshotPolicy.FixedInterval} in a {@link CompositeSnapshotPolicy} to
 * snapshot on size or time, whichever comes first.</p>
 */
public final class SizeBasedSnapshotPolicy implements SnapshotPolicy {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(SizeBasedSnapshotPolicy.class);

    private final int eventCountThreshold;
    private final Duration pollInterval;
    private final int keepHistory;

    /**
     * @param eventCountThreshold number of events appended since the last snapshot
     *                            that triggers the next one; must be {@code > 0}
     * @param pollInterval        how often the log length is checked; must be positive
     * @param keepHistory         archives retained after each snapshot: {@link SnapshotPolicy#KEEP_ALL}
     *                            (no purge at all), or {@code >= 1}
     */
    public SizeBasedSnapshotPolicy(int eventCountThreshold, Duration pollInterval, int keepHistory) {
        if (eventCountThreshold <= 0) {
            throw new IllegalArgumentException("eventCountThreshold must be > 0, was " + eventCountThreshold);
        }
        Objects.requireNonNull(pollInterval, "pollInterval");
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be > 0, was " + pollInterval);
        }
        if (keepHistory < 1) {
            throw new IllegalArgumentException("keepHistory must be >= 1 (or SnapshotPolicy.KEEP_ALL), was "
                    + keepHistory);
        }
        this.eventCountThreshold = eventCountThreshold;
        this.pollInterval = pollInterval;
        this.keepHistory = keepHistory;
    }

    /**
     * Same as {@link #SizeBasedSnapshotPolicy(int, Duration, int)} with
     * {@link SnapshotPolicy#KEEP_ALL}: archives are never deleted by this policy.
     */
    public SizeBasedSnapshotPolicy(int eventCountThreshold, Duration pollInterval) {
        this(eventCountThreshold, pollInterval, KEEP_ALL);
    }

    public int eventCountThreshold() { return eventCountThreshold; }
    public Duration pollInterval() { return pollInterval; }
    public int keepHistory() { return keepHistory; }

    @Override
    public AutoCloseable activate(SnapshotContext context) {
        Objects.requireNonNull(context, "context");
        Poller poller = new Poller(context);
        long period = Math.max(1L, pollInterval.toMillis());
        ScheduledFuture<?> task = context.scheduler().scheduleAtFixedRate(
                poller::pollOffScheduler, period, period, TimeUnit.MILLISECONDS);
        return () -> {
            poller.closed.set(true);
            task.cancel(false);
        };
    }

    /** Per-activation mutable state; the policy object itself stays immutable. */
    private final class Poller {
        private final SnapshotContext context;
        private final AtomicBoolean inFlight = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean polling = new AtomicBoolean();
        /** Log length right after the last snapshot (0 before the first). */
        private volatile int baseline;

        Poller(SnapshotContext context) {
            this.context = context;
        }

        /**
         * Reading the length can block (a file log holds its lock through a whole compaction),
         * and the scheduler is the engine's only timer thread, so the poll runs on its own thread.
         */
        void pollOffScheduler() {
            if (closed.get() || inFlight.get() || !polling.compareAndSet(false, true)) return;
            Thread.ofVirtual().name("fom-size-policy").start(() -> {
                try {
                    poll();
                } finally {
                    polling.set(false);
                }
            });
        }

        void poll() {
            if (closed.get() || inFlight.get()) return;
            try {
                int length = context.logBackend().length();
                if (length < baseline) {
                    baseline = length; // compacted by someone else
                }
                if (length - baseline < eventCountThreshold) return;
                if (!inFlight.compareAndSet(false, true)) return;
                try {
                    context.snapshot().whenComplete((res, err) -> {
                        try {
                            if (err != null) {
                                log.warn("SizeBasedSnapshotPolicy snapshot failed: {}", err.toString());
                            } else {
                                // The compacted length, not the length now: appends since then are growth.
                                baseline = res.eventsCopied();
                                if (!closed.get() && keepHistory != KEEP_ALL) context.purgeArchives(keepHistory);
                            }
                        } catch (Throwable t) {
                            log.warn("SizeBasedSnapshotPolicy post-snapshot step failed: {}", t.toString());
                        } finally {
                            inFlight.set(false);
                        }
                    });
                } catch (Throwable t) {
                    inFlight.set(false);
                    throw t;
                }
            } catch (Throwable t) {
                log.warn("SizeBasedSnapshotPolicy poll failed: {}", t.toString());
            }
        }
    }
}
