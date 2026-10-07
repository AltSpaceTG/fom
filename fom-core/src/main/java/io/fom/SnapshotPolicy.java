package io.fom;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * When the engine snapshots (compacts) its log on its own.
 *
 * <p>Built in: {@link Disabled}, {@link FixedInterval}, {@link SizeBasedSnapshotPolicy}
 * and {@link CompositeSnapshotPolicy}. A custom policy schedules itself in
 * {@link #activate(SnapshotContext)}.</p>
 *
 * <p>Each snapshot moves the old log aside as an archive ({@code <log>.archived.<millis>}
 * for a file log, {@code <table>_archived_<millis>} in Postgres). Nothing deletes them
 * unless a policy has a finite {@code keepHistory} or you call
 * {@code Engine.purgeArchives(n)}, so they grow the disk without bound by default.</p>
 */
public interface SnapshotPolicy extends Serializable {

    /**
     * {@code keepHistory} meaning "never delete archives", the default of every built-in
     * policy: no purge runs at all. Any other value must be {@code >= 1}.
     */
    int KEEP_ALL = Integer.MAX_VALUE;

    /**
     * Start this policy. Returns what the engine closes on shutdown; only the built-ins
     * the engine schedules itself ({@link Disabled}, {@link FixedInterval}) return
     * {@code null}. A custom policy must return non-null.
     */
    default AutoCloseable activate(SnapshotContext context) {
        return null;
    }

    /** No automatic rotation. {@code Engine.snapshot()} remains available manually. */
    final class Disabled implements SnapshotPolicy {

        public static final Disabled INSTANCE = new Disabled();

        private Disabled() {
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Disabled;
        }

        @Override
        public int hashCode() {
            return Disabled.class.hashCode();
        }

        @Override
        public String toString() {
            return "Disabled";
        }
    }

    /**
     * Fire {@code Engine.snapshot()} every {@code interval}; after each one keep the
     * newest {@code keepHistory} archives, or all of them with {@link #KEEP_ALL}.
     *
     * @param interval    time between snapshots; must be positive
     * @param keepHistory archives retained after each snapshot: {@link #KEEP_ALL}, or {@code >= 1}
     */
    record FixedInterval(Duration interval, int keepHistory) implements SnapshotPolicy {

        /** Snapshot every {@code interval} and keep every archive ({@link #KEEP_ALL}). */
        public FixedInterval(Duration interval) {
            this(interval, KEEP_ALL);
        }

        public FixedInterval {
            Objects.requireNonNull(interval, "interval");
            if (interval.isNegative() || interval.isZero()) {
                throw new IllegalArgumentException("interval must be > 0, was " + interval);
            }
            if (keepHistory < 1) {
                throw new IllegalArgumentException("keepHistory must be >= 1 (or SnapshotPolicy.KEEP_ALL), was "
                        + keepHistory);
            }
        }

        /** Whether this policy deletes archives after a snapshot ({@code false} for {@link #KEEP_ALL}). */
        public boolean purgesArchives() {
            return keepHistory != KEEP_ALL;
        }
    }
}
