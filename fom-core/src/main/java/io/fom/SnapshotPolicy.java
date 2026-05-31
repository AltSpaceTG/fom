package io.fom;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * How the engine schedules automatic snapshot rotation.
 *
 * <p>Built-in variants ({@link Disabled}, {@link FixedInterval}) live in core.
 * Additional variants such as size-based, composite, or external triggers
 * implement this interface and provide their own activation via
 * {@link #activate(SnapshotContext)} — that's how {@code fom-log-maintenance}
 * extends the policy surface without modifying core.</p>
 */
public interface SnapshotPolicy extends Serializable {

    /**
     * Activate this policy against the engine. Returns an {@link AutoCloseable}
     * the engine cancels on shutdown, or {@code null} if the policy is a
     * built-in handled directly by the engine (currently {@link Disabled} and
     * {@link FixedInterval}).
     *
     * <p>User implementations must return non-null and own their own
     * scheduling — typically a {@link java.util.concurrent.ScheduledFuture}.</p>
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

    /** Fire {@code Engine.snapshot()} every {@code interval}; keep the last {@code keepHistory} archives. */
    record FixedInterval(Duration interval, int keepHistory) implements SnapshotPolicy {

        public FixedInterval {
            Objects.requireNonNull(interval, "interval");
            if (interval.isNegative() || interval.isZero()) {
                throw new IllegalArgumentException("interval must be > 0, was " + interval);
            }
            if (keepHistory < 1) {
                throw new IllegalArgumentException("keepHistory must be >= 1, was " + keepHistory);
            }
        }
    }
}
