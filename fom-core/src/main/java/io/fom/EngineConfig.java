package io.fom;

import java.time.Duration;
import java.util.Objects;

/**
 * Compile-time configuration of an {@link Engine}. Pure {@code record} —
 * may be built programmatically without HOCON. The optional
 * {@code fom-config-hocon} module provides a parser that returns the same
 * shape.
 *
 * <p>All durations here are <em>defaults</em>; the Engine resolves per-process
 * overrides against this baseline.</p>
 */
public record EngineConfig(Duration defaultInitTimeout,
                           Duration defaultLoadTimeout,
                           Duration defaultCleanupTimeout,
                           Duration defaultComputeTimeout,
                           Duration defaultCancelInitTimeout,
                           Duration queryTimeout,
                           Duration logLookupInterval,
                           Duration dedupWindow,
                           Duration backoffMin,
                           Duration backoffMax,
                           int maxLoadRetries,
                           SnapshotPolicy snapshotPolicy) {

    public EngineConfig {
        requirePositive(defaultInitTimeout, "defaultInitTimeout");
        requirePositive(defaultLoadTimeout, "defaultLoadTimeout");
        requirePositive(defaultCleanupTimeout, "defaultCleanupTimeout");
        requirePositive(defaultComputeTimeout, "defaultComputeTimeout");
        requirePositive(defaultCancelInitTimeout, "defaultCancelInitTimeout");
        requirePositive(queryTimeout, "queryTimeout");
        requirePositive(logLookupInterval, "logLookupInterval");
        requirePositive(dedupWindow, "dedupWindow");
        requirePositive(backoffMin, "backoffMin");
        requirePositive(backoffMax, "backoffMax");
        if (backoffMax.compareTo(backoffMin) < 0) {
            throw new IllegalArgumentException("backoffMax must be >= backoffMin");
        }
        if (maxLoadRetries < 1) {
            throw new IllegalArgumentException("maxLoadRetries must be >= 1, was " + maxLoadRetries);
        }
        Objects.requireNonNull(snapshotPolicy, "snapshotPolicy");
    }

    /** Defaults. */
    public static EngineConfig defaults() {
        return new EngineConfig(
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofMinutes(60),
                Duration.ofSeconds(60),
                Duration.ofSeconds(10),
                Duration.ofMillis(100),
                Duration.ofMillis(100),
                Duration.ofMillis(50),
                Duration.ofMinutes(5),
                1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    /** Builder-style copy with a new snapshot policy. */
    public EngineConfig withSnapshotPolicy(SnapshotPolicy policy) {
        return new EngineConfig(
                defaultInitTimeout, defaultLoadTimeout, defaultCleanupTimeout,
                defaultComputeTimeout, defaultCancelInitTimeout, queryTimeout,
                logLookupInterval, dedupWindow, backoffMin, backoffMax, maxLoadRetries, policy);
    }

    private static void requirePositive(Duration d, String name) {
        Objects.requireNonNull(d, name);
        if (d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException(name + " must be > 0, was " + d);
        }
    }
}
