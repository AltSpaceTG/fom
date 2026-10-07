package io.fom;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration of an {@link Engine}; {@code fom-config-hocon} reads the same shape
 * from a file. Every value applies to all processes of the engine.
 *
 * @param initTimeout           total budget for a process's {@code init}, across all retries
 * @param loadTimeout           budget for one {@code load} attempt
 * @param cleanupTimeout        budget for draining in-flight queries plus {@code cleanUp}
 * @param queryTimeout          deadline of a query unless the caller passes one
 * @param dedupWindow           re-init requests for one process within this window collapse into one
 * @param backoffMin            smallest delay between init/load retries
 * @param backoffMax            largest delay between init/load retries
 * @param maxLoadRetries        failed {@code load} attempts before falling back to {@code init}
 * @param snapshotPolicy        when the log is compacted
 * @param reinitRetryBackoffMin first delay before a failed re-init is tried again (the old version
 *                              keeps serving meanwhile); {@code null} means {@code max(initTimeout, 30s)}
 *                              (at most an explicit max), worked out on every read so it follows
 *                              {@link #withInitTimeout}; {@link Duration#ZERO} turns automatic retries off
 * @param reinitRetryBackoffMax largest delay between automatic re-init retries; {@code null} means
 *                              {@code max(10min, reinitRetryBackoffMin)}; an explicit one must be > 0
 * @param reinitStrategy        whether the old version serves during a re-init; {@code null} means
 *                              {@link ReinitStrategy#KEEP_OLD}. A node can override it in the graph
 */
public record EngineConfig(Duration initTimeout,
                           Duration loadTimeout,
                           Duration cleanupTimeout,
                           Duration queryTimeout,
                           Duration dedupWindow,
                           Duration backoffMin,
                           Duration backoffMax,
                           int maxLoadRetries,
                           SnapshotPolicy snapshotPolicy,
                           Duration reinitRetryBackoffMin,
                           Duration reinitRetryBackoffMax,
                           ReinitStrategy reinitStrategy) {

    private static final Duration REINIT_RETRY_FLOOR = Duration.ofSeconds(30);
    private static final Duration REINIT_RETRY_CEILING = Duration.ofMinutes(10);

    public EngineConfig {
        requirePositive(initTimeout, "initTimeout");
        requirePositive(loadTimeout, "loadTimeout");
        requirePositive(cleanupTimeout, "cleanupTimeout");
        requirePositive(queryTimeout, "queryTimeout");
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
        // A null retry bound stays null: it is derived on every read, so it follows a later withInitTimeout.
        Duration min = reinitRetryBackoffMin != null ? reinitRetryBackoffMin
                : derivedReinitRetryMin(initTimeout, reinitRetryBackoffMax);
        if (min.isNegative()) {
            throw new IllegalArgumentException("reinitRetryBackoffMin must be >= 0, was " + min);
        }
        if (reinitRetryBackoffMax != null) requirePositive(reinitRetryBackoffMax, "reinitRetryBackoffMax");
        if (reinitRetryBackoffMax != null && reinitRetryBackoffMax.compareTo(min) < 0) {
            throw new IllegalArgumentException("reinitRetryBackoffMax must be >= reinitRetryBackoffMin");
        }
        if (reinitStrategy == null) reinitStrategy = ReinitStrategy.KEEP_OLD;
    }

    /** The nine-value form: re-init retries and strategy take their defaults. */
    public EngineConfig(Duration initTimeout, Duration loadTimeout, Duration cleanupTimeout, Duration queryTimeout,
                        Duration dedupWindow, Duration backoffMin, Duration backoffMax, int maxLoadRetries,
                        SnapshotPolicy snapshotPolicy) {
        this(initTimeout, loadTimeout, cleanupTimeout, queryTimeout, dedupWindow, backoffMin, backoffMax,
                maxLoadRetries, snapshotPolicy, null, null, null);
    }

    /** {@code max(initTimeout, 30s)}, but never above an explicit {@code max}. */
    private static Duration derivedReinitRetryMin(Duration initTimeout, Duration max) {
        Duration min = initTimeout != null && initTimeout.compareTo(REINIT_RETRY_FLOOR) > 0
                ? initTimeout : REINIT_RETRY_FLOOR;
        return max != null && max.compareTo(min) < 0 ? max : min;
    }

    /** First delay before a failed re-init is retried: as given, or {@code max(initTimeout, 30s)} capped by an explicit max. */
    @Override
    public Duration reinitRetryBackoffMin() {
        return reinitRetryBackoffMin != null ? reinitRetryBackoffMin
                : derivedReinitRetryMin(initTimeout, reinitRetryBackoffMax);
    }

    /** Largest delay between re-init retries: as given, or {@code max(10min, reinitRetryBackoffMin())}. */
    @Override
    public Duration reinitRetryBackoffMax() {
        if (reinitRetryBackoffMax != null) return reinitRetryBackoffMax;
        Duration min = reinitRetryBackoffMin();
        return min.compareTo(REINIT_RETRY_CEILING) > 0 ? min : REINIT_RETRY_CEILING;
    }

    /** Whether a failed re-init is retried on its own; see {@link #reinitRetryBackoffMin}. */
    public boolean reinitRetryEnabled() {
        return !reinitRetryBackoffMin().isZero();
    }

    /**
     * 30s init, load and cleanup budgets, 10s queries, 100ms dedup window, 50ms–5min
     * backoff, init after one failed load, no automatic snapshots; a re-init keeps the old version
     * serving and a failed one is retried after 30s, doubling up to 10min.
     */
    public static EngineConfig defaults() {
        return new EngineConfig(
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Duration.ofMillis(100),
                Duration.ofMillis(50),
                Duration.ofMinutes(5),
                1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    /** A copy with another snapshot policy. */
    public EngineConfig withSnapshotPolicy(SnapshotPolicy policy) {
        return new EngineConfig(
                initTimeout, loadTimeout, cleanupTimeout,
                queryTimeout, dedupWindow, backoffMin, backoffMax, maxLoadRetries, policy,
                reinitRetryBackoffMin, reinitRetryBackoffMax, reinitStrategy);
    }

    /** A copy with another init budget. Re-init retry bounds that were not given follow it. */
    public EngineConfig withInitTimeout(Duration timeout) {
        return new EngineConfig(timeout, loadTimeout, cleanupTimeout,
                queryTimeout, dedupWindow, backoffMin, backoffMax, maxLoadRetries, snapshotPolicy,
                reinitRetryBackoffMin, reinitRetryBackoffMax, reinitStrategy);
    }

    /** A copy with another load budget. */
    public EngineConfig withLoadTimeout(Duration timeout) {
        return new EngineConfig(initTimeout, timeout, cleanupTimeout,
                queryTimeout, dedupWindow, backoffMin, backoffMax, maxLoadRetries, snapshotPolicy,
                reinitRetryBackoffMin, reinitRetryBackoffMax, reinitStrategy);
    }

    /** A copy with another cleanup budget. */
    public EngineConfig withCleanupTimeout(Duration timeout) {
        return new EngineConfig(initTimeout, loadTimeout, timeout,
                queryTimeout, dedupWindow, backoffMin, backoffMax, maxLoadRetries, snapshotPolicy,
                reinitRetryBackoffMin, reinitRetryBackoffMax, reinitStrategy);
    }

    /** A copy with another default query timeout. */
    public EngineConfig withQueryTimeout(Duration timeout) {
        return new EngineConfig(initTimeout, loadTimeout, cleanupTimeout,
                timeout, dedupWindow, backoffMin, backoffMax, maxLoadRetries, snapshotPolicy,
                reinitRetryBackoffMin, reinitRetryBackoffMax, reinitStrategy);
    }

    /** A copy with another dedup window. */
    public EngineConfig withDedupWindow(Duration window) {
        return new EngineConfig(initTimeout, loadTimeout, cleanupTimeout,
                queryTimeout, window, backoffMin, backoffMax, maxLoadRetries, snapshotPolicy,
                reinitRetryBackoffMin, reinitRetryBackoffMax, reinitStrategy);
    }

    /** A copy with other retry backoff bounds. */
    public EngineConfig withBackoff(Duration min, Duration max) {
        return new EngineConfig(initTimeout, loadTimeout, cleanupTimeout,
                queryTimeout, dedupWindow, min, max, maxLoadRetries, snapshotPolicy,
                reinitRetryBackoffMin, reinitRetryBackoffMax, reinitStrategy);
    }

    /** A copy with another load-retry count. */
    public EngineConfig withMaxLoadRetries(int retries) {
        return new EngineConfig(initTimeout, loadTimeout, cleanupTimeout,
                queryTimeout, dedupWindow, backoffMin, backoffMax, retries, snapshotPolicy,
                reinitRetryBackoffMin, reinitRetryBackoffMax, reinitStrategy);
    }

    /**
     * A copy with other bounds for retrying a failed re-init: doubling from {@code min} up to
     * {@code max}, with jitter. {@code min} of {@link Duration#ZERO} turns automatic retries off;
     * {@code null} leaves a bound derived, as in the canonical constructor.
     */
    public EngineConfig withReinitRetryBackoff(Duration min, Duration max) {
        return new EngineConfig(initTimeout, loadTimeout, cleanupTimeout,
                queryTimeout, dedupWindow, backoffMin, backoffMax, maxLoadRetries, snapshotPolicy,
                min, max, reinitStrategy);
    }

    /** A copy with another re-init strategy. */
    public EngineConfig withReinitStrategy(ReinitStrategy strategy) {
        Objects.requireNonNull(strategy, "strategy");
        return new EngineConfig(initTimeout, loadTimeout, cleanupTimeout,
                queryTimeout, dedupWindow, backoffMin, backoffMax, maxLoadRetries, snapshotPolicy,
                reinitRetryBackoffMin, reinitRetryBackoffMax, strategy);
    }

    private static void requirePositive(Duration d, String name) {
        Objects.requireNonNull(d, name);
        if (d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException(name + " must be > 0, was " + d);
        }
    }
}
