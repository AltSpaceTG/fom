package io.fom.api;

import io.fom.Sid;

import java.time.Duration;
import java.util.UUID;

/**
 * Observability SPI — receives lifecycle and query events from the runtime.
 * All callbacks have no-op defaults; implementers override only what they care
 * about. Implementations must be fast and non-blocking — they execute on the
 * FSM dispatcher and on worker virtual threads.
 *
 * <p>Adapters: {@code MicrometerEngineObserver} (in {@code fom-micrometer})
 * records counters/timers/gauges per §6.8; {@code OtelEngineObserver} (in
 * {@code fom-otel}) starts spans for tracing.</p>
 */
public interface EngineObserver {

    /** Called whenever a {@link io.fom.fsm.ProcessFSM} transitions between states. */
    default void onStateTransition(String processName, String fromState, String toState) { }

    default void onInitStarted(String processName, int attempt) { }

    default void onInitCompleted(String processName, Sid newSid, Duration duration) { }

    default void onInitFailed(String processName, int attempt, Throwable cause) { }

    default void onLoadStarted(String processName, Sid sid, int attempt) { }

    default void onLoadCompleted(String processName, Sid sid, Duration duration) { }

    default void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) { }

    /** A new top-level query is being dispatched to a process. */
    default void onQuerySent(String processName, UUID queryId, Class<?> messageType) { }

    default void onQueryCompleted(String processName, UUID queryId, Duration duration) { }

    /**
     * Query failed. {@code reason} is one of: {@code "cancelled"},
     * {@code "init-in-progress"}, {@code "rejected"}, {@code "timeout"},
     * or {@code "exception"}.
     */
    default void onQueryFailed(String processName, UUID queryId, String reason, Throwable cause) { }

    /** Compute durations rolled up — fires after the worker returns. */
    default void onComputeDuration(String processName, Duration duration) { }

    /**
     * The dedup window collapsed {@code collapsedCount} re-init causes into
     * a single firing for {@code processName}.
     */
    default void onDedupCollapsed(String processName, int collapsedCount) { }

    /** Cleanup callback finished — {@code ok=false} when it threw or timed out. */
    default void onCleanupCompleted(String processName, Sid sid, boolean ok, Duration duration) { }

    /**
     * A new Sid was promoted to {@code Serving}. {@code previousSid} is
     * {@code null} on first cold start; non-null on every subsequent re-init.
     */
    default void onSidPromotion(String processName, Sid previousSid, Sid newSid) { }

    /**
     * Init has been running for a notable amount of time with zero
     * outstanding waiters. Useful for detecting orphaned long inits per
     * TZ §8.3.2.
     */
    default void onOrphanedInit(String processName, Sid placeholderSid, Duration elapsed) { }

    /** No-op observer — engine default. */
    EngineObserver NOOP = new EngineObserver() { };
}
