package io.fom.api;

import io.fom.Sid;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Receives lifecycle and query events from the engine. Every callback defaults to a
 * no-op. Callbacks run on the engine's dispatcher and worker threads, so keep them
 * fast and non-blocking.
 *
 * <p>Ready-made: {@code MicrometerEngineObserver} ({@code fom-micrometer}) for metrics,
 * {@code OtelEngineObserver} ({@code fom-otel}) for tracing.</p>
 */
public interface EngineObserver {

    /** A process moved from one lifecycle state to another (e.g. {@code "Loading"} to {@code "Serving"}). */
    default void onStateTransition(String processName, String fromState, String toState) { }

    default void onInitStarted(String processName, int attempt) { }

    default void onInitCompleted(String processName, Sid newSid, Duration duration) { }

    /**
     * An {@code init} attempt failed. Usually follows {@link #onInitStarted}; the exception is a
     * re-init abandoned because the log would not retire the current state, reported as attempt
     * {@code 1} with no start while the process keeps serving its old state.
     */
    default void onInitFailed(String processName, int attempt, Throwable cause) { }

    default void onLoadStarted(String processName, Sid sid, int attempt) { }

    default void onLoadCompleted(String processName, Sid sid, Duration duration) { }

    default void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) { }

    /**
     * A query was sent to a process, by {@code Engine.query}/{@code queryProcess} or by
     * {@code ctx.query}. Runs on the sending thread, so it can capture caller context such as
     * the current span. Exactly one {@link #onQueryCompleted} or {@link #onQueryFailed} follows.
     * A query to an unknown or paused process is refused earlier and fires nothing.
     *
     * <p>{@code parentQueryId} is the query whose {@code compute} sent this one, so the two can
     * be nested; {@code null} for queries from the {@code Engine} and from {@code init}/{@code load}.</p>
     */
    default void onQuerySent(String processName, UUID queryId, Class<?> messageType, UUID parentQueryId) { }

    /**
     * A process left the graph for good ({@code remove} or a graph swap
     * without it). Observers that keep per-process state (e.g. metrics tagged
     * with the process name) can release it here.
     */
    default void onProcessRemoved(String processName) { }

    /**
     * A {@code ScheduledWatcher} gave up and will not tick again while the engine runs on. Not
     * reported when the caller closes it or the engine closes.
     */
    default void onWatcherStopped(String processName, WatcherStopReason reason) { }

    /** The query's reply succeeded; {@code duration} runs from send to reply. */
    default void onQueryCompleted(String processName, UUID queryId, Duration duration) { }

    /**
     * The query's reply failed. {@code reason} is {@code "timeout"} (the caller's
     * deadline passed), {@code "rejected"} (the process is Dead or shutting down),
     * {@code "init-in-progress"}, {@code "cancelled"} or {@code "exception"}
     * ({@code compute} threw); {@code cause} carries the detail.
     */
    default void onQueryFailed(String processName, UUID queryId, String reason, Throwable cause) { }

    /** How long one {@code compute} took; fires after it returns. */
    default void onComputeDuration(String processName, Duration duration) { }

    /**
     * The dedup window collapsed {@code collapsedCount} re-init causes into
     * a single firing for {@code processName}.
     */
    default void onDedupCollapsed(String processName, int collapsedCount) { }

    /** {@code cleanUp} finished; {@code ok} is {@code false} if it threw or timed out. */
    default void onCleanupCompleted(String processName, Sid sid, boolean ok, Duration duration) { }

    /**
     * A new Sid started serving. {@code previousSid} is {@code null} for the process's first Sid
     * in this engine (a cold start, a load after a restart, or after it was removed and re-added).
     */
    default void onSidPromotion(String processName, Sid previousSid, Sid newSid) { }

    /**
     * A re-init started while {@code servingSid} keeps answering queries. The new version's
     * {@code onInit*}/{@code onLoad*} callbacks follow; {@link #onSidPromotion} marks the switch.
     */
    default void onReinitStarted(String processName, Sid servingSid) { }

    /**
     * A re-init gave up and {@code keptSid} keeps serving. Unless the cause is permanent (lost
     * leadership, a refused append, an undeclared dependency, {@code cancelInit}), it is retried
     * after {@code EngineConfig.reinitRetryBackoffMin}.
     */
    default void onReinitFailed(String processName, Sid keptSid, Throwable cause) { }

    /** No-op observer — engine default. */
    EngineObserver NOOP = new EngineObserver() { };

    /**
     * Pass every callback to {@code observers}, in order; one that throws is logged and
     * does not stop the rest.
     *
     * <pre>{@code
     * new Engine(config, backend, serde,
     *         EngineObserver.composite(new MicrometerEngineObserver(registry),
     *                                  new OtelEngineObserver(tracer)));
     * }</pre>
     *
     * @param observers the delegates; none may be {@code null}
     * @return {@link #NOOP} for no observers, the observer itself for exactly
     *         one, otherwise a {@link CompositeEngineObserver}
     */
    static EngineObserver composite(EngineObserver... observers) {
        Objects.requireNonNull(observers, "observers");
        return composite(List.of(observers));
    }

    /** {@link #composite(EngineObserver...)} over a collection. */
    static EngineObserver composite(Collection<? extends EngineObserver> observers) {
        Objects.requireNonNull(observers, "observers");
        List<EngineObserver> delegates = List.copyOf(observers);
        if (delegates.isEmpty()) return NOOP;
        if (delegates.size() == 1) return delegates.get(0);
        return new CompositeEngineObserver(delegates);
    }
}
