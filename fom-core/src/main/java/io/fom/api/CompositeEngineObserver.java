package io.fom.api;

import io.fom.Sid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Passes every {@link EngineObserver} callback to several observers, in order. One
 * that throws does not stop the rest; its failure is logged, as the engine does for
 * a single observer. Made by {@link EngineObserver#composite(EngineObserver...)}.
 *
 * <p>Every SPI method is forwarded explicitly; a test fails if a new one is missed.</p>
 */
public final class CompositeEngineObserver implements EngineObserver {

    private static final Logger log = LoggerFactory.getLogger(CompositeEngineObserver.class);

    private final List<EngineObserver> delegates;
    private final Map<String, AtomicLong> failures = new ConcurrentHashMap<>();

    CompositeEngineObserver(List<EngineObserver> delegates) {
        this.delegates = List.copyOf(delegates); // rejects nulls
    }

    /** The delegates, in fan-out order. */
    public List<EngineObserver> delegates() {
        return delegates;
    }

    private void fanOut(String name, Consumer<EngineObserver> callback) {
        for (EngineObserver delegate : delegates) {
            try {
                callback.accept(delegate);
            } catch (Throwable t) {
                delegateThrew(name, delegate, t);
            }
        }
    }

    /** WARN on the 1st, 2nd, 4th, 8th… failure of each callback, DEBUG otherwise. */
    private void delegateThrew(String callback, EngineObserver delegate, Throwable t) {
        long n = failures.computeIfAbsent(callback, k -> new AtomicLong()).incrementAndGet();
        if (Long.bitCount(n) == 1) {
            log.warn("CompositeEngineObserver delegate {}.{} threw (failure #{}; "
                            + "repeats are logged at doubling intervals): {}",
                    delegate.getClass().getName(), callback, n, t.toString());
        } else {
            log.debug("CompositeEngineObserver delegate {}.{} threw (failure #{}): {}",
                    delegate.getClass().getName(), callback, n, t.toString());
        }
    }

    @Override
    public void onStateTransition(String processName, String fromState, String toState) {
        fanOut("onStateTransition", o -> o.onStateTransition(processName, fromState, toState));
    }

    @Override
    public void onInitStarted(String processName, int attempt) {
        fanOut("onInitStarted", o -> o.onInitStarted(processName, attempt));
    }

    @Override
    public void onInitCompleted(String processName, Sid newSid, Duration duration) {
        fanOut("onInitCompleted", o -> o.onInitCompleted(processName, newSid, duration));
    }

    @Override
    public void onInitFailed(String processName, int attempt, Throwable cause) {
        fanOut("onInitFailed", o -> o.onInitFailed(processName, attempt, cause));
    }

    @Override
    public void onLoadStarted(String processName, Sid sid, int attempt) {
        fanOut("onLoadStarted", o -> o.onLoadStarted(processName, sid, attempt));
    }

    @Override
    public void onLoadCompleted(String processName, Sid sid, Duration duration) {
        fanOut("onLoadCompleted", o -> o.onLoadCompleted(processName, sid, duration));
    }

    @Override
    public void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) {
        fanOut("onLoadFailed", o -> o.onLoadFailed(processName, sid, attempt, cause));
    }

    @Override
    public void onQuerySent(String processName, UUID queryId, Class<?> messageType, UUID parentQueryId) {
        fanOut("onQuerySent", o -> o.onQuerySent(processName, queryId, messageType, parentQueryId));
    }

    @Override
    public void onProcessRemoved(String processName) {
        fanOut("onProcessRemoved", o -> o.onProcessRemoved(processName));
    }

    @Override
    public void onWatcherStopped(String processName, WatcherStopReason reason) {
        fanOut("onWatcherStopped", o -> o.onWatcherStopped(processName, reason));
    }

    @Override
    public void onQueryCompleted(String processName, UUID queryId, Duration duration) {
        fanOut("onQueryCompleted", o -> o.onQueryCompleted(processName, queryId, duration));
    }

    @Override
    public void onQueryFailed(String processName, UUID queryId, String reason, Throwable cause) {
        fanOut("onQueryFailed", o -> o.onQueryFailed(processName, queryId, reason, cause));
    }

    @Override
    public void onComputeDuration(String processName, Duration duration) {
        fanOut("onComputeDuration", o -> o.onComputeDuration(processName, duration));
    }

    @Override
    public void onDedupCollapsed(String processName, int collapsedCount) {
        fanOut("onDedupCollapsed", o -> o.onDedupCollapsed(processName, collapsedCount));
    }

    @Override
    public void onCleanupCompleted(String processName, Sid sid, boolean ok, Duration duration) {
        fanOut("onCleanupCompleted", o -> o.onCleanupCompleted(processName, sid, ok, duration));
    }

    @Override
    public void onSidPromotion(String processName, Sid previousSid, Sid newSid) {
        fanOut("onSidPromotion", o -> o.onSidPromotion(processName, previousSid, newSid));
    }

    @Override
    public void onReinitStarted(String processName, Sid servingSid) {
        fanOut("onReinitStarted", o -> o.onReinitStarted(processName, servingSid));
    }

    @Override
    public void onReinitFailed(String processName, Sid keptSid, Throwable cause) {
        fanOut("onReinitFailed", o -> o.onReinitFailed(processName, keptSid, cause));
    }

    @Override
    public String toString() {
        return "CompositeEngineObserver" + delegates;
    }
}
