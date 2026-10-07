package io.fom;

import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogEvent;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Engine-free SnapshotContext with a real single-thread scheduler (like the engine's). */
final class FakeSnapshotContext implements SnapshotContext, AutoCloseable {

    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "fake-fom-scheduler");
        t.setDaemon(true);
        return t;
    });
    final AtomicInteger length = new AtomicInteger();
    final AtomicInteger snapshotCalls = new AtomicInteger();
    final AtomicInteger purgeCalls = new AtomicInteger();
    /** Log length after a snapshot completes. */
    volatile int compactedLength = 4;
    /** When non-null, snapshot() returns this (possibly incomplete) future instead of completing at once. */
    final AtomicReference<CompletableFuture<SnapshotResult>> nextResult = new AtomicReference<>();

    @Override
    public CompletionStage<SnapshotResult> snapshot() {
        snapshotCalls.incrementAndGet();
        CompletableFuture<SnapshotResult> pending = nextResult.getAndSet(null);
        CompletableFuture<SnapshotResult> f = pending != null
                ? pending
                : CompletableFuture.completedFuture(new SnapshotResult("new", "old", 0, 0));
        return f.thenApply(r -> {
            length.set(compactedLength);
            return r;
        });
    }

    @Override
    public ScheduledExecutorService scheduler() {
        return scheduler;
    }

    @Override
    public LogBackend logBackend() {
        return new LogBackend() {
            @Override public String logId() { return "fake"; }
            @Override public int length() { return length.get(); }
            @Override public LogEvent get(int clock) { throw new UnsupportedOperationException(); }
            @Override public LogEvent[] getBetween(int fromClock, int toClock) { throw new UnsupportedOperationException(); }
            @Override public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) { throw new UnsupportedOperationException(); }
            @Override public LogBackendReport introspect() { throw new UnsupportedOperationException(); }
            @Override public SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId) { throw new UnsupportedOperationException(); }
            @Override public void close() { }
        };
    }

    @Override
    public void purgeArchives(int keepHistory) {
        purgeCalls.incrementAndGet();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
