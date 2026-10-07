package io.fom;

import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogEvent;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** InMemoryLogBackend that counts compactions and archive purges. */
final class CountingLogBackend implements LogBackend {

    private final InMemoryLogBackend delegate = new InMemoryLogBackend();
    final AtomicInteger compacts = new AtomicInteger();
    final AtomicInteger purges = new AtomicInteger();

    @Override public String logId() { return delegate.logId(); }
    @Override public int length() { return delegate.length(); }
    @Override public LogEvent get(int clock) { return delegate.get(clock); }
    @Override public LogEvent[] getBetween(int fromClock, int toClock) { return delegate.getBetween(fromClock, toClock); }
    @Override public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) { return delegate.append(event, leaderInstanceId); }
    @Override public LogBackendReport introspect() { return delegate.introspect(); }

    @Override
    public SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId) {
        compacts.incrementAndGet();
        return delegate.compact(snapshotEvents, leaderInstanceId);
    }

    @Override
    public void purgeArchives(int keepHistory) {
        purges.incrementAndGet();
        delegate.purgeArchives(keepHistory);
    }

    @Override public void close() { delegate.close(); }
}
