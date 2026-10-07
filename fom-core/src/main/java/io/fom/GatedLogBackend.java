package io.fom;

import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogEvent;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The engine's view of the user's {@link LogBackend}. Appends share a read lock; a
 * snapshot holds the write lock from its scan until {@code compact()} returns, so no
 * append can land between the two and be lost. Scans that must not see a compaction
 * halfway run under the read lock ({@link #readStable}).
 */
final class GatedLogBackend implements LogBackend {

    private final LogBackend delegate;
    private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();

    GatedLogBackend(LogBackend delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /** Plan and compact with no append in between. {@code planner} reads the raw delegate. */
    SnapshotResult snapshot(Function<LogBackend, LogCompaction.Plan> planner, String leaderInstanceId) {
        gate.writeLock().lock();
        try {
            LogCompaction.Plan plan = planner.apply(delegate);
            return delegate.compact(plan.events(), leaderInstanceId);
        } finally {
            gate.writeLock().unlock();
        }
    }

    /**
     * Run {@code reader} against a log no compaction can rewrite meanwhile
     * (appends may still add events at the end).
     */
    <T> T readStable(Function<LogBackend, T> reader) {
        gate.readLock().lock();
        try {
            return reader.apply(this);
        } finally {
            gate.readLock().unlock();
        }
    }

    @Override
    public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
        gate.readLock().lock();
        try {
            return delegate.append(event, leaderInstanceId);
        } finally {
            gate.readLock().unlock();
        }
    }

    @Override
    public SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId) {
        // Direct compaction (e.g. a custom SnapshotPolicy): still exclusive with appends.
        gate.writeLock().lock();
        try {
            return delegate.compact(snapshotEvents, leaderInstanceId);
        } finally {
            gate.writeLock().unlock();
        }
    }

    @Override
    public void purgeArchives(int keepHistory) {
        // Never while a compaction is creating an archive, whether or not the backend guards that.
        gate.writeLock().lock();
        try {
            delegate.purgeArchives(keepHistory);
        } finally {
            gate.writeLock().unlock();
        }
    }

    @Override
    public String logId() {
        return delegate.logId();
    }

    @Override
    public int length() {
        return delegate.length();
    }

    @Override
    public LogEvent get(int position) {
        return delegate.get(position);
    }

    @Override
    public LogEvent[] getBetween(int fromPosition, int toPosition) {
        return delegate.getBetween(fromPosition, toPosition);
    }

    @Override
    public void forEachBetween(int fromPosition, int toPosition, Consumer<? super LogEvent> action) {
        delegate.forEachBetween(fromPosition, toPosition, action);
    }

    @Override
    public LogBackendReport introspect() {
        return delegate.introspect();
    }

    /** The engine never owns the user's backend; closing is the caller's job. */
    @Override
    public void close() {
    }
}
