package io.fom.log;

import io.fom.SnapshotResult;
import io.fom.api.LeadershipLostException;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory log for tests and examples. Writes serialise on a lock; reads are
 * lock-free and see either the old or the new contents of a compaction, never a mix.
 */
public final class InMemoryLogBackend implements LogBackend {

    private final String logId;
    /**
     * The first {@code size} slots of a growing array. An append fills the slot past every
     * published view and publishes a new view, so readers need neither a lock nor a full copy.
     */
    private record View(LogEvent[] items, int size) {
        LogEvent last() {
            return size == 0 ? null : items[size - 1];
        }
    }

    private volatile View events = new View(new LogEvent[16], 0);
    private final ReentrantLock appendLock = new ReentrantLock();
    private volatile boolean closed = false;

    /** Instance id of the latest {@link LogLeader}, or {@code null} before the first. */
    private volatile String currentLeader = null;

    // Guarded by appendLock.
    private final Map<String, Integer> eventCounts = new LinkedHashMap<>();
    private long maxTimestampMillis = 0L;

    /** The {@link #introspect()} answer, rebuilt whenever the log changes. */
    private volatile LogBackendReport report;

    public InMemoryLogBackend() {
        this("in-memory-" + UUID.randomUUID());
    }

    public InMemoryLogBackend(String logId) {
        this.logId = Objects.requireNonNull(logId, "logId");
        refreshReport();
    }

    @Override
    public String logId() {
        return logId;
    }

    @Override
    public int length() {
        return events.size();
    }

    @Override
    public LogEvent get(int position) {
        View view = events;
        Objects.checkIndex(position, view.size());
        return view.items()[position];
    }

    @Override
    public LogEvent[] getBetween(int fromPosition, int toPosition) {
        View view = events;
        if (fromPosition < 0 || toPosition < fromPosition || toPosition > view.size()) {
            throw new IndexOutOfBoundsException(
                    "Invalid range [" + fromPosition + ", " + toPosition + ") for length " + view.size());
        }
        return Arrays.copyOfRange(view.items(), fromPosition, toPosition);
    }

    @Override
    public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(leaderInstanceId, "leaderInstanceId");
        ensureOpen();
        appendLock.lock();
        try {
            ensureOpen();
            boolean isLeaderClaim = event instanceof LogLeader newLeader
                    && newLeader.instanceId().equals(leaderInstanceId);

            if (!isLeaderClaim && !leaderInstanceId.equals(currentLeader)) {
                return Optional.empty();
            }

            View view = events;
            long clock = LogClocks.nextClock(view.last());
            LogEvent persisted = LogClocks.withClock(event, clock);
            LogEvent[] items = view.items();
            if (view.size() == items.length) {
                items = Arrays.copyOf(items, items.length * 2);
            }
            items[view.size()] = persisted; // past every published view: no reader looks there yet
            events = new View(items, view.size() + 1);
            recordEvent(persisted);
            if (persisted instanceof LogLeader claimed) {
                currentLeader = claimed.instanceId();
            }
            refreshReport();
            return Optional.of(persisted);
        } finally {
            appendLock.unlock();
        }
    }

    /** Takes no lock, so frequent introspection cannot stall appends. */
    @Override
    public LogBackendReport introspect() {
        return report;
    }

    /** Call under {@code appendLock}. */
    private void recordEvent(LogEvent event) {
        eventCounts.merge(event.getClass().getSimpleName(), 1, Integer::sum);
        if (event.timestamp() > maxTimestampMillis) {
            maxTimestampMillis = event.timestamp();
        }
    }

    /** Call under {@code appendLock}. */
    private void refreshReport() {
        report = new LogBackendReport(logId, events.size(), currentLeader,
                eventCounts, maxTimestampMillis);
    }

    @Override
    public SnapshotResult compact(List<LogEvent> snapshotEvents, String leaderInstanceId) {
        Objects.requireNonNull(snapshotEvents, "snapshotEvents");
        Objects.requireNonNull(leaderInstanceId, "leaderInstanceId");
        if (snapshotEvents.isEmpty()
                || !(snapshotEvents.get(0) instanceof LogLeader first)
                || !first.instanceId().equals(leaderInstanceId)) {
            throw new IllegalArgumentException(
                    "snapshotEvents must start with LogLeader(" + leaderInstanceId + ")");
        }
        ensureOpen();
        // Leadership before clocks: a plan made just before a takeover has stale clocks,
        // and the caller should hear that it lost the lead. Checked again under the lock.
        requireLeader(leaderInstanceId);
        LogClocks.requireIncreasingClocks(snapshotEvents);
        appendLock.lock();
        try {
            ensureOpen();
            requireLeader(leaderInstanceId);
            String archivedId = "in-memory-archived-" + System.currentTimeMillis();
            int oldLength = events.size();
            List<LogEvent> kept = List.copyOf(snapshotEvents);
            LogEvent[] fresh = kept.toArray(new LogEvent[Math.max(16, kept.size() * 2)]);
            String leader = null;
            long checkpointClock = -1;
            eventCounts.clear();
            maxTimestampMillis = 0L;
            for (LogEvent e : kept) {
                if (e instanceof LogLeader claimed) {
                    leader = claimed.instanceId();
                }
                if (e instanceof LogSnapshot snap) {
                    checkpointClock = snap.checkpointClock();
                }
                recordEvent(e);
            }
            events = new View(fresh, kept.size());
            currentLeader = leader;
            refreshReport();
            return new SnapshotResult(logId, archivedId,
                    checkpointClock < 0 ? Math.max(oldLength - 1, 0) : checkpointClock,
                    kept.size());
        } finally {
            appendLock.unlock();
        }
    }

    private void requireLeader(String leaderInstanceId) {
        String leader = currentLeader;
        if (leader != null && !leader.equals(leaderInstanceId)) {
            throw new LeadershipLostException("Cannot compact " + logId + ": instance "
                    + leaderInstanceId + " is no longer the leader (" + leader + " is); nothing was written");
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("InMemoryLogBackend " + logId + " is closed");
        }
    }

}
