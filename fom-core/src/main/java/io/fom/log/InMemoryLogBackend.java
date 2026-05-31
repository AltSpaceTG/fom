package io.fom.log;

import io.fom.SnapshotResult;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory append-only log for tests and quickstart examples.
 * Backed by a {@link CopyOnWriteArrayList}; writes serialise on
 * {@link #appendLock}, reads are lock-free.
 */
public final class InMemoryLogBackend implements LogBackend {

    private final String logId;
    private final CopyOnWriteArrayList<LogEvent> events = new CopyOnWriteArrayList<>();
    private final ReentrantLock appendLock = new ReentrantLock();
    private volatile boolean closed = false;

    /** Cached identifier of the most recently seen {@link LogLeader}. {@code null} until first leader. */
    private volatile String currentLeader = null;

    public InMemoryLogBackend() {
        this("in-memory-" + UUID.randomUUID());
    }

    public InMemoryLogBackend(String logId) {
        this.logId = Objects.requireNonNull(logId, "logId");
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
    public LogEvent get(int clock) {
        return events.get(clock);
    }

    @Override
    public LogEvent[] getBetween(int fromClock, int toClock) {
        if (fromClock < 0 || toClock < fromClock || toClock > events.size()) {
            throw new IndexOutOfBoundsException(
                    "Invalid range [" + fromClock + ", " + toClock + ") for length " + events.size());
        }
        List<LogEvent> slice = events.subList(fromClock, toClock);
        return slice.toArray(LogEvent[]::new);
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

            if (!isLeaderClaim) {
                String leader = currentLeader;
                if (leader == null || !leader.equals(leaderInstanceId)) {
                    return Optional.empty();
                }
            }

            int clock = events.size();
            LogEvent persisted = ClockRewriter.withClock(event, clock);
            events.add(persisted);
            if (persisted instanceof LogLeader claimed) {
                currentLeader = claimed.instanceId();
            }
            return Optional.of(persisted);
        } finally {
            appendLock.unlock();
        }
    }

    @Override
    public LogBackendReport introspect() {
        int len = events.size();
        Map<String, Integer> counts = new HashMap<>();
        long lastTs = 0L;
        for (int i = 0; i < len; i++) {
            LogEvent e = events.get(i);
            counts.merge(e.getClass().getSimpleName(), 1, Integer::sum);
            if (e.timestamp() > lastTs) {
                lastTs = e.timestamp();
            }
        }
        return new LogBackendReport(logId, len, currentLeader,
                new LinkedHashMap<>(counts), lastTs);
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
        appendLock.lock();
        try {
            ensureOpen();
            String archivedId = "in-memory-archived-" + System.currentTimeMillis();
            int oldLen = events.size();
            events.clear();
            currentLeader = null;
            int copied = 0;
            int checkpointClock = -1;
            for (LogEvent e : snapshotEvents) {
                int clock = events.size();
                LogEvent persisted = ClockRewriter.withClock(e, clock);
                events.add(persisted);
                copied++;
                if (persisted instanceof LogLeader claimed) {
                    currentLeader = claimed.instanceId();
                }
                if (persisted instanceof LogSnapshot snap) {
                    checkpointClock = snap.checkpointClock();
                }
            }
            return new SnapshotResult(logId, archivedId,
                    checkpointClock < 0 ? Math.max(oldLen - 1, 0) : checkpointClock,
                    copied);
        } finally {
            appendLock.unlock();
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
