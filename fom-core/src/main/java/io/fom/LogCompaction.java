package io.fom;

import io.fom.log.LogBackend;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogDead;
import io.fom.log.LogDependencyChanged;
import io.fom.log.LogTrigger;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogLoaded;
import io.fom.log.LogPaused;
import io.fom.log.LogResumed;
import io.fom.log.LogSnapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * What a compacted log keeps. Used by {@link Engine#snapshot()} and, without a
 * running engine, by {@link #compact(LogBackend)} (the {@code fom-log compact}
 * command).
 *
 * <p>A compacted log holds, in order:</p>
 * <ol>
 *   <li>a {@link LogLeader} for the current leader;</li>
 *   <li>every still-live {@link LogInitialized} of the processes in the latest
 *       graph, and any not yet loaded replacement of one, each group preceded by the {@link LogChangeGraph} it was written
 *       under — so a restart still compares persisted state against the
 *       definition it was built for. The latest graph comes last, so events
 *       appended after compaction fall under it;</li>
 *   <li>a {@link LogTrigger} naming the processes whose requested re-init has
 *       not happened yet;</li>
 *   <li>a {@link LogPaused} for every paused process;</li>
 *   <li>a {@link LogSnapshot} marker.</li>
 * </ol>
 *
 * <p>Clocks are never renumbered: kept events keep the clock they were written
 * with, so a {@link Sid} names the same state before and after compaction. The
 * new events at the end continue after the highest clock the log ever used, so
 * later appends never reuse a clock either.</p>
 */
public final class LogCompaction {

    private LogCompaction() {
    }

    /** A still-live {@link LogInitialized} and the log position of the graph it was written under ({@code -1} if none). */
    record LiveInit(LogInitialized init, int graphPosition) { }

    /**
     * One pass over a log. {@code liveInits} and {@code paused} (value: stale
     * flag) cover every process name seen; callers filter by their graph.
     * {@code candidates} holds, per process, a written but not yet loaded
     * replacement of its live init ({@link LogInitialized#replaces()}).
     * {@code pausedForDependency} is the subset of {@code paused} the engine
     * paused only because a dependency was paused ({@link LogPaused#forDependency}).
     * {@code latestGraphPosition} is {@code -1} if the log has no graph.
     * {@code pendingReinit} names processes with live state and a re-init
     * request ({@link LogTrigger} or {@link LogDependencyChanged}) newer than
     * both it and its candidate.
     */
    record Scan(Map<String, LiveInit> liveInits,
                Map<String, LiveInit> candidates,
                Map<String, Boolean> paused,
                Set<String> pausedForDependency,
                int latestGraphPosition,
                Set<String> pendingReinit) { }

    /** The events to compact to, with their clocks. */
    record Plan(List<LogEvent> events) {

        Plan {
            events = List.copyOf(events);
        }
    }

    static Scan scan(LogBackend backend) {
        var scanner = new Scanner();
        // Streamed one event at a time where the backend allows it: the heap follows the live state.
        backend.forEachBetween(0, backend.length(), scanner::accept);
        return scanner.result();
    }

    /**
     * Tracks per process the live init (incumbent) and an init written to replace it (candidate).
     * The incumbent stays live until a {@link LogLoaded} of the candidate promotes the candidate.
     */
    private static final class Scanner {
        private final Map<String, LiveInit> incumbents = new HashMap<>();
        private final Map<String, LiveInit> candidates = new HashMap<>();
        private final Map<String, Boolean> paused = new HashMap<>();
        private final Set<String> pausedForDependency = new HashSet<>();
        private final Map<String, Long> latestRequestClock = new HashMap<>();
        private int graphPosition = -1;
        private int position;

        void accept(LogEvent event) {
            int at = position++;
            switch (event) {
                case LogChangeGraph ignored -> graphPosition = at;
                case LogInitialized init -> initialized(new LiveInit(init, graphPosition));
                case LogLoaded loaded -> {
                    String name = loaded.sid().processName();
                    LiveInit candidate = candidates.get(name);
                    if (candidate != null && candidate.init().sid().equals(loaded.sid())) {
                        incumbents.put(name, candidates.remove(name));
                    }
                }
                case LogDead dead -> retire(dead.sid());
                case LogPaused p -> {
                    paused.put(p.processName(), p.stale());
                    if (p.forDependency()) pausedForDependency.add(p.processName());
                    else pausedForDependency.remove(p.processName());
                }
                case LogResumed r -> {
                    paused.remove(r.processName());
                    pausedForDependency.remove(r.processName());
                }
                case LogTrigger t -> {
                    for (String name : t.processNames()) latestRequestClock.put(name, t.clock());
                }
                case LogDependencyChanged d -> latestRequestClock.put(d.sid().processName(), d.clock());
                default -> { }
            }
        }

        private void initialized(LiveInit live) {
            String name = live.init().processName();
            LiveInit incumbent = incumbents.get(name);
            Sid replaces = live.init().replaces();
            if (incumbent != null && replaces != null && replaces.equals(incumbent.init().sid())) {
                candidates.put(name, live);
            } else {
                // A cold init, or the version it replaces is already gone: it is the live state now.
                incumbents.put(name, live);
                candidates.remove(name);
            }
        }

        private void retire(Sid sid) {
            String name = sid.processName();
            LiveInit candidate = candidates.get(name);
            if (candidate != null && candidate.init().sid().equals(sid)) {
                candidates.remove(name);
                return;
            }
            // Dropped right away so removed processes don't hold heap until the scan ends.
            LiveInit incumbent = incumbents.get(name);
            if (incumbent != null && incumbent.init().clock() <= sid.clock()) {
                incumbents.remove(name);
                candidates.remove(name);
            }
        }

        Scan result() {
            Set<String> pending = new HashSet<>();
            latestRequestClock.forEach((name, clock) -> {
                LiveInit incumbent = incumbents.get(name);
                if (incumbent == null) return;
                LiveInit candidate = candidates.get(name);
                long newest = candidate == null ? incumbent.init().clock() : candidate.init().clock();
                if (clock > newest) pending.add(name);
            });
            return new Scan(incumbents, candidates, paused, pausedForDependency, graphPosition, pending);
        }
    }

    static Plan plan(LogBackend backend, String leaderInstanceId, long now) {
        List<LogEvent> events = new ArrayList<>();
        int length = backend.length();
        long lastClock = length == 0 ? -1 : backend.get(length - 1).clock();
        // Clock 0 is always the log's first LogLeader, so it precedes every kept event.
        events.add(new LogLeader(0, now, leaderInstanceId));
        long nextClock = lastClock + 1;

        Scan scan = scan(backend);
        if (scan.latestGraphPosition() >= 0) {
            Set<String> inGraph = new HashSet<>();
            for (LogChangeGraph.Node node : ((LogChangeGraph) backend.get(scan.latestGraphPosition())).nodes()) {
                inGraph.add(node.name());
            }

            // Live state grouped by the graph it was written under. Positions order the groups as
            // their clocks do, so the kept clocks still increase; the latest graph comes last.
            Map<Integer, List<LogInitialized>> byGraph = new TreeMap<>();
            byGraph.put(scan.latestGraphPosition(), new ArrayList<>());
            // A candidate is kept beside its incumbent so a restart can still finish the replacement.
            for (var kept : List.of(scan.liveInits(), scan.candidates())) {
                for (LiveInit live : kept.values()) {
                    if (inGraph.contains(live.init().processName())) {
                        byGraph.computeIfAbsent(live.graphPosition(), k -> new ArrayList<>()).add(live.init());
                    }
                }
            }
            for (var group : byGraph.entrySet()) {
                if (group.getKey() == scan.latestGraphPosition()) {
                    events.add(backend.get(group.getKey()));
                } else if (group.getKey() >= 0) {
                    events.add(trimmedTo((LogChangeGraph) backend.get(group.getKey()), group.getValue()));
                }
                group.getValue().sort(Comparator.comparingLong(LogInitialized::clock));
                events.addAll(group.getValue());
            }

            List<String> pending = new ArrayList<>(scan.pendingReinit());
            pending.retainAll(inGraph);
            if (!pending.isEmpty()) {
                pending.sort(Comparator.naturalOrder());
                events.add(new LogTrigger(nextClock++, now, pending));
            }
            for (var entry : new TreeMap<>(scan.paused()).entrySet()) {
                if (inGraph.contains(entry.getKey())) {
                    events.add(new LogPaused(nextClock++, now, entry.getKey(), entry.getValue(),
                            scan.pausedForDependency().contains(entry.getKey())));
                }
            }
        }
        events.add(new LogSnapshot(nextClock, now, Math.max(lastClock, 0)));
        return new Plan(events);
    }

    /**
     * An older graph, cut down to the nodes whose state is kept with it. The other nodes have left
     * the graph; copying them would carry their params (possibly secrets) past every compaction.
     */
    private static LogChangeGraph trimmedTo(LogChangeGraph older, List<LogInitialized> kept) {
        Set<String> names = new HashSet<>();
        for (LogInitialized init : kept) names.add(init.processName());
        var nodes = older.nodes().stream().filter(n -> names.contains(n.name())).toList();
        return nodes.size() == older.nodes().size() ? older
                : new LogChangeGraph(older.clock(), older.timestamp(), older.formatVersion(), nodes);
    }

    /**
     * Compact {@code backend} without a running engine: keep what a snapshot
     * keeps and let the backend archive the rest. The current leader stays the
     * leader. The caller must make sure no engine writes to the log meanwhile
     * ({@code FileLogBackend} enforces that with its file lock).
     *
     * @throws IllegalStateException if the log has no leader: it is empty, was never claimed, or this
     *                               backend was fenced off
     */
    public static SnapshotResult compact(LogBackend backend) {
        Objects.requireNonNull(backend, "backend");
        String leader = backend.introspect().currentLeader();
        if (leader == null) {
            throw new IllegalStateException("Log " + backend.logId() + " has no leader; nothing to compact");
        }
        return backend.compact(plan(backend, leader, System.currentTimeMillis()).events(), leader);
    }
}
