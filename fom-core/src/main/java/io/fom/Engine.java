package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.QueryException;
import io.fom.api.Routable;
import io.fom.fsm.GraphMachine;
import io.fom.fsm.ReinitCause;
import io.fom.log.LogBackend;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogDead;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogSnapshot;
import io.fom.log.LogTrigger;
import io.fom.serde.SerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Public facade of the runtime.
 *
 * <p><strong>Stage 2:</strong> multi-process graphs are wired —
 * {@link #newGraph(Graph)} spawns a {@link GraphMachine} that drives every
 * {@link io.fom.fsm.ProcessFSM} through its lifecycle in topological order.
 * {@link #query(Object)} dispatches by {@link Routable} → graph
 * {@code typeRouting} → fail; {@link #queryProcess(String, Object)} bypasses
 * both for explicit addressing.</p>
 *
 * <p>Triggers, watchers, dedup window and introspection arrive in Stages 4–5.</p>
 */
public final class Engine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Engine.class);

    private volatile EngineConfig config;
    private final LogBackend logBackend;
    private final SerDe serDe;
    private final boolean leaderAtStart;
    private final String instanceId;
    private final ScheduledExecutorService scheduler;
    private final EngineObserver observer;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Serialises control-plane mutations: {@code newGraph} + {@code updateConfig}. */
    private final java.util.concurrent.locks.ReentrantLock controlLock =
            new java.util.concurrent.locks.ReentrantLock();

    private volatile Graph graph;
    private volatile GraphMachine graphMachine;
    private volatile ScheduledFuture<?> snapshotTask;
    private volatile AutoCloseable customSnapshotHandle;

    public Engine(EngineConfig config, LogBackend logBackend, SerDe serDe, boolean leaderAtStart) {
        this(config, logBackend, serDe, leaderAtStart, EngineObserver.NOOP);
    }

    public Engine(EngineConfig config,
                  LogBackend logBackend,
                  SerDe serDe,
                  boolean leaderAtStart,
                  EngineObserver observer) {
        this.config = Objects.requireNonNull(config, "config");
        this.logBackend = Objects.requireNonNull(logBackend, "logBackend");
        this.serDe = Objects.requireNonNull(serDe, "serDe");
        this.leaderAtStart = leaderAtStart;
        this.observer = Objects.requireNonNull(observer, "observer");
        this.instanceId = "fom-" + UUID.randomUUID();
        this.scheduler = Executors.newScheduledThreadPool(
                1, r -> {
                    var t = new Thread(r, "fom-scheduler-" + instanceId);
                    t.setDaemon(true);
                    return t;
                });
    }

    public String instanceId() {
        return instanceId;
    }

    public EngineConfig config() {
        return config;
    }

    /**
     * Swap the engine's active {@link EngineConfig}. Future FSM operations
     * (init/load/compute timeouts, dedup window, backoff bounds) pick up the
     * new values; in-flight operations keep their captured values until they
     * return. If {@code snapshotPolicy} changed, the scheduled rotation is
     * cancelled and re-armed against the new policy.
     */
    public void updateConfig(EngineConfig next) {
        Objects.requireNonNull(next, "next");
        ensureOpen();
        controlLock.lock();
        try {
            EngineConfig prev = this.config;
            this.config = next;
            if (!Objects.equals(prev.snapshotPolicy(), next.snapshotPolicy()) && graphMachine != null) {
                cancelScheduledSnapshot();
                scheduleSnapshotRotation();
            }
        } finally {
            controlLock.unlock();
        }
    }

    private void cancelScheduledSnapshot() {
        ScheduledFuture<?> task = snapshotTask;
        if (task != null) task.cancel(false);
        snapshotTask = null;
        AutoCloseable custom = customSnapshotHandle;
        if (custom != null) {
            try { custom.close(); } catch (Exception ignored) { }
        }
        customSnapshotHandle = null;
    }

    public boolean leaderAtStart() {
        return leaderAtStart;
    }

    public LogEvent[] getLog() {
        return logBackend.getBetween(0, logBackend.length());
    }

    /**
     * Install {@code graph}: persist {@link LogLeader} and {@link LogChangeGraph},
     * spawn one {@code ProcessFSM} per node in topological order, and block
     * until every node reaches {@code Serving}.
     *
     * @throws IllegalStateException if called after {@link #close()} or
     *         after a graph has already been installed (in-place graph swap arrives in Stage 5)
     */
    public boolean newGraph(Graph graph) {
        Objects.requireNonNull(graph, "graph");
        ensureOpen();
        if (!leaderAtStart) {
            throw new IllegalStateException(
                    "Stage 2 requires leaderAtStart=true (follower mode arrives in Stage 3)");
        }
        // Serialise concurrent newGraph() calls: without this, two callers could
        // both observe graphMachine==null and each spawn a GraphMachine (leak).
        controlLock.lock();
        try {
            if (this.graphMachine != null) {
                // In-place graph swap — diff against the running graph.
                // Added/changed nodes always cold-init: the old LogInitialized still
                // sitting in the log is stale (the node's definition changed) and
                // must not be warm-loaded. Cross-restart warm-load is the initial
                // newGraph path below.
                persistGraphChange(graph);
                GraphDiff diff = this.graphMachine.applyGraphChange(graph, name -> null);
                this.graph = graph;
                return diff.hasChanges();
            }

            claimLeadership();
            persistGraphChange(graph);

            Map<String, GraphMachine.RestoreSnapshot> snapshots = scanRestoreSnapshots(graph);
            GraphMachine machine = new GraphMachine(
                    graph, () -> this.config, logBackend, instanceId, scheduler, observer, snapshots::get);
            this.graphMachine = machine;
            this.graph = graph;

            machine.startAll();
            scheduleSnapshotRotation();
            return true;
        } finally {
            controlLock.unlock();
        }
    }

    private void scheduleSnapshotRotation() {
        SnapshotPolicy policy = config.snapshotPolicy();
        if (policy instanceof SnapshotPolicy.Disabled) return;
        if (policy instanceof SnapshotPolicy.FixedInterval fi) {
            long periodMs = fi.interval().toMillis();
            snapshotTask = scheduler.scheduleAtFixedRate(() -> {
                // Kick the snapshot off on the common pool and react to its result
                // there — never block the single-thread scheduler with a .get(),
                // which would stall dedup/retry/watcher timers for the snapshot's
                // whole duration.
                snapshot().toCompletableFuture().whenComplete((res, err) -> {
                    if (err != null) {
                        log.warn("Engine[{}] scheduled snapshot failed: {}", instanceId, err.toString());
                    } else if (fi.keepHistory() >= 1) {
                        purgeOldArchives(fi.keepHistory());
                    }
                });
            }, periodMs, periodMs, TimeUnit.MILLISECONDS);
            return;
        }
        // User-supplied policy — delegate scheduling to it.
        AutoCloseable handle = policy.activate(snapshotContext());
        if (handle != null) {
            customSnapshotHandle = handle;
        }
    }

    private SnapshotContext snapshotContext() {
        Engine self = this;
        return new SnapshotContext() {
            @Override public CompletionStage<SnapshotResult> snapshot() { return self.snapshot(); }
            @Override public java.util.concurrent.ScheduledExecutorService scheduler() { return self.scheduler; }
            @Override public io.fom.log.LogBackend logBackend() { return self.logBackend; }
            @Override public void purgeArchives(int keepHistory) { self.purgeOldArchives(keepHistory); }
        };
    }

    /**
     * Dispatch a query by type. Priority:
     * <ol>
     *   <li>If {@code msg instanceof Routable} → route to {@code msg.targetProcess()}.</li>
     *   <li>Otherwise, look up {@code graph.typeRouting()} by {@code msg.getClass()}
     *       (exact match) and apply the {@link QueryRoute}.</li>
     *   <li>Otherwise → fail with {@link QueryException}.</li>
     * </ol>
     */
    public CompletionStage<Object> query(Object q) {
        return query(q, config.queryTimeout());
    }

    public CompletionStage<Object> query(Object q, Duration timeout) {
        Objects.requireNonNull(q, "q");
        Objects.requireNonNull(timeout, "timeout");
        ensureOpen();
        GraphMachine machine = graphMachine;
        if (machine == null) {
            return failed(new IllegalStateException("No graph installed; call newGraph(graph) first"));
        }
        String target;
        try {
            target = resolveTarget(q);
        } catch (RuntimeException e) {
            return failed(e);
        }
        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        return machine.submitQuery(target, q, deadlineMs)
                .toCompletableFuture()
                .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Explicit addressing — bypasses {@link Routable} and {@code typeRouting}. */
    public CompletionStage<Object> queryProcess(String processName, Object q) {
        return queryProcess(processName, q, config.queryTimeout());
    }

    public CompletionStage<Object> queryProcess(String processName, Object q, Duration timeout) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(q, "q");
        Objects.requireNonNull(timeout, "timeout");
        ensureOpen();
        GraphMachine machine = graphMachine;
        if (machine == null) {
            return failed(new IllegalStateException("No graph installed; call newGraph(graph) first"));
        }
        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        return machine.submitQuery(processName, q, deadlineMs)
                .toCompletableFuture()
                .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public boolean trigger(String processName, Serializable value) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(value, "value");
        ensureOpen();
        GraphMachine machine = graphMachine;
        if (machine == null) {
            throw new IllegalStateException("No graph installed; call newGraph(graph) first");
        }
        if (!machine.contains(processName)) {
            throw new IllegalArgumentException("Unknown process: '" + processName + "'");
        }
        persistTrigger(Map.of(processName, value));
        machine.handleTrigger(processName, new ReinitCause.Triggered(value));
        return true;
    }

    public boolean trigger(Map<String, Serializable> nameToValue) {
        Objects.requireNonNull(nameToValue, "nameToValue");
        if (nameToValue.isEmpty()) return false;
        ensureOpen();
        GraphMachine machine = graphMachine;
        if (machine == null) {
            throw new IllegalStateException("No graph installed; call newGraph(graph) first");
        }
        for (String name : nameToValue.keySet()) {
            if (!machine.contains(name)) {
                throw new IllegalArgumentException("Unknown process: '" + name + "'");
            }
        }
        persistTrigger(nameToValue);
        for (var entry : nameToValue.entrySet()) {
            machine.handleTrigger(entry.getKey(), new ReinitCause.Triggered(entry.getValue()));
        }
        return true;
    }

    private void persistTrigger(Map<String, Serializable> nameToValue) {
        Map<String, byte[]> serialised = new HashMap<>(nameToValue.size());
        for (var entry : nameToValue.entrySet()) {
            serialised.put(entry.getKey(),
                    serDe.serializeTriggerValue(entry.getKey(), entry.getValue()));
        }
        logBackend.append(new LogTrigger(0, System.currentTimeMillis(), serialised), instanceId);
    }

    public <V extends Serializable> AutoCloseable watch(ScheduledWatcher<V> watcher) {
        Objects.requireNonNull(watcher, "watcher");
        ensureOpen();
        GraphMachine machine = graphMachine;
        if (machine == null) {
            throw new IllegalStateException("No graph installed; call newGraph(graph) first");
        }
        if (!machine.contains(watcher.processName())) {
            throw new IllegalArgumentException("Unknown process: '" + watcher.processName() + "'");
        }
        var holder = new java.util.concurrent.atomic.AtomicReference<V>(watcher.initialValue());
        ScheduledFuture<?> handle = scheduler.scheduleWithFixedDelay(
                () -> tickWatcher(watcher, holder),
                watcher.initialDelay().toMillis(),
                watcher.interval().toMillis(),
                TimeUnit.MILLISECONDS);
        return () -> handle.cancel(false);
    }

    private <V extends Serializable> void tickWatcher(ScheduledWatcher<V> watcher,
                                                      java.util.concurrent.atomic.AtomicReference<V> holder) {
        if (closed.get()) return;
        try {
            V prev = holder.get();
            Optional<V> next = watcher.check().apply(prev);
            if (next.isPresent()) {
                V value = next.get();
                holder.set(value);
                trigger(watcher.processName(), value);
            }
        } catch (Throwable t) {
            log.warn("Engine[{}] watcher {} threw: {}", instanceId, watcher.processName(), t.toString());
        }
    }

    public CompletionStage<EngineReport> introspect() {
        ensureOpen();
        GraphMachine machine = graphMachine;
        Version version = new Version(0);
        var logReport = logBackend.introspect();
        if (machine == null) {
            var empty = new EngineReport.GraphMachineReport(java.util.List.of(), java.util.Map.of());
            return CompletableFuture.completedFuture(
                    new EngineReport(instanceId, leaderAtStart, isLeader(logReport), version, logReport, empty));
        }
        var nodeReports = new ArrayList<EngineReport.NodeReport>();
        var mailboxSizes = new HashMap<String, Integer>();
        for (String name : machine.processNames()) {
            var fsm = machine.fsm(name);
            if (fsm == null) continue;
            nodeReports.add(new EngineReport.NodeReport(
                    name,
                    fsm.currentSid(),
                    fsm.currentState().name(),
                    0, // initRetries — tracking added in §5+ extension
                    0, // loadRetries
                    null));
            mailboxSizes.put(name, fsm.mailboxSize());
        }
        var graphReport = new EngineReport.GraphMachineReport(nodeReports, mailboxSizes);
        return CompletableFuture.completedFuture(
                new EngineReport(instanceId, leaderAtStart, isLeader(logReport), version, logReport, graphReport));
    }

    private boolean isLeader(io.fom.log.LogBackendReport report) {
        return instanceId.equals(report.currentLeader());
    }

    public CompletionStage<Done> shutdown(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        GraphMachine machine = graphMachine;
        if (machine == null) {
            return CompletableFuture.completedFuture(Done.INSTANCE);
        }
        return CompletableFuture.supplyAsync(() -> {
            machine.shutdownAll(timeout);
            return Done.INSTANCE;
        });
    }

    public CompletionStage<Void> cancelInit(Sid sid) {
        Objects.requireNonNull(sid, "sid");
        ensureOpen();
        GraphMachine machine = graphMachine;
        if (machine == null) {
            return failed(new IllegalStateException("No graph installed"));
        }
        var fsm = machine.fsm(sid.processName());
        if (fsm == null) {
            return failed(new IllegalArgumentException("Unknown process: '" + sid.processName() + "'"));
        }
        return fsm.cancelInit(sid);
    }

    public CompletionStage<SnapshotResult> snapshot() {
        ensureOpen();
        if (graphMachine == null) {
            return failed(new IllegalStateException("No graph installed"));
        }
        // Run on the common pool, NOT the single-thread scheduler — otherwise a
        // scheduled snapshot blocked on .get() would deadlock the scheduler.
        return CompletableFuture.supplyAsync(this::doSnapshot);
    }

    private SnapshotResult doSnapshot() {
        Graph g = this.graph;
        if (g == null) {
            throw new IllegalStateException("Graph went null mid-snapshot");
        }
        long now = System.currentTimeMillis();
        int checkpointClock = Math.max(logBackend.length() - 1, 0);

        // Find the latest still-live LogInitialized for every node in the graph.
        Map<String, LogInitialized> latest = new HashMap<>();
        Map<String, Integer> latestDeadClock = new HashMap<>();
        int len = logBackend.length();
        for (int i = 0; i < len; i++) {
            LogEvent e = logBackend.get(i);
            switch (e) {
                case LogInitialized init when g.nodes().containsKey(init.processName()) ->
                        latest.put(init.processName(), init);
                case LogDead dead when g.nodes().containsKey(dead.sid().processName()) ->
                        latestDeadClock.put(dead.sid().processName(), dead.sid().clock());
                default -> { }
            }
        }

        List<LogEvent> events = new ArrayList<>();
        events.add(new LogLeader(0, now, instanceId));
        events.add(new LogSnapshot(0, now, checkpointClock));
        events.add(new LogChangeGraph(0, now, serDe.serializeProcessGraph(g)));
        for (var entry : latest.entrySet()) {
            Integer deadClock = latestDeadClock.get(entry.getKey());
            if (deadClock != null && deadClock >= entry.getValue().clock()) continue;
            LogInitialized old = entry.getValue();
            events.add(new LogInitialized(0, now, old.processName(), old.properties()));
        }
        return logBackend.compact(events, instanceId);
    }

    private void purgeOldArchives(int keepHistory) {
        try {
            Path logPath = Paths.get(logBackend.logId());
            Path parent = logPath.getParent();
            if (parent == null) return;
            String prefix = logPath.getFileName().toString() + ".archived.";
            List<Path> archives;
            try (Stream<Path> stream = Files.list(parent)) {
                archives = stream
                        .filter(p -> p.getFileName().toString().startsWith(prefix))
                        .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                        .toList();
            }
            int toDelete = archives.size() - keepHistory;
            for (int i = 0; i < toDelete; i++) {
                Files.deleteIfExists(archives.get(i));
            }
        } catch (IOException e) {
            log.warn("Engine[{}] archive purge failed: {}", instanceId, e.toString());
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        ScheduledFuture<?> task = snapshotTask;
        if (task != null) {
            task.cancel(false);
        }
        AutoCloseable customHandle = customSnapshotHandle;
        if (customHandle != null) {
            try {
                customHandle.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
        GraphMachine machine = graphMachine;
        if (machine != null) {
            try {
                machine.shutdownAll(config.defaultCleanupTimeout());
            } catch (Exception e) {
                log.warn("Engine[{}] shutdown error during close: {}", instanceId, e.toString());
            }
        }
        try {
            scheduler.shutdownNow();
        } catch (Exception ignored) {
            // best-effort
        }
        // Engine does NOT close logBackend — caller owns its lifecycle.
    }

    // ───────────────── internals ─────────────────

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Engine " + instanceId + " is closed");
        }
    }

    private String resolveTarget(Object q) {
        if (q instanceof Routable r) {
            String name = r.targetProcess();
            if (name == null || name.isEmpty()) {
                throw new QueryException(
                        "Routable returned invalid target for " + q.getClass().getName() + ": '" + name + "'");
            }
            if (graphMachine != null && !graphMachine.contains(name)) {
                throw new QueryException(
                        "Routable.targetProcess() for " + q.getClass().getName()
                                + " resolved to unknown process: '" + name + "'");
            }
            return name;
        }
        QueryRoute route = graph.typeRouting().get(q.getClass());
        if (route == null) {
            throw new QueryException(
                    "No route for type " + q.getClass().getName()
                            + ". Either implement Routable, register via GraphBuilder.handles() / .route(),"
                            + " or use queryProcess(name, msg).");
        }
        return switch (route) {
            case QueryRoute.Static s -> s.processName();
            case QueryRoute.Dynamic d -> {
                String name = d.resolver().apply(q);
                if (name == null || name.isEmpty()) {
                    throw new QueryException(
                            "Dynamic resolver for " + q.getClass().getName()
                                    + " returned invalid target: '" + name + "'");
                }
                if (!graphMachine.contains(name)) {
                    throw new QueryException(
                            "Dynamic resolver for " + q.getClass().getName()
                                    + " returned unknown process: '" + name + "'");
                }
                yield name;
            }
        };
    }

    private void claimLeadership() {
        var leaderEvent = new LogLeader(0, System.currentTimeMillis(), instanceId);
        Optional<LogEvent> persisted = logBackend.append(leaderEvent, instanceId);
        if (persisted.isEmpty()) {
            throw new IllegalStateException(
                    "Engine[" + instanceId + "] could not claim leadership on " + logBackend.logId());
        }
    }

    private void persistGraphChange(Graph graph) {
        byte[] bytes = serDe.serializeProcessGraph(graph);
        var event = new LogChangeGraph(0, System.currentTimeMillis(), bytes);
        Optional<LogEvent> persisted = logBackend.append(event, instanceId);
        if (persisted.isEmpty()) {
            throw new IllegalStateException(
                    "Engine[" + instanceId + "] lost leadership while persisting graph");
        }
    }

    /**
     * Walk the log once and produce a per-process map of the latest live
     * {@link LogInitialized} for every node in the graph.
     */
    private Map<String, GraphMachine.RestoreSnapshot> scanRestoreSnapshots(Graph g) {
        int len = logBackend.length();
        Map<String, LogInitialized> latestInit = new HashMap<>();
        Map<String, Integer> latestDeadClock = new HashMap<>();
        for (int i = 0; i < len; i++) {
            LogEvent e = logBackend.get(i);
            switch (e) {
                case LogInitialized init when g.nodes().containsKey(init.processName()) ->
                        latestInit.put(init.processName(), init);
                case LogDead dead when g.nodes().containsKey(dead.sid().processName()) ->
                        latestDeadClock.put(dead.sid().processName(), dead.sid().clock());
                default -> { }
            }
        }
        Map<String, GraphMachine.RestoreSnapshot> result = new HashMap<>();
        for (var e : latestInit.entrySet()) {
            Integer deadClock = latestDeadClock.get(e.getKey());
            if (deadClock != null && deadClock >= e.getValue().clock()) continue;
            result.put(e.getKey(), new GraphMachine.RestoreSnapshot(
                    e.getValue().clock(), e.getValue().properties()));
        }
        return result;
    }

    private static <T> CompletionStage<T> failed(Throwable t) {
        var f = new CompletableFuture<T>();
        f.completeExceptionally(t);
        return f;
    }
}
