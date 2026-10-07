package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.InitInProgressException;
import io.fom.api.LeadershipLostException;
import io.fom.api.QueryException;
import io.fom.api.Routable;
import io.fom.api.WatcherStopReason;
import io.fom.fsm.GraphMachine;
import io.fom.fsm.GraphMachine.RestoreSnapshot;
import io.fom.fsm.ProcessFSM;
import io.fom.fsm.QueryDeadlines;
import io.fom.fsm.ReinitCause;
import io.fom.fsm.State;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogPaused;
import io.fom.log.LogResumed;
import io.fom.log.LogTrigger;
import io.fom.serde.SerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Public facade of the runtime.
 *
 * <p>{@link #newGraph(Graph)} claims leadership of the log and installs a graph:
 * a {@link GraphMachine} drives every {@link ProcessFSM} through its
 * lifecycle in topological order, warm-loading persisted state where it exists.
 * Calling it again swaps the graph in place. {@link #query(Object)} dispatches by
 * {@link Routable} → graph {@code typeRouting} → fail;
 * {@link #queryProcess(String, Object)} bypasses both for explicit addressing.</p>
 *
 * <p>Also: triggers and watchers ({@link #trigger}, {@link #watch}), snapshots,
 * pausing/removing processes, and {@link #introspect()}.</p>
 */
public final class Engine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Engine.class);

    private volatile EngineConfig config;
    private final GatedLogBackend logBackend;
    private final SerDe serDe;
    private final String instanceId;
    private final ScheduledExecutorService scheduler;
    /** Runs watcher checks that bring no executor of their own: they may block, the scheduler must not. */
    private final ExecutorService watcherExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("fom-watcher-", 0).factory());
    private final EngineObserver observer;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    /** Completed once the first close() is done; later callers wait for it. */
    private final CompletableFuture<Void> closeDone = new CompletableFuture<>();
    private volatile Thread closingThread;

    /** Serialises control-plane mutations: {@code newGraph}, {@code updateConfig}, pause/resume/remove. */
    private final ReentrantLock controlLock = new ReentrantLock();

    /**
     * Graphs that newGraph calls are waiting to install behind {@link #controlLock}, by identity.
     * {@link #pendingLock} guards it and makes a remove/pause's refusal check and its cancellation
     * atomic against a newGraph joining the queue.
     */
    private final List<Graph> pendingGraphs = new ArrayList<>();
    private final ReentrantLock pendingLock = new ReentrantLock();

    /** Set while an updateGraph change function runs on this thread. */
    private final ThreadLocal<Boolean> insideGraphChange = new ThreadLocal<>();

    private volatile Graph graph;
    private volatile GraphMachine graphMachine;

    /** Dead processes a trigger is currently restarting (one restart at a time per process). */
    private final Set<String> restartsInFlight = ConcurrentHashMap.newKeySet();
    private volatile ScheduledFuture<?> snapshotTask;
    private volatile AutoCloseable customSnapshotHandle;

    public Engine(EngineConfig config, LogBackend logBackend, SerDe serDe) {
        this(config, logBackend, serDe, EngineObserver.NOOP);
    }

    public Engine(EngineConfig config,
                  LogBackend logBackend,
                  SerDe serDe,
                  EngineObserver observer) {
        this.config = Objects.requireNonNull(config, "config");
        this.logBackend = new GatedLogBackend(Objects.requireNonNull(logBackend, "logBackend"));
        this.serDe = Objects.requireNonNull(serDe, "serDe");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.instanceId = "fom-" + UUID.randomUUID();
        var timer = new java.util.concurrent.ScheduledThreadPoolExecutor(
                1, r -> {
                    var t = new Thread(r, "fom-scheduler-" + instanceId);
                    t.setDaemon(true);
                    return t;
                });
        // A cancelled retry or deadline must not hold its FSM until it would have fired.
        timer.setRemoveOnCancelPolicy(true);
        this.scheduler = timer;
    }

    public String instanceId() {
        return instanceId;
    }

    public EngineConfig config() {
        return config;
    }

    /**
     * Swap the engine's {@link EngineConfig}. Operations started from now on use the new values;
     * running ones keep what they captured. A changed {@code snapshotPolicy} re-arms the rotation.
     */
    public void updateConfig(EngineConfig next) {
        requireNotInsideGraphChange("updateConfig");
        Objects.requireNonNull(next, "next");
        ensureOpen();
        lockControl("updateConfig");
        try {
            ensureOpen();
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

    /**
     * Change the running graph atomically: {@code change} gets the installed graph and returns the
     * one to install, with no other control-plane call in between. Prefer it to
     * {@code newGraph(modified currentGraph())}, which a concurrent removal can slip into.
     *
     * @param change must be quick and must not call back into this engine's control plane
     * @return what {@link #newGraph} returns for the resulting graph
     * @throws IllegalStateException if no graph is installed yet, or the engine is closed
     */
    public boolean updateGraph(UnaryOperator<Graph> change) {
        requireNotInsideGraphChange("updateGraph");
        Objects.requireNonNull(change, "change");
        lockControl("updateGraph");
        try {
            ensureOpen();
            Graph current = this.graph;
            if (current == null) throw noGraph();
            Graph next;
            insideGraphChange.set(Boolean.TRUE);
            try {
                next = Objects.requireNonNull(change.apply(current), "the change returned null");
            } finally {
                insideGraphChange.remove();
            }
            return newGraph(next); // the control lock is reentrant: nothing runs in between
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Install {@code graph}. The first call claims leadership ({@link LogLeader}),
     * persists {@link LogChangeGraph}, spawns one {@code ProcessFSM} per node in
     * topological order (warm-loading persisted state, keeping paused processes
     * paused) and blocks until every running node reaches {@code Serving}. Later
     * calls swap the running graph in place.
     *
     * @return {@code true} on the first install, or if a swap changed anything
     * @throws IllegalStateException if called after {@link #close()} or if
     *         leadership cannot be claimed
     */
    public boolean newGraph(Graph graph) {
        requireNotInsideGraphChange("newGraph");
        Objects.requireNonNull(graph, "graph");
        // Checked before the lock too: the operation holding it may be waiting for this dispatcher.
        Graph installed = this.graph;
        if (installed != null) requireNotRedefiningOwnDispatcher(installed, graph);
        return install(graph, true);
    }

    /** @param respawnDead whether unchanged nodes that are Dead or never started are started again */
    private boolean install(Graph graph, boolean respawnDead) {
        ensureOpen();
        pendingLock.lock();
        try {
            pendingGraphs.add(graph); // lets a concurrent remove/pause see what is queued behind the lock
        } finally {
            pendingLock.unlock();
        }
        try {
            return applyGraph(graph, respawnDead);
        } finally {
            forgetPending(graph);
        }
    }

    private void forgetPending(Graph graph) {
        pendingLock.lock();
        try {
            for (int i = 0; i < pendingGraphs.size(); i++) {
                if (pendingGraphs.get(i) == graph) { // this call's own entry, not an equal graph of another call
                    pendingGraphs.remove(i);
                    return;
                }
            }
        } finally {
            pendingLock.unlock();
        }
    }

    private boolean applyGraph(Graph graph, boolean respawnDead) {
        // Two concurrent first installs must not each build a GraphMachine.
        lockControl("newGraph");
        try {
            // Again under the lock: close() takes it too, so no FSM starts on a closing engine.
            ensureOpen();
            return graphMachine == null ? startFirstGraph(graph) : swapGraph(graph, respawnDead);
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * In-place swap, diffed against the running graph. Added and changed nodes cold-init: their old
     * state was built for another definition. Warm-loading across restarts is the first install's job.
     */
    private boolean swapGraph(Graph graph, boolean respawnDead) {
        Graph previous = this.graph;
        requireNoLiveConsumerOfPaused(graph, graphMachine);
        // Under the lock this time, against the graph the swap really replaces.
        requireNotRedefiningOwnDispatcher(previous, graph);
        warnParamsWithoutEquals(previous, graph);
        // An identical graph records nothing; the swap below still runs, to restart Dead nodes.
        if (GraphDiff.compute(previous, graph).hasChanges() || routingDiffers(previous, graph)) {
            persistGraphChange(graph);
        }
        // Callers (currentGraph, routing, tenant lookups) see the new graph from now on, as the machine does.
        this.graph = graph;
        GraphDiff diff;
        try {
            // Unchanged nodes that are not running (a failed start, a Dead process) warm-load their live state.
            var respawnState = new AtomicReference<Map<String, RestoreSnapshot>>();
            diff = graphMachine.applyGraphChange(graph, name -> null, name -> {
                // Nodes start concurrently and several may ask at once: scan the log once.
                synchronized (respawnState) {
                    if (respawnState.get() == null) respawnState.set(readRestoreSnapshots(graph, null));
                }
                return respawnState.get().get(name);
            }, respawnDead);
        } finally {
            // The machine routes against the new topology even if a node failed to start; stay in sync.
            this.graph = graph;
        }
        // Routing lives in the graph, not in the FSMs: a routing-only change touches no node.
        return diff.hasChanges() || routingDiffers(previous, graph);
    }

    private boolean startFirstGraph(Graph graph) {
        // Leadership first: it fences any other writer, so the scan below sees everything.
        claimLeadership();
        // Scanned before the graph is recorded: a log the scan refuses gets no LogChangeGraph appended.
        LogCompaction.Scan scan = logBackend.readStable(LogCompaction::scan);
        persistGraphChange(graph);
        Map<String, RestoreSnapshot> snapshots = scanRestoreSnapshots(graph, scan);
        PausedAtStart pausedAtStartup = pausedAtStart(graph, scan);
        Map<String, Boolean> pausedAtStart = pausedAtStartup.stale();
        Map<String, RestoreSnapshot> candidates = scanCandidates(graph, scan, snapshots);
        coldInitReleaseFirstCandidates(graph, snapshots, candidates, pausedAtStart);
        Set<String> mustReinit = invalidateStaleConsumers(graph, snapshots, pausedAtStart);
        candidates.keySet().retainAll(snapshots.keySet());
        // Re-inits requested before the last shutdown are replayed once the warm-loaded state is
        // back; a paused process just becomes stale.
        List<String> replay = new ArrayList<>();
        Set<String> requested = new HashSet<>(scan.pendingReinit());
        requested.addAll(pausedAtStartup.staleReleased());
        for (String name : requested) {
            if (!snapshots.containsKey(name)) continue; // cold-inits anyway
            if (pausedAtStart.containsKey(name)) {
                pausedAtStart.put(name, true);
            } else {
                replay.add(name);
            }
        }
        GraphMachine machine = new GraphMachine(
                graph, () -> this.config, logBackend, instanceId, scheduler, observer, snapshots::get);
        this.graphMachine = machine;
        this.graph = graph;
        // Kept in the machine: a retry after a failed start rescans the log and must not warm-load
        // a consumer built on state its producer has since replaced.
        machine.requireReinit(mustReinit);

        try {
            machine.startAll(pausedAtStart, pausedAtStartup.forDependency(), candidates::get);
        } catch (RuntimeException startFailed) {
            keepReplayAfterFailedStart(machine, replay, startFailed);
            // A retry takes the swap path, which never arms rotation: arm it now.
            try {
                scheduleSnapshotRotation();
            } catch (RuntimeException alsoFailed) {
                startFailed.addSuppressed(alsoFailed);
            }
            throw startFailed;
        }
        for (String name : replay) {
            log.info("Engine[{}] replaying the re-init of '{}' requested before the last shutdown",
                    instanceId, name);
            machine.handleTrigger(name, new ReinitCause.Triggered("requested before restart"));
        }
        scheduleSnapshotRotation();
        return true;
    }

    /**
     * A re-init requested before the last shutdown must survive a failed start. A node still
     * starting or serving takes it as a trigger; a Dead one is marked to cold-init on retry.
     */
    private static void keepReplayAfterFailedStart(GraphMachine machine, List<String> replay,
                                                   RuntimeException startFailed) {
        for (String name : replay) {
            try {
                if (machine.isDead(name)) {
                    machine.requireReinit(Set.of(name));
                } else {
                    machine.handleTrigger(name, new ReinitCause.Triggered("requested before restart"));
                }
            } catch (RuntimeException alsoFailed) {
                startFailed.addSuppressed(alsoFailed); // never mask why the start failed
            }
        }
    }

    private void scheduleSnapshotRotation() {
        SnapshotPolicy policy = config.snapshotPolicy();
        if (policy instanceof SnapshotPolicy.Disabled) return;
        if (policy instanceof SnapshotPolicy.FixedInterval fixed) {
            snapshotTask = scheduleFixedSnapshots(fixed);
            return;
        }
        // A custom policy schedules itself.
        AutoCloseable handle = policy.activate(snapshotContext());
        if (handle != null) customSnapshotHandle = handle;
    }

    private ScheduledFuture<?> scheduleFixedSnapshots(SnapshotPolicy.FixedInterval policy) {
        long periodMs = Math.max(1L, policy.interval().toMillis()); // a sub-millisecond interval is 0 ms
        var running = new AtomicBoolean();
        var failuresInARow = new AtomicLong();
        return scheduler.scheduleAtFixedRate(() -> {
            // A tick while a snapshot runs is skipped: queued snapshots would hold the log gate back to back.
            if (!running.compareAndSet(false, true)) return;
            try {
                // Not waited for here: a blocked scheduler would stall the dedup, retry and watcher timers.
                snapshot().toCompletableFuture().whenComplete((result, err) -> {
                    try {
                        afterScheduledSnapshot(policy, err, failuresInARow);
                    } finally {
                        running.set(false);
                    }
                });
            } catch (RuntimeException e) {
                running.set(false);
                long n = failuresInARow.incrementAndGet();
                if (Long.bitCount(n) == 1) {
                    log.warn("Engine[{}] scheduled snapshot could not start ({} failure(s) in a row): {}",
                            instanceId, n, e.toString());
                }
            }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    private void afterScheduledSnapshot(SnapshotPolicy.FixedInterval policy, Throwable err, AtomicLong failuresInARow) {
        if (err != null) {
            // A snapshot that keeps failing fails on every tick: WARN on the 1st, 2nd, 4th, 8th… in a row.
            long n = failuresInARow.incrementAndGet();
            if (Long.bitCount(n) == 1) {
                log.warn("Engine[{}] scheduled snapshot failed ({} in a row; repeats are logged at"
                        + " doubling intervals): {}", instanceId, n, err.toString());
            } else {
                log.debug("Engine[{}] scheduled snapshot failed ({} in a row): {}", instanceId, n, err.toString());
            }
            return;
        }
        long n = failuresInARow.getAndSet(0);
        if (n > 0) log.info("Engine[{}] scheduled snapshot succeeded after {} failure(s)", instanceId, n);
        // After close() the backend may be closed too.
        if (policy.purgesArchives() && !closed.get()) purgeOldArchives(policy.keepHistory());
    }

    private SnapshotContext snapshotContext() {
        return new SnapshotContext() {
            @Override public CompletionStage<SnapshotResult> snapshot() { return Engine.this.snapshot(); }
            @Override public ScheduledExecutorService scheduler() { return Engine.this.scheduler; }
            @Override public LogBackend logBackend() { return Engine.this.logBackend; }
            @Override public void purgeArchives(int keepHistory) { purgeOldArchives(keepHistory); }
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
        GraphMachine machine;
        String target;
        try {
            machine = requireMachine();
            target = resolveTarget(machine, q);
        } catch (RuntimeException e) {
            return failed(e);
        }
        return submitWithDeadline(machine, target, q, timeout);
    }

    /** Explicit addressing — bypasses {@link Routable} and {@code typeRouting}. */
    public CompletionStage<Object> queryProcess(String processName, Object q) {
        return queryProcess(processName, q, config.queryTimeout());
    }

    public CompletionStage<Object> queryProcess(String processName, Object q, Duration timeout) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(q, "q");
        Objects.requireNonNull(timeout, "timeout");
        GraphMachine machine;
        try {
            machine = requireMachine();
        } catch (RuntimeException e) {
            return failed(e);
        }
        return submitWithDeadline(machine, processName, q, timeout);
    }

    /** {@link #queryProcess(String, Object)} keyed by a typed {@link ProcessRef}. */
    public CompletionStage<Object> queryProcess(ProcessRef ref, Object q) {
        Objects.requireNonNull(ref, "ref");
        return queryProcess(ref.name(), q);
    }

    /** {@link #queryProcess(String, Object, Duration)} keyed by a typed {@link ProcessRef}. */
    public CompletionStage<Object> queryProcess(ProcessRef ref, Object q, Duration timeout) {
        Objects.requireNonNull(ref, "ref");
        return queryProcess(ref.name(), q, timeout);
    }

    /**
     * Submit a query whose reply fails with a {@link TimeoutException} naming the target once
     * {@code timeout} passes. Sent from inside a compute, it also inherits that compute's deadline
     * and is reported as its child, so nobody waits for an answer past the outer query's deadline.
     */
    private static CompletionStage<Object> submitWithDeadline(GraphMachine machine, String target, Object q,
                                                              Duration timeout) {
        long now = System.currentTimeMillis();
        long deadlineMs = now + timeout.toMillis();
        String message = "Query to '" + target + "' did not complete within " + timeout;
        Long inherited = ProcessFSM.issuingDeadline();
        if (inherited != null && inherited < deadlineMs) {
            deadlineMs = inherited;
            message = "Query to '" + target + "' did not complete within the deadline of the query"
                    + " whose compute sent it";
        }
        if (deadlineMs <= now) {
            return failed(new TimeoutException(message)); // not even submitted
        }
        CompletableFuture<Object> reply = machine.submitQuery(target, q, deadlineMs,
                ProcessFSM.issuingQueryId()).toCompletableFuture();
        QueryDeadlines.failAfter(reply, deadlineMs - now, message);
        return reply;
    }

    /**
     * Ask {@code processName} to re-initialise. The request is recorded in the log
     * first, then applied after the dedup window; a {@code Dead} process is restarted
     * instead. Returns once the request is recorded, not once the re-init is done.
     *
     * @throws IllegalArgumentException if the process is not in the graph
     * @throws LeadershipLostException if another instance owns the log — the
     *         request was not recorded and nothing was applied
     */
    public boolean trigger(String processName, Serializable value) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(value, "value");
        GraphMachine machine = requireMachine();
        if (!machine.knows(processName)) {
            throw new IllegalArgumentException("Unknown process: '" + processName + "'");
        }
        // Recorded first, for a Dead process too: the record makes the request durable, and a
        // refused append is how this instance learns it no longer leads.
        persistTrigger(List.of(processName));
        if (machine.isDead(processName)) {
            restartDead(machine, processName);
            return true;
        }
        machine.handleTrigger(processName, new ReinitCause.Triggered(value));
        return true;
    }

    /** {@link #trigger(String, Serializable)} keyed by a typed {@link ProcessRef}. */
    public boolean trigger(ProcessRef ref, Serializable value) {
        Objects.requireNonNull(ref, "ref");
        return trigger(ref.name(), value);
    }

    /**
     * Ask several processes to re-initialise, recording one request for all of them.
     * Nothing is applied until the request is in the log, so a refused append leaves
     * every named process untouched.
     *
     * @throws IllegalArgumentException if any name is not in the graph
     * @throws LeadershipLostException if another instance owns the log
     */
    public boolean trigger(Map<String, Serializable> nameToValue) {
        Objects.requireNonNull(nameToValue, "nameToValue");
        ensureOpen(); // before the empty check: a closed engine fails every call, even a no-op one
        if (nameToValue.isEmpty()) return false;
        GraphMachine machine = requireMachine();
        for (String name : nameToValue.keySet()) {
            if (!machine.knows(name)) {
                throw new IllegalArgumentException("Unknown process: '" + name + "'");
            }
        }
        Map<String, Serializable> alive = new LinkedHashMap<>();
        List<String> dead = new ArrayList<>();
        for (var entry : nameToValue.entrySet()) {
            if (machine.isDead(entry.getKey())) {
                dead.add(entry.getKey());
            } else {
                alive.put(entry.getKey(), entry.getValue());
            }
        }
        // Recorded first, for every name (Dead ones included), so a refused append applies nothing.
        persistTrigger(nameToValue.keySet());
        for (String name : dead) {
            restartDead(machine, name);
        }
        for (var entry : alive.entrySet()) {
            machine.handleTrigger(entry.getKey(), new ReinitCause.Triggered(entry.getValue()));
        }
        return true;
    }

    /**
     * A Dead process ignores re-init requests: start it again. Its FSM is replaced
     * at once (queries wait for the restart instead of being rejected) and it
     * warm-loads its persisted state if it has live state. If a graph swap holds
     * the machine, the whole graph is retried after it instead.
     */
    private void restartDead(GraphMachine machine, String processName) {
        if (!restartsInFlight.add(processName)) {
            return; // a restart is already on its way
        }
        log.info("Engine[{}] trigger for Dead process '{}': restarting it", instanceId, processName);
        // Read for the graph the machine holds when it restarts the process (under its lock).
        Function<Graph, Function<String, RestoreSnapshot>> stateFor = g -> readRestoreSnapshots(g, null)::get;
        try {
            if (machine.restartDead(processName, stateFor)) {
                restartsInFlight.remove(processName);
                return;
            }
        } catch (RuntimeException e) {
            restartsInFlight.remove(processName);
            throw e;
        }
        Thread.ofVirtual().name("fom-restart-" + processName).start(() -> {
            // Only once the swap holding the machine is over: its graph is the one to keep.
            controlLock.lock();
            try {
                if (closed.get() || graphMachine != machine || !machine.isDead(processName)) return;
                if (!machine.restartDead(processName, stateFor)) {
                    newGraph(currentGraph());
                }
            } catch (RuntimeException e) {
                log.warn("Engine[{}] restarting Dead process '{}' failed: {}", instanceId, processName, e.toString());
            } finally {
                controlLock.unlock();
                restartsInFlight.remove(processName);
            }
        });
    }

    /** Recorded before it is applied, so a re-init still in the dedup window at shutdown is replayed. */
    private void persistTrigger(Collection<String> processNames) {
        var persisted = logBackend.append(
                new LogTrigger(0, System.currentTimeMillis(), List.copyOf(processNames)), instanceId);
        if (persisted.isEmpty()) {
            throw new LeadershipLostException("Engine " + instanceId + " is no longer the leader of "
                    + logBackend.logId() + "; the trigger for " + processNames + " was not recorded");
        }
    }

    /**
     * Poll {@code watcher} at its interval. {@code check} runs on the watcher's own executor, or on
     * an engine virtual thread when it has none, so a blocking check never delays the engine's
     * timers. Checks of one watcher never overlap: a tick is skipped while the previous one runs,
     * so an executor must run each task it accepts or reject it by throwing.
     *
     * @return a handle whose {@code close()} stops scheduling further checks (a
     *         check already running finishes and may still trigger)
     */
    public <V extends Serializable> AutoCloseable watch(ScheduledWatcher<V> watcher) {
        Objects.requireNonNull(watcher, "watcher");
        GraphMachine machine = requireMachine();
        if (!machine.contains(watcher.processName()) && !machine.isPaused(watcher.processName())) {
            throw new IllegalArgumentException("Unknown process: '" + watcher.processName() + "'");
        }
        var lastValue = new AtomicReference<V>(watcher.initialValue());
        Executor ownExecutor = watcher.executor().orElse(null);
        AtomicBoolean checkRunning = new AtomicBoolean();
        var handleRef = new AtomicReference<ScheduledFuture<?>>();
        AtomicBoolean stopped = new AtomicBoolean();
        Runnable stop = () -> {
            stopped.set(true); // also covers a first tick that runs before handleRef is set
            ScheduledFuture<?> scheduled = handleRef.get();
            if (scheduled != null) scheduled.cancel(false);
        };
        Runnable check = () -> {
            try {
                checkWatcher(watcher, lastValue, stop);
            } finally {
                checkRunning.set(false);
            }
        };
        Runnable dispatch = () -> {
            if (ownExecutor == null) {
                check.run();
                return;
            }
            try {
                ownExecutor.execute(check);
            } catch (RuntimeException e) { // rejected: the executor is shut down or saturated
                checkRunning.set(false);
                if (ownExecutor instanceof ExecutorService es && es.isShutdown()) {
                    // Permanent: stop with one WARN instead of the same rejection on every tick.
                    log.warn("Engine[{}] watcher for '{}' stopped: its executor is shut down",
                            instanceId, watcher.processName());
                    watcherStopped(watcher, WatcherStopReason.EXECUTOR_SHUT_DOWN);
                    stop.run();
                    return;
                }
                log.warn("Engine[{}] watcher {} could not dispatch its check: {}",
                        instanceId, watcher.processName(), e.toString());
            }
        };
        ScheduledFuture<?> handle = scheduler.scheduleWithFixedDelay(() -> {
            if (closed.get() || stopped.get() || !checkRunning.compareAndSet(false, true)) return;
            try {
                watcherExecutor.execute(dispatch); // never run user code on the scheduler thread
            } catch (RuntimeException e) { // engine closing
                checkRunning.set(false);
            }
        }, watcher.initialDelay().toMillis(), Math.max(1L, watcher.interval().toMillis()), TimeUnit.MILLISECONDS);
        handleRef.set(handle);
        if (stopped.get()) handle.cancel(false);
        return () -> handle.cancel(false);
    }

    private <V extends Serializable> void checkWatcher(ScheduledWatcher<V> watcher, AtomicReference<V> lastValue,
                                                       Runnable stop) {
        if (closed.get()) return;
        if (!stopIfProcessGone(watcher, stop)) return;
        try {
            Optional<V> next = watcher.check().apply(lastValue.get());
            // A value from a check that was still running when the engine closed is dropped quietly.
            if (next.isEmpty() || closed.get()) return;
            if (!stopIfProcessGone(watcher, stop)) return;
            V value = next.get();
            // Advanced only once the trigger is recorded, so a failed append is retried on the next tick.
            trigger(watcher.processName(), value);
            lastValue.set(value);
        } catch (Throwable t) {
            if (closed.get()) return;
            if (t instanceof LeadershipLostException) {
                // Nothing this watcher triggers can be recorded any more; leadership is not regained.
                log.warn("Engine[{}] watcher for '{}' stopped: {}", instanceId, watcher.processName(), t.getMessage());
                watcherStopped(watcher, WatcherStopReason.LEADERSHIP_LOST);
                stop.run();
                return;
            }
            if (t instanceof IllegalArgumentException && !stopIfProcessGone(watcher, stop)) {
                return; // removed between the check above and the trigger
            }
            log.warn("Engine[{}] watcher {} tick failed (check or trigger; retried on the next tick): {}",
                    instanceId, watcher.processName(), t.toString());
        }
    }

    /**
     * @return {@code true} if the watcher's process is still in the graph; otherwise
     *         stops the watcher (logging one WARN) and returns {@code false}
     */
    private boolean stopIfProcessGone(ScheduledWatcher<?> watcher, Runnable stop) {
        GraphMachine machine = graphMachine;
        if (machine == null || machine.knows(watcher.processName())) return true;
        log.warn("Engine[{}] watcher for '{}' stopped: the process is no longer in the graph",
                instanceId, watcher.processName());
        watcherStopped(watcher, WatcherStopReason.PROCESS_REMOVED);
        stop.run();
        return false;
    }

    private void watcherStopped(ScheduledWatcher<?> watcher, WatcherStopReason reason) {
        try {
            observer.onWatcherStopped(watcher.processName(), reason);
        } catch (Throwable t) {
            log.warn("Engine[{}] observer onWatcherStopped threw: {}", instanceId, t.toString());
        }
    }

    /** Whether {@link #close()} has been called: every other call then fails. */
    public boolean isClosed() {
        return closed.get();
    }

    public CompletionStage<EngineReport> introspect() {
        ensureOpen();
        GraphMachine machine = graphMachine;
        var logReport = logBackend.introspect();
        if (machine == null) {
            var empty = new EngineReport.GraphReport(List.of(), Map.of());
            return CompletableFuture.completedFuture(
                    new EngineReport(instanceId, isLeader(logReport), logReport, empty));
        }
        var nodeReports = new ArrayList<EngineReport.NodeReport>();
        var mailboxSizes = new HashMap<String, Integer>();
        // Decided per graph node, so a node whose FSM is being replaced is neither missing nor listed twice.
        for (String name : new TreeSet<>(machine.graphNodeNames())) {
            var fsm = machine.fsm(name);
            if (fsm != null) {
                var view = fsm.view();
                String replacement = view.state() instanceof State.Serving serving
                        && serving.replacement() != null ? serving.replacement().name() : null;
                nodeReports.add(new EngineReport.NodeReport(
                        name,
                        view.sid(),
                        view.state().name(),
                        fsm.initRetries(),
                        fsm.loadRetries(),
                        fsm.lastFailure(),
                        replacement,
                        view.state() instanceof State.Serving && (fsm.stale() || machine.reinitPending(name))));
                mailboxSizes.put(name, fsm.mailboxSize());
            } else if (machine.isPaused(name)) {
                nodeReports.add(new EngineReport.NodeReport(name, machine.pausedSid(name), "Paused", 0, 0, null,
                        null, machine.isPausedStale(name)));
                mailboxSizes.put(name, 0);
            } else {
                // Being (re)started right now, or waiting for a dependency that failed to start.
                nodeReports.add(new EngineReport.NodeReport(name, null, "Starting", 0, 0, null));
                mailboxSizes.put(name, 0);
            }
        }
        var graphReport = new EngineReport.GraphReport(nodeReports, mailboxSizes);
        return CompletableFuture.completedFuture(
                new EngineReport(instanceId, isLeader(logReport), logReport, graphReport));
    }

    /**
     * The instance named by the newest {@link LogLeader} record, or {@code null} if there is
     * none. Scans backwards, so a takeover is found at once; call it under
     * {@link GatedLogBackend#readStable} so a concurrent compaction cannot move the positions.
     */
    private static String leaderFromLog(LogBackend backend) {
        final int chunk = 256;
        for (int to = backend.length(); to > 0; to -= chunk) {
            LogEvent[] events = backend.getBetween(Math.max(0, to - chunk), to);
            for (int i = events.length - 1; i >= 0; i--) {
                if (events[i] instanceof LogLeader leader) return leader.instanceId();
            }
        }
        return null;
    }

    /**
     * Where the engine runs its own blocking work (a snapshot, a shutdown). Not the common
     * ForkJoinPool: user code that blocks its workers would stall the engine along with it.
     */
    private static void onVirtualThread(Runnable task) {
        Thread.ofVirtual().name("fom-engine-task").start(task);
    }

    private boolean isLeader(LogBackendReport report) {
        return instanceId.equals(report.currentLeader());
    }

    /**
     * Stop every running process — consumers first, processes of the same
     * dependency depth together — giving each one {@code timeout} to drain its
     * in-flight queries and run {@code cleanUp}. The log, the scheduler and the
     * backend stay open, so the engine can be reused with {@link #newGraph};
     * {@link #close()} shuts those down too and is what most callers want.
     *
     * <p>Reuse needs this engine to still lead its log: the next {@link #newGraph} restarts the
     * stopped nodes as an in-place swap and does not claim leadership again, so on a log taken
     * over it throws {@link LeadershipLostException}. Close a deposed engine and open a new one.</p>
     *
     * @return completes once every process has stopped (or its budget ran out)
     */
    public CompletionStage<Done> shutdown(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        GraphMachine machine = graphMachine;
        if (machine == null) {
            return CompletableFuture.completedFuture(Done.INSTANCE);
        }
        return CompletableFuture.supplyAsync(() -> {
            machine.shutdownAll(timeout);
            return Done.INSTANCE;
        }, Engine::onVirtualThread);
    }

    /**
     * Cancel the init or load running for {@code processName} right now — a cold
     * init has no Sid yet, so this is the form to use for it. The init stage is
     * cancelled and the process goes {@code Dead}; {@link #trigger} it (or call
     * {@code newGraph(currentGraph())}) to start it again. During a
     * {@link ReinitStrategy#KEEP_OLD} re-init only the new version is cancelled: the old one
     * keeps serving, reported stale, and the re-init is not retried until a trigger or a
     * restart; a request queued behind it starts at once. While an automatic re-init retry is
     * only scheduled, the retry is cancelled (the process stays stale). Does nothing otherwise.
     *
     * @throws IllegalArgumentException (in the returned stage) for an unknown or paused process
     */
    public CompletionStage<Void> cancelInit(String processName) {
        Objects.requireNonNull(processName, "processName");
        ensureOpen();
        GraphMachine machine = graphMachine;
        if (machine == null) return failed(noGraph());
        var fsm = machine.fsm(processName);
        if (fsm == null) {
            return failed(new IllegalArgumentException("Unknown or paused process: '" + processName + "'"));
        }
        return fsm.cancelCurrentInit();
    }

    /** {@link #cancelInit(String)} keyed by a typed {@link ProcessRef}. */
    public CompletionStage<Void> cancelInit(ProcessRef ref) {
        Objects.requireNonNull(ref, "ref");
        return cancelInit(ref.name());
    }

    /**
     * Cancel the init or load for exactly {@code sid}: only while the process is
     * loading that Sid (or re-initialising after its load failed). During a
     * {@link ReinitStrategy#KEEP_OLD} re-init that is the new version's Sid; the serving
     * version's Sid cancels nothing. Use {@link #cancelInit(String)} to cancel whatever runs.
     */
    public CompletionStage<Void> cancelInit(Sid sid) {
        Objects.requireNonNull(sid, "sid");
        ensureOpen();
        GraphMachine machine = graphMachine;
        if (machine == null) return failed(noGraph());
        var fsm = machine.fsm(sid.processName());
        if (fsm == null) {
            return failed(new IllegalArgumentException("Unknown process: '" + sid.processName() + "'"));
        }
        return fsm.cancelInit(sid);
    }

    /**
     * The currently installed graph.
     *
     * @throws IllegalStateException if no graph has been installed yet
     */
    public Graph currentGraph() {
        Graph g = graph;
        if (g == null) throw noGraph();
        return g;
    }

    /** {@link #remove(Collection)} for names given inline. */
    public boolean remove(String... names) {
        Objects.requireNonNull(names, "names");
        return remove(List.of(names));
    }

    /** {@link #remove(Collection)} keyed by typed {@link ProcessRef}s. */
    public boolean remove(ProcessRef... refs) {
        return remove(namesOf(refs));
    }

    /** {@link #pause(Collection)} for names given inline. */
    public void pause(String... names) {
        Objects.requireNonNull(names, "names");
        pause(List.of(names));
    }

    /** {@link #pause(Collection)} keyed by typed {@link ProcessRef}s. */
    public void pause(ProcessRef... refs) {
        pause(namesOf(refs));
    }

    /** {@link #resume(Collection)} for names given inline. */
    public void resume(String... names) {
        Objects.requireNonNull(names, "names");
        resume(List.of(names));
    }

    /** {@link #resume(Collection)} keyed by typed {@link ProcessRef}s. */
    public void resume(ProcessRef... refs) {
        resume(namesOf(refs));
    }

    /**
     * A graph swap can hold {@code controlLock} for a whole init budget while one of these
     * processes hangs in its first init or load. Removing or pausing it would discard that work
     * anyway, so cancel it instead of waiting for it.
     */
    private void cancelInitsIfControlBusy(Collection<String> names) {
        if (controlLock.tryLock()) {
            controlLock.unlock();
            return;
        }
        GraphMachine machine = graphMachine;
        if (machine == null) return;
        for (String name : names) {
            // Only a start the operation holding the lock waits for; others are left alone.
            var fsm = name == null || !machine.isAwaitingStart(name) ? null : machine.fsm(name);
            if (fsm == null) continue;
            String state = fsm.currentState().name();
            if ("Initializing".equals(state) || "Loading".equals(state)) {
                fsm.cancelCurrentInitForStop();
            }
        }
    }

    private static List<String> namesOf(ProcessRef... refs) {
        Objects.requireNonNull(refs, "refs");
        List<String> names = new ArrayList<>(refs.length);
        for (ProcessRef ref : refs) {
            names.add(Objects.requireNonNull(ref, "ref").name());
        }
        return names;
    }

    /**
     * Remove processes from the running graph for good — an in-place graph
     * swap to {@link Graph#without}. Their state is retired ({@code LogDead}), so
     * adding them back later cold-inits. Paused processes can be removed too.
     *
     * @return {@code true} if anything was removed
     * @throws IllegalArgumentException for an unknown name, if a remaining
     *         process depends on a removed one, or if removing them all would
     *         leave an empty graph (close the engine instead)
     */
    public boolean remove(Collection<String> names) {
        requireNotInsideGraphChange("remove");
        Objects.requireNonNull(names, "names");
        requireNotStoppingOwnDispatcher("remove", names);
        refuseIfCallbackWouldWait("remove");
        // A request that will be refused must not cancel anything (decided for real under the lock below).
        cancelIfAcceptedByEveryKnownGraph(names, g -> {
            Set<String> known = knownNames(g, names);
            if (!known.isEmpty()) g.without(known);
        });
        lockControl("remove");
        try {
            ensureOpen();
            Graph g = currentGraph();
            Set<String> toRemove = knownNames(g, names);
            if (toRemove.isEmpty()) return false;
            Graph next = g.without(toRemove); // refuses a removal that would leave a dependency missing
            log.info("Engine[{}] removing {}", instanceId, new TreeSet<>(toRemove));
            // Only removes: a Dead or never-started process elsewhere is left as it is.
            return install(next, false);
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Stop processes but keep their state: no {@code LogDead} is written, so
     * {@link #resume} warm-loads them. Queries to a paused process fail
     * with {@link io.fom.api.QueryRejectedException}; a trigger or a reactive
     * dependency change that arrives meanwhile makes it re-initialise on resume.
     * Already paused names are ignored.
     *
     * <p>The pause is persisted ({@code LogPaused}, re-emitted by snapshots): after
     * a restart the processes come up paused until resumed. A process that would
     * run but depends on a paused one is then paused as well.</p>
     *
     * @throws IllegalArgumentException for an unknown name, or if a process that
     *         stays running depends on one being paused — pause consumers
     *         together with their producers
     */
    public void pause(Collection<String> names) {
        requireNotInsideGraphChange("pause");
        Objects.requireNonNull(names, "names");
        requireNotStoppingOwnDispatcher("pause", names);
        refuseIfCallbackWouldWait("pause");
        // A request that will be refused must not cancel anything (decided for real under the lock below).
        GraphMachine machineBefore = graphMachine;
        if (machineBefore != null) {
            cancelIfAcceptedByEveryKnownGraph(names, g -> requirePausable(g, machineBefore, knownNames(g, names)));
        }
        lockControl("pause");
        try {
            ensureOpen();
            Graph g = currentGraph();
            GraphMachine machine = graphMachine;
            Set<String> toPause = knownNames(g, names);
            requirePausable(g, machine, toPause);
            Set<String> newlyPaused = new TreeSet<>(toPause);
            newlyPaused.removeIf(machine::isPaused);
            if (!newlyPaused.isEmpty()) log.info("Engine[{}] pausing {}", instanceId, newlyPaused);
            machine.pause(toPause, config.cleanupTimeout());
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Restart paused processes, dependencies first, warm-loading their kept
     * state. Names that are not paused are ignored.
     *
     * @throws IllegalArgumentException for an unknown name, or if a dependency of
     *         a process being resumed stays paused
     * @throws RuntimeException if a process fails to reach {@code Serving}; it
     *         stays paused and the call can be retried
     */
    public void resume(Collection<String> names) {
        requireNotInsideGraphChange("resume");
        Objects.requireNonNull(names, "names");
        lockControl("resume");
        try {
            ensureOpen();
            Graph g = currentGraph();
            GraphMachine machine = graphMachine;
            Set<String> toResume = new HashSet<>();
            for (String name : knownNames(g, names)) {
                if (machine.isPaused(name)) toResume.add(name);
            }
            if (toResume.isEmpty()) return;
            for (String name : toResume) {
                for (Dependency dep : g.nodes().get(name).dependencies()) {
                    if (machine.isPaused(dep.name()) && !toResume.contains(dep.name())) {
                        throw new IllegalArgumentException("Cannot resume '" + name
                                + "': its dependency '" + dep.name() + "' is still paused");
                    }
                }
            }
            RestoreState state = readRestoreState(g, toResume);
            log.info("Engine[{}] resuming {}", instanceId, new TreeSet<>(toResume));
            machine.resume(toResume, state.snapshots()::get, state.candidates()::get);
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Paused processes nobody asked to pause: at startup they depended on a
     * paused process, so the engine paused them too. Resume them with
     * {@link #resume} once their dependencies are resumed; if the engine
     * restarts first, one whose dependencies are no longer paused simply starts.
     */
    public Set<String> pausedByDependency() {
        GraphMachine machine = graphMachine;
        return machine == null ? Set.of() : Set.copyOf(machine.pausedForDependencyNames());
    }

    /**
     * Atomically resume those of {@code names} that are still paused only for a
     * dependency ({@link #pausedByDependency()}) and none of whose dependencies
     * is paused; names paused by an operator meanwhile, or still blocked, are left
     * alone. Unknown names are ignored.
     *
     * <p>If another operation cancels one of these starts meanwhile (an operator
     * pausing it, a {@code cancelInit}), that is not an error: the process stays
     * paused and is simply missing from the result, as are the names after it.</p>
     *
     * @return the names resumed
     * @throws RuntimeException if a process fails to reach {@code Serving} for any
     *         other reason; it stays paused and the call can be retried
     */
    public Set<String> resumeUnblocked(Collection<String> names) {
        requireNotInsideGraphChange("resumeUnblocked");
        Objects.requireNonNull(names, "names");
        lockControl("resumeUnblocked");
        try {
            ensureOpen();
            Graph g = currentGraph();
            GraphMachine machine = graphMachine;
            Set<String> waiting = machine.pausedForDependencyNames();
            Set<String> toResume = new HashSet<>();
            for (String name : names) {
                ProcessNode node = g.nodes().get(name);
                if (node == null || !waiting.contains(name)) continue;
                boolean blocked = node.dependencies().stream().anyMatch(dep -> machine.isPaused(dep.name()));
                if (!blocked) toResume.add(name);
            }
            if (toResume.isEmpty()) return Set.of();
            RestoreState state = readRestoreState(g, toResume);
            try {
                machine.resume(toResume, state.snapshots()::get, state.candidates()::get);
            } catch (RuntimeException e) {
                // Another operation cancelled the start (an operator pausing it, say): it stays paused.
                if (!causedBy(e, InitInProgressException.class)) throw e;
                log.info("Engine[{}] resuming {} was cancelled by another operation: {}",
                        instanceId, toResume, e.toString());
            }
            Set<String> resumed = new HashSet<>(toResume);
            resumed.removeIf(machine::isPaused);
            return Set.copyOf(resumed);
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Cancel the inits of {@code names} only if {@code check} accepts (throws no
     * {@link IllegalArgumentException} for) the graph the machine runs or is installing and every
     * graph a newGraph call waits to install, and the machine's graph did not change meanwhile.
     */
    private void cancelIfAcceptedByEveryKnownGraph(Collection<String> names, Consumer<Graph> check) {
        pendingLock.lock(); // no newGraph joins the queue between the check and the cancellation
        try {
            GraphMachine machine = graphMachine;
            Graph running = machine != null ? machine.graph() : this.graph;
            List<Graph> graphs = new ArrayList<>(pendingGraphs);
            if (running != null) graphs.add(running);
            try {
                for (Graph g : graphs) check.accept(g);
            } catch (IllegalArgumentException refused) {
                return;
            }
            if (machine == null || machine.graph() == running) cancelInitsIfControlBusy(names);
        } finally {
            pendingLock.unlock();
        }
    }

    private static void requirePausable(Graph g, GraphMachine machine, Set<String> toPause) {
        for (ProcessNode node : g.nodes().values()) {
            if (toPause.contains(node.name()) || machine.isPaused(node.name())) continue;
            for (Dependency dep : node.dependencies()) {
                if (toPause.contains(dep.name())) {
                    throw new IllegalArgumentException("Cannot pause '" + dep.name() + "': '"
                            + node.name() + "' depends on it and would keep running");
                }
            }
        }
    }

    private static boolean causedBy(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (type.isInstance(c)) return true;
            if (c.getCause() == c) break;
        }
        return false;
    }

    /**
     * Whether the two graphs route query types differently: a type routed by one and
     * not the other, or a static route to another process. Two dynamic routes for the
     * same type count as equal: their resolvers are lambdas, so a rebuilt graph would
     * otherwise always look changed.
     */
    private static boolean routingDiffers(Graph before, Graph after) {
        Map<Class<?>, QueryRoute> from = before.typeRouting();
        Map<Class<?>, QueryRoute> to = after.typeRouting();
        if (!from.keySet().equals(to.keySet())) return true;
        for (var entry : from.entrySet()) {
            QueryRoute other = to.get(entry.getKey());
            boolean same = switch (entry.getValue()) {
                case QueryRoute.Static s -> other instanceof QueryRoute.Static t
                        && s.processName().equals(t.processName());
                case QueryRoute.Dynamic ignored -> other instanceof QueryRoute.Dynamic;
            };
            if (!same) return true;
        }
        return false;
    }

    private static Set<String> knownNames(Graph g, Collection<String> names) {
        Set<String> known = new HashSet<>();
        for (String name : names) {
            Objects.requireNonNull(name, "name");
            if (!g.nodes().containsKey(name)) {
                throw new IllegalArgumentException("Unknown process: '" + name + "'");
            }
            known.add(name);
        }
        return known;
    }

    /** A graph swap must not leave a running process depending on a paused one. */
    private static void requireNoLiveConsumerOfPaused(Graph next, GraphMachine machine) {
        for (ProcessNode node : next.nodes().values()) {
            if (machine.isPaused(node.name())) continue;
            for (Dependency dep : node.dependencies()) {
                if (machine.isPaused(dep.name())) {
                    throw new IllegalArgumentException("'" + node.name() + "' would depend on paused '"
                            + dep.name() + "'; resume it first or pause the consumer too");
                }
            }
        }
    }

    /**
     * Delete all but the newest {@code keepHistory} archives of this log. Neither {@link #snapshot()}
     * nor a snapshot policy with {@link SnapshotPolicy#KEEP_ALL} purges, so callers relying on those
     * purge here. Unlike a policy's own purge, a refused or failed deletion fails this call.
     *
     * <p>Leader-only. A log with no {@code LogLeader} yet can be purged by anyone. Confirming
     * leadership may read the whole log backwards, so this is a maintenance call, not a hot-path one.</p>
     *
     * @throws IllegalArgumentException if {@code keepHistory < 0}
     * @throws LeadershipLostException if another instance leads the log
     */
    public void purgeArchives(int keepHistory) {
        if (keepHistory < 0) {
            throw new IllegalArgumentException("keepHistory must be >= 0, was " + keepHistory);
        }
        ensureOpen();
        // A backend that has not written since a takeover still reports itself as the leader, so
        // "this instance leads" is confirmed against the log. A report naming someone else never lags.
        var report = logBackend.introspect();
        String leader = isLeader(report) || report.currentLeader() == null
                ? logBackend.readStable(Engine::leaderFromLog)
                : report.currentLeader();
        if (leader != null && !instanceId.equals(leader)) {
            throw new LeadershipLostException("Engine " + instanceId + " is not the leader of "
                    + logBackend.logId() + " (" + leader + " is); no archive was removed");
        }
        logBackend.purgeArchives(keepHistory);
    }

    public CompletionStage<SnapshotResult> snapshot() {
        ensureOpen();
        if (graphMachine == null) return failed(noGraph());
        // Its own virtual thread: a scheduled snapshot must not block the scheduler, and user
        // code can block the common pool.
        return CompletableFuture.supplyAsync(this::doSnapshot, Engine::onVirtualThread);
    }

    private SnapshotResult doSnapshot() {
        // Planned and compacted under the gate's write lock: an append in between would be lost.
        return logBackend.snapshot(
                backend -> LogCompaction.plan(backend, instanceId, System.currentTimeMillis()), instanceId);
    }

    /**
     * @param stale          processes that come up paused (name → stale flag); mutable
     * @param forDependency  those of them paused only because a dependency is
     * @param staleReleased  processes paused only for a dependency that is no longer
     *                       paused, which start now but were marked stale meanwhile
     */
    private record PausedAtStart(Map<String, Boolean> stale, Set<String> forDependency, Set<String> staleReleased) { }

    /**
     * Processes the operator paused stay paused. A process that would run but
     * depends on a paused one cannot start either — and the engine cannot be
     * asked to resume anything before it is up — so it comes up paused too,
     * which is logged and persisted as a pause for a dependency. Such a pause
     * lapses by itself once no dependency is paused any more.
     */
    private PausedAtStart pausedAtStart(Graph g, LogCompaction.Scan scan) {
        Map<String, Boolean> result = new HashMap<>();
        Set<String> forDependency = new HashSet<>();
        Set<String> staleReleased = new HashSet<>();
        for (ProcessNode node : g.topologicalOrder()) {
            String name = node.name();
            Boolean staleInLog = scan.paused().get(name);
            boolean forDependencyInLog = scan.pausedForDependency().contains(name);
            if (staleInLog != null && !forDependencyInLog) {
                result.put(name, staleInLog);
                continue;
            }
            String pausedDependency = null;
            for (Dependency dep : node.dependencies()) {
                if (result.containsKey(dep.name())) {
                    pausedDependency = dep.name();
                    break;
                }
            }
            boolean stale = staleInLog != null && staleInLog;
            if (pausedDependency != null) {
                if (!forDependencyInLog) {
                    log.warn("Engine[{}] process '{}' depends on paused '{}' and starts paused too",
                            instanceId, name, pausedDependency);
                    appendAsLeader(new LogPaused(0, System.currentTimeMillis(), name, false, true));
                }
                result.put(name, stale);
                forDependency.add(name);
            } else if (forDependencyInLog) {
                log.info("Engine[{}] process '{}' was paused only because a dependency was; none is paused now, "
                        + "so it starts", instanceId, name);
                appendAsLeader(new LogResumed(0, System.currentTimeMillis(), name));
                if (stale) staleReleased.add(name);
            }
        }
        // A paused process the graph leaves out counts as removed, so its pause is cleared as a
        // removal clears it; otherwise adding it back later would bring it up paused after a restart.
        for (String name : new TreeSet<>(scan.paused().keySet())) {
            if (g.nodes().containsKey(name)) continue;
            log.info("Engine[{}] paused process '{}' is not in the graph; its pause is cleared", instanceId, name);
            appendAsLeader(new LogResumed(0, System.currentTimeMillis(), name));
        }
        return new PausedAtStart(result, forDependency, staleReleased);
    }

    private void purgeOldArchives(int keepHistory) {
        if (keepHistory == SnapshotPolicy.KEEP_ALL) return; // retention off: do not even list the archives
        try {
            logBackend.purgeArchives(keepHistory);
        } catch (RuntimeException e) {
            log.warn("Engine[{}] archive purge failed: {}", instanceId, e.toString());
        }
    }

    @Override
    public void close() {
        String dispatched = ProcessFSM.dispatchedProcess(instanceId);
        if (dispatched != null) {
            if (closed.get()) return; // already closing: nothing for this thread to wait for
            throw new IllegalStateException("close() called on the dispatcher of '" + dispatched
                    + "' (from an EngineObserver callback about it); call it from another thread");
        }
        if (!closed.compareAndSet(false, true)) {
            // Another caller is closing: return once it is done, or the caller may close the backend
            // under processes still stopping. Unless this is the closing thread itself, re-entering.
            if (closingThread != Thread.currentThread()) closeDone.join();
            return;
        }
        closingThread = Thread.currentThread();
        log.info("Engine[{}] closing", instanceId);
        try {
            closeOnce();
        } finally {
            closingThread = null;
            closeDone.complete(null);
        }
    }

    /**
     * An observer callback about a process runs on that process's dispatcher. Stopping the process
     * from there would wait for the very thread that has to handle the stop, so it is refused up front.
     */
    /** lockControl's refusal, checked before a call's pre-lock side effects (cancelling starts). */
    private void refuseIfCallbackWouldWait(String call) {
        if (ProcessFSM.dispatchedProcess(instanceId) != null && controlLock.isLocked()
                && !controlLock.isHeldByCurrentThread()) {
            throw new IllegalStateException(call + "(...) called from an EngineObserver callback while another"
                    + " control-plane call is in progress; call it from another thread");
        }
    }

    /**
     * Takes the control lock. An observer callback runs on a process's dispatcher, and the control call
     * holding the lock (a first newGraph, say) may be waiting for that very process: from there the
     * call fails at once instead of waiting out a budget.
     */
    private void lockControl(String call) {
        if (ProcessFSM.dispatchedProcess(instanceId) == null) {
            controlLock.lock();
        } else if (!controlLock.tryLock()) {
            throw new IllegalStateException(call + "(...) called from an EngineObserver callback while another"
                    + " control-plane call is in progress; call it from another thread");
        }
    }

    private void requireNotStoppingOwnDispatcher(String call, Collection<String> names) {
        String dispatched = ProcessFSM.dispatchedProcess(instanceId);
        if (dispatched != null && names.contains(dispatched)) {
            throw new IllegalStateException(call + "('" + dispatched + "') called on that process's own dispatcher"
                    + " (from an EngineObserver callback about it); call it from another thread");
        }
    }

    private void requireNotRedefiningOwnDispatcher(Graph previous, Graph next) {
        String dispatched = ProcessFSM.dispatchedProcess(instanceId);
        if (dispatched == null) return;
        GraphDiff pending = GraphDiff.compute(previous, next);
        if (pending.removed().contains(dispatched) || pending.changed().contains(dispatched)) {
            throw new IllegalStateException("a graph change that removes or redefines '" + dispatched
                    + "' was called on that process's own dispatcher (from an EngineObserver callback about it);"
                    + " call it from another thread");
        }
    }

    /**
     * A control-plane call from inside an updateGraph change function would re-enter the reentrant
     * control lock and run, and the graph the function then returns would silently undo it.
     */
    private void requireNotInsideGraphChange(String call) {
        if (insideGraphChange.get() != null) {
            throw new IllegalStateException(call + " called from inside an updateGraph change function;"
                    + " the change must only compute the new graph");
        }
    }

    private void closeOnce() {
        // A first newGraph still waiting for its nodes holds controlLock for up to
        // their whole init budget: make it give up first.
        GraphMachine starting = graphMachine;
        if (starting != null) starting.abortStartup();
        // Wait for an in-flight control-plane call, so it cannot start FSMs or arm snapshots after the teardown.
        controlLock.lock();
        try {
            cancelScheduledSnapshot();
            GraphMachine machine = graphMachine;
            if (machine != null) {
                try {
                    machine.shutdownAll(config.cleanupTimeout());
                } catch (Exception e) {
                    log.warn("Engine[{}] shutdown error during close: {}", instanceId, e.toString());
                }
            }
            try {
                scheduler.shutdownNow();
                // Interrupted and given a bounded wait, so no check triggers into a closed engine.
                watcherExecutor.shutdownNow();
                if (!watcherExecutor.awaitTermination(config.cleanupTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                    log.warn("Engine[{}] watcher checks still running after close", instanceId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception ignored) {
                // best-effort
            }
        } finally {
            controlLock.unlock();
        }
        // The log backend is not closed here: the caller owns it.
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Engine " + instanceId + " is closed");
        }
    }

    /** The graph machine, for calls that need a running graph. */
    /** The FSM running {@code name}, or {@code null} (for tests). */
    ProcessFSM fsmOf(String name) {
        GraphMachine machine = graphMachine;
        return machine == null ? null : machine.fsm(name);
    }

    private GraphMachine requireMachine() {
        ensureOpen();
        GraphMachine machine = graphMachine;
        if (machine == null) throw noGraph();
        return machine;
    }

    private static IllegalStateException noGraph() {
        return new IllegalStateException("No graph installed; call newGraph(graph) first");
    }

    private String resolveTarget(GraphMachine machine, Object q) {
        if (q instanceof Routable r) {
            String name = r.targetProcess();
            if (name == null || name.isEmpty()) {
                throw new QueryException(
                        "Routable returned invalid target for " + q.getClass().getName() + ": '" + name + "'");
            }
            if (!machine.knows(name)) { // a node still starting is known
                throw new QueryException("Routable.targetProcess() for " + q.getClass().getName()
                        + " resolved to unknown process: '" + name + "'");
            }
            return name;
        }
        // An enum constant with a body is an anonymous subclass: route it by its enum type.
        Class<?> type = q instanceof Enum<?> constant ? constant.getDeclaringClass() : q.getClass();
        QueryRoute route = graph.typeRouting().get(type);
        if (route == null) {
            throw new QueryException("No route for type " + type.getName()
                    + "; implement Routable, register it with .handles()/.route(), or use queryProcess");
        }
        return switch (route) {
            case QueryRoute.Static s -> s.processName();
            case QueryRoute.Dynamic d -> {
                String name = d.resolver().apply(q);
                if (name == null || name.isEmpty()) {
                    throw new QueryException("Dynamic resolver for " + q.getClass().getName()
                            + " returned invalid target: '" + name + "'");
                }
                if (!machine.knows(name)) { // a node still starting is known
                    throw new QueryException("Dynamic resolver for " + q.getClass().getName()
                            + " returned unknown process: '" + name + "'");
                }
                yield name;
            }
        };
    }

    private void claimLeadership() {
        var leaderEvent = new LogLeader(0, System.currentTimeMillis(), instanceId);
        if (logBackend.append(leaderEvent, instanceId).isEmpty()) {
            throw new IllegalStateException(
                    "Engine[" + instanceId + "] could not claim leadership on " + logBackend.logId());
        }
    }

    private void persistGraphChange(Graph graph) {
        appendAsLeader(new LogChangeGraph(0, System.currentTimeMillis(), graphNodes(graph)), "graph");
    }

    private void appendAsLeader(LogEvent event) {
        appendAsLeader(event, event.getClass().getSimpleName());
    }

    private void appendAsLeader(LogEvent event, String what) {
        if (logBackend.append(event, instanceId).isEmpty()) {
            throw new LeadershipLostException("Engine[" + instanceId + "] lost leadership while persisting " + what);
        }
    }

    /** {@link #scanRestoreSnapshots} over the current log, which a snapshot cannot shrink meanwhile. */
    private Map<String, RestoreSnapshot> readRestoreSnapshots(Graph g, Set<String> only) {
        return logBackend.readStable(backend -> scanRestoreSnapshots(g, LogCompaction.scan(backend), only));
    }

    private Map<String, RestoreSnapshot> scanRestoreSnapshots(Graph g, LogCompaction.Scan scan) {
        return scanRestoreSnapshots(g, scan, null);
    }

    /** What a resume loads: the live state and any replacement of it persisted but not loaded. */
    private record RestoreState(Map<String, RestoreSnapshot> snapshots, Map<String, RestoreSnapshot> candidates) { }

    private RestoreState readRestoreState(Graph g, Set<String> only) {
        return logBackend.readStable(backend -> {
            LogCompaction.Scan scan = LogCompaction.scan(backend);
            Map<String, RestoreSnapshot> snapshots = scanRestoreSnapshots(g, scan, only);
            return new RestoreState(snapshots, scanCandidates(g, scan, snapshots));
        });
    }

    /**
     * The replacements persisted but never loaded (the engine stopped mid re-init) of the states in
     * {@code snapshots}, if written under the same definition: loaded in the background once the
     * old state serves, without a new init.
     */
    private Map<String, RestoreSnapshot> scanCandidates(Graph g, LogCompaction.Scan scan,
                                                        Map<String, RestoreSnapshot> snapshots) {
        Map<String, RestoreSnapshot> result = new HashMap<>();
        for (var entry : scan.candidates().entrySet()) {
            String name = entry.getKey();
            RestoreSnapshot incumbent = snapshots.get(name);
            LogCompaction.LiveInit live = scan.liveInits().get(name);
            if (incumbent == null || live == null || incumbent.clock() != live.init().clock()) continue;
            LogCompaction.LiveInit candidate = entry.getValue();
            if (candidate.graphPosition() != live.graphPosition() && candidate.graphPosition() >= 0) {
                LogChangeGraph.Node then = graphNodesAtPosition(candidate.graphPosition()).get(name);
                if (then == null || !sameDefinition(then, g.nodes().get(name))) continue;
            }
            result.put(name, new RestoreSnapshot(candidate.init().clock(), candidate.init().properties()));
        }
        return result;
    }

    /**
     * A {@link ReinitStrategy#RELEASE_FIRST} node never holds two versions, so a replacement persisted
     * under KEEP_OLD is not loaded beside its old state: the node cold-inits (a paused one becomes
     * stale), and its reactive consumers re-initialise with it.
     */
    private void coldInitReleaseFirstCandidates(Graph g, Map<String, RestoreSnapshot> snapshots,
                                                Map<String, RestoreSnapshot> candidates,
                                                Map<String, Boolean> pausedAtStart) {
        for (String name : List.copyOf(candidates.keySet())) {
            ProcessNode node = g.nodes().get(name);
            ReinitStrategy strategy = node.reinitStrategy() != null ? node.reinitStrategy() : config.reinitStrategy();
            if (strategy != ReinitStrategy.RELEASE_FIRST) continue;
            candidates.remove(name);
            if (pausedAtStart.containsKey(name)) {
                pausedAtStart.put(name, true);
            } else {
                log.info("Engine[{}] process '{}' has an unfinished re-init and re-initialises under RELEASE_FIRST; "
                        + "cold-initialising instead of warm-loading", instanceId, name);
                snapshots.remove(name);
            }
        }
    }

    /**
     * The latest live {@link LogInitialized} of every node of {@code g} whose definition has not
     * changed since it was persisted.
     *
     * @param only the names the caller will start, or {@code null} for all: checking a definition
     *             decodes its param, which resuming one node should not pay for every other node
     */
    private Map<String, RestoreSnapshot> scanRestoreSnapshots(Graph g, LogCompaction.Scan scan, Set<String> only) {
        Map<Integer, Map<String, LogChangeGraph.Node>> graphsByPosition = new HashMap<>();
        Map<String, RestoreSnapshot> result = new HashMap<>();
        for (var entry : scan.liveInits().entrySet()) {
            String name = entry.getKey();
            if (!g.nodes().containsKey(name)) continue;
            if (only != null && !only.contains(name)) continue;
            LogCompaction.LiveInit live = entry.getValue();
            // State is only valid for the definition it was built under, as in a graph swap.
            if (live.graphPosition() >= 0) {
                LogChangeGraph.Node then = graphsByPosition
                        .computeIfAbsent(live.graphPosition(), this::graphNodesAtPosition).get(name);
                if (then == null || !sameDefinition(then, g.nodes().get(name))) {
                    log.info("Engine[{}] process '{}' definition changed since its state was persisted "
                            + "(clock {}); cold-initialising instead of warm-loading",
                            instanceId, name, live.init().clock());
                    continue;
                }
            }
            result.put(name, new RestoreSnapshot(live.init().clock(), live.init().properties()));
        }
        return result;
    }

    /**
     * A reactive consumer's state is only valid against the producer state it was built from.
     * At startup that no longer holds when a reactive dependency cold-inits, or when the
     * dependency's live {@link LogInitialized} is newer than the consumer's (the engine stopped
     * before the cascade reached it). Such a consumer serves its state and re-initialises in the
     * background once started ({@link ReinitStrategy#RELEASE_FIRST}: cold-inits instead); a paused
     * one is marked stale.
     *
     * @return the processes that had state but must re-initialise because it was built on retired state
     */
    private Set<String> invalidateStaleConsumers(Graph g, Map<String, RestoreSnapshot> snapshots,
                                                 Map<String, Boolean> pausedAtStart) {
        Set<String> coldInits = new HashSet<>();
        Set<String> invalidated = new HashSet<>();
        for (ProcessNode node : g.topologicalOrder()) {
            String name = node.name();
            RestoreSnapshot own = snapshots.get(name);
            String reason = null;
            for (Dependency dep : node.dependencies()) {
                if (!(dep instanceof Dependency.Reactive)) continue;
                RestoreSnapshot produced = snapshots.get(dep.name());
                if (coldInits.contains(dep.name())) {
                    reason = "its reactive dependency '" + dep.name() + "' cold-inits";
                } else if (own != null && produced != null && produced.clock() > own.clock()) {
                    reason = "its reactive dependency '" + dep.name() + "' was re-initialised (clock "
                            + produced.clock() + ") after it (clock " + own.clock() + ")";
                }
                if (reason != null) break;
            }
            if (pausedAtStart.containsKey(name)) {
                if (reason != null && own != null) pausedAtStart.put(name, true);
                continue;
            }
            if (own == null) {
                coldInits.add(name);
            } else if (reason != null) {
                ReinitStrategy strategy = node.reinitStrategy() != null ? node.reinitStrategy() : config.reinitStrategy();
                if (strategy == ReinitStrategy.RELEASE_FIRST) {
                    log.info("Engine[{}] process '{}' was built against retired state ({}); "
                            + "cold-initialising instead of warm-loading", instanceId, name, reason);
                    snapshots.remove(name);
                    coldInits.add(name);
                } else {
                    log.info("Engine[{}] process '{}' was built against retired state ({}); "
                            + "it serves that state and re-initialises once started", instanceId, name, reason);
                }
                invalidated.add(name);
            }
        }
        return invalidated;
    }

    /**
     * The structure of {@code g} as recorded in {@link LogChangeGraph}. Factories and routes
     * live in code and are never read back from the log, so they are left out.
     */
    private List<LogChangeGraph.Node> graphNodes(Graph g) {
        List<LogChangeGraph.Node> nodes = new ArrayList<>();
        for (ProcessNode node : g.topologicalOrder()) {
            List<String> reactive = new ArrayList<>();
            List<String> stable = new ArrayList<>();
            for (Dependency dep : node.dependencies()) {
                (dep instanceof Dependency.Reactive ? reactive : stable).add(dep.name());
            }
            byte[] param = node.param() == null ? null : serDe.serializeParam(node.name(), node.param());
            nodes.add(new LogChangeGraph.Node(node.name(), reactive, stable, param));
        }
        return nodes;
    }

    /** The nodes recorded by the {@link LogChangeGraph} at log {@code position}, by name. */
    private Map<String, LogChangeGraph.Node> graphNodesAtPosition(int position) {
        Map<String, LogChangeGraph.Node> byName = new HashMap<>();
        if (logBackend.get(position) instanceof LogChangeGraph change) {
            for (LogChangeGraph.Node node : change.nodes()) {
                byName.put(node.name(), node);
            }
        }
        return byName;
    }

    /**
     * Whether {@code node} still has the definition {@code recorded}: the same
     * dependencies (by name and kind) and an equal {@code param} — the rule a
     * graph swap applies ({@link GraphDiff#equivalent}). A recorded param that
     * cannot be decoded is logged and treated as unchanged.
     */
    private boolean sameDefinition(LogChangeGraph.Node recorded, ProcessNode node) {
        Set<String> reactive = new HashSet<>();
        Set<String> stable = new HashSet<>();
        for (Dependency dep : node.dependencies()) {
            (dep instanceof Dependency.Reactive ? reactive : stable).add(dep.name());
        }
        if (!reactive.equals(new HashSet<>(recorded.reactiveDependencies()))
                || !stable.equals(new HashSet<>(recorded.stableDependencies()))) {
            return false;
        }
        if (recorded.param() == null || node.param() == null) {
            return recorded.param() == null && node.param() == null;
        }
        try {
            boolean same = Objects.equals(serDe.loadParam(node.name(), recorded.param()), node.param());
            if (!same && !overridesEquals(node.param())) {
                log.warn("Engine[{}] the param of '{}' ({}) does not override equals(), so it never equals "
                        + "its persisted copy and the process cold-inits on every restart; use a record "
                        + "(Kotlin: a data class or data object) or implement equals()",
                        instanceId, node.name(), node.param().getClass().getName());
            }
            return same;
        } catch (RuntimeException e) {
            // The whole cause chain: the usual culprit is a renamed or moved param class, named only by the cause.
            log.warn("Engine[{}] cannot decode the recorded param of '{}' to verify its definition, so a "
                    + "changed param is NOT detected and the process warm-loads its old state: {}",
                    instanceId, node.name(), describeCauses(e));
            return true;
        }
    }

    /**
     * The swap-path twin of the restart WARN in {@link #sameDefinition}: a param without its own
     * {@code equals} never equals the one of the running graph, so every rebuilt graph re-inits it.
     */
    private void warnParamsWithoutEquals(Graph previous, Graph next) {
        for (var entry : next.nodes().entrySet()) {
            ProcessNode before = previous.nodes().get(entry.getKey());
            Object param = entry.getValue().param();
            if (before == null || param == null || before.param() == null) continue;
            if (param != before.param() && param.getClass() == before.param().getClass()
                    && !overridesEquals(param)) {
                log.warn("Engine[{}] the param of '{}' ({}) does not override equals(), so it never equals "
                        + "the running one and every newGraph re-inits the process; use a record "
                        + "or implement equals()", instanceId, entry.getKey(), param.getClass().getName());
            }
        }
    }

    private static String describeCauses(Throwable e) {
        var text = new StringBuilder(e.toString());
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        seen.add(e);
        for (Throwable c = e.getCause(); c != null && seen.add(c); c = c.getCause()) {
            text.append(" <- ").append(c);
        }
        return text.toString();
    }

    private static boolean overridesEquals(Object value) {
        try {
            return value.getClass().getMethod("equals", Object.class).getDeclaringClass() != Object.class;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static <T> CompletionStage<T> failed(Throwable t) {
        var f = new CompletableFuture<T>();
        f.completeExceptionally(t);
        return f;
    }
}
