package io.fom.fsm;

import io.fom.Dependency;
import io.fom.EngineConfig;
import io.fom.Graph;
import io.fom.GraphDiff;
import io.fom.ProcessNode;
import io.fom.ReinitStrategy;
import io.fom.Sid;
import io.fom.api.EngineObserver;
import io.fom.api.InitInProgressException;
import io.fom.api.LeadershipLostException;
import io.fom.api.QueryException;
import io.fom.api.QueryRejectedException;
import io.fom.log.LogBackend;
import io.fom.log.LogDead;
import io.fom.log.LogDependencyChanged;
import io.fom.log.LogEvent;
import io.fom.log.LogPaused;
import io.fom.log.LogResumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.Instant;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Owns the {@link ProcessFSM}s of one graph: routes cross-process queries, collapses triggers
 * within the dedup window, and runs the reactive cascade after every Sid promotion.
 *
 * <p>A node starts once all of its dependencies serve, so a cold-init consumer can query them.
 * Shutdown goes consumers first.</p>
 */
public final class GraphMachine implements ProcessRouter, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GraphMachine.class);

    /** Added to a stop budget before giving up on the wait. */
    private static final long STOP_GRACE_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

    private volatile Graph graph;
    private final Supplier<EngineConfig> configSource;
    private final LogBackend logBackend;
    private final String instanceId;
    private final ScheduledExecutorService scheduler;
    private final EngineObserver observer;
    private final Function<String, RestoreSnapshot> restoreLookup;

    // Read lock-free from query, dedup and promotion threads while starts and swaps change it.
    private final Map<String, ProcessFSM> fsms = new ConcurrentHashMap<>();

    /**
     * Nodes stopped by {@link #pause}: no FSM, but their state stays live (no {@code LogDead}) so
     * {@link #resume} can warm-load it.
     */
    private final Map<String, PausedNode> paused = new ConcurrentHashMap<>();

    /**
     * Nodes not started yet whose state is due for a re-init (triggered, or a reactive dependency
     * changed). They warm-load and re-init once serving; a {@link ReinitStrategy#RELEASE_FIRST}
     * node cold-inits instead.
     */
    private final Set<String> staleUnspawned = ConcurrentHashMap.newKeySet();

    /** Nodes a control operation started and waits for; only their inits may be cancelled on its behalf. */
    private final Set<String> awaitingStart = ConcurrentHashMap.newKeySet();

    /**
     * Queries for graph nodes with no FSM right now (starting, or mid swap), handed over once the FSM
     * exists. Its monitor also guards installing an FSM against them.
     */
    private final Map<String, List<Envelope.Query>> waitingForFsm = new HashMap<>();

    /**
     * Last Sid promoted per process. A fresh FSM (resume, restart from Dead, swap) reports its first
     * promotion without a previous Sid; the cascade continues from this one.
     */
    private final Map<String, Sid> lastPromoted = new ConcurrentHashMap<>();

    private volatile boolean startupAborted;

    /**
     * @param sid           the state consumers were built on
     * @param forDependency paused by the engine only because a dependency was (see {@link LogPaused})
     * @param sidRetired    that state is already retired (paused mid RELEASE_FIRST re-init), so it is
     *                      not reported as the node's Sid — a restart would not show it either
     */
    private record PausedNode(Sid sid, boolean stale, boolean forDependency, boolean sidRetired) {
        PausedNode(Sid sid, boolean stale, boolean forDependency) {
            this(sid, stale, forDependency, false);
        }
    }

    /** Producer name → its consumers. */
    private volatile Map<String, List<Edge>> reverseDeps;

    /** Not a monitor: a swap blocks for seconds, and a parked virtual thread would pin its carrier. */
    private final ReentrantLock graphSwapLock = new ReentrantLock();

    private final Object dedupLock = new Object();
    private final Map<String, ReinitCause> pendingCause = new HashMap<>();
    private final Map<String, Integer> pendingCounts = new HashMap<>();
    private final Map<String, ScheduledFuture<?>> pendingTask = new HashMap<>();

    /**
     * Held while {@link #paused} changes together with its log record, so the records land in the
     * order of the changes; otherwise a restart could bring back paused a node that was resumed.
     */
    private final Object pausedRecord = new Object();

    /** Cascades writing LogDependencyChanged off a producer's dispatcher; see {@link #shutdownAll}. */
    private final Set<Thread> cascades = ConcurrentHashMap.newKeySet();

    /** Per process, shared by its successive FSMs; dropped when the process is removed. */
    private final Map<String, ConcurrentHashMap<String, AtomicLong>> observerFailuresByProcess =
            new ConcurrentHashMap<>();
    /** Callbacks about no particular process. */
    private final Map<String, AtomicLong> observerFailures = new ConcurrentHashMap<>();

    private volatile boolean shuttingDown;

    public GraphMachine(Graph graph,
                        EngineConfig config,
                        LogBackend logBackend,
                        String instanceId,
                        ScheduledExecutorService scheduler,
                        EngineObserver observer,
                        Function<String, RestoreSnapshot> restoreLookup) {
        this(graph, () -> config, logBackend, instanceId, scheduler, observer, restoreLookup);
    }

    public GraphMachine(Graph graph,
                        Supplier<EngineConfig> configSource,
                        LogBackend logBackend,
                        String instanceId,
                        ScheduledExecutorService scheduler,
                        EngineObserver observer,
                        Function<String, RestoreSnapshot> restoreLookup) {
        this.graph = Objects.requireNonNull(graph, "graph");
        this.configSource = Objects.requireNonNull(configSource, "configSource");
        this.logBackend = Objects.requireNonNull(logBackend, "logBackend");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.restoreLookup = Objects.requireNonNull(restoreLookup, "restoreLookup");
        this.reverseDeps = buildReverseDeps(graph);
    }

    public EngineObserver observer() {
        return observer;
    }

    public int mailboxSize(String processName) {
        ProcessFSM fsm = fsms.get(processName);
        return fsm == null ? 0 : fsm.mailboxSize();
    }

    // ───────────────── graph swap ─────────────────

    /**
     * Swap to {@code next} in place: removed and changed nodes are stopped (their state retired),
     * added and changed ones started. Unchanged nodes keep running; reactive consumers of a changed
     * node re-init once its replacement serves.
     *
     * @return the diff; {@code hasChanges()} is {@code false} if {@code next} is structurally identical
     */
    public GraphDiff applyGraphChange(Graph next, Function<String, RestoreSnapshot> snapshots) {
        return applyGraphChange(next, snapshots, name -> null);
    }

    /**
     * As {@link #applyGraphChange(Graph, Function)}; {@code respawnState} gives the state of
     * unchanged nodes that have no running FSM (never started, or Dead), which they warm-load.
     */
    public GraphDiff applyGraphChange(Graph next,
                                      Function<String, RestoreSnapshot> snapshots,
                                      Function<String, RestoreSnapshot> respawnState) {
        return applyGraphChange(next, snapshots, respawnState, true);
    }

    /**
     * As {@link #applyGraphChange(Graph, Function, Function)}; with {@code respawnDead=false},
     * unchanged nodes that are Dead or never started are left alone.
     */
    public GraphDiff applyGraphChange(Graph next,
                                      Function<String, RestoreSnapshot> snapshots,
                                      Function<String, RestoreSnapshot> respawnState,
                                      boolean respawnDead) {
        Objects.requireNonNull(next, "next");
        Objects.requireNonNull(snapshots, "snapshots");
        Objects.requireNonNull(respawnState, "respawnState");
        graphSwapLock.lock();
        try {
            Graph prev = this.graph;
            Set<String> respawned = new HashSet<>();
            GraphDiff computed = GraphDiff.compute(prev, next);
            GraphDiff diff = respawnDead ? respawningDeadNodes(computed, respawned) : computed;

            // Installed first, so cross-process queries route against the new topology.
            this.graph = next;
            this.reverseDeps = buildReverseDeps(next);
            // The strategy is not part of a node's definition: a running node just takes the new one.
            for (String name : diff.unchanged()) {
                ProcessFSM running = fsms.get(name);
                if (running != null) running.setReinitStrategyOverride(next.nodes().get(name).reinitStrategy());
            }

            Duration cleanupTimeout = configSource.get().cleanupTimeout();
            Duration startBudget = startBudget();
            Map<String, RuntimeException> refusedRetires = new TreeMap<>();
            retirePausedNodes(diff, refusedRetires);
            // A changed node's replacement promotes with no previous Sid, yet its consumers must re-init.
            Map<String, Sid> retiredSids = lastServedSids(diff.changed());
            stopRemovedAndChanged(prev, diff, cleanupTimeout, refusedRetires);
            forgetStopped(diff);

            RuntimeException spawnFailure = null;
            try {
                startByDependencies(next.topologicalOrder(),
                        name -> !paused.containsKey(name)
                                && (diff.added().contains(name) || diff.changed().contains(name)),
                        name -> respawned.contains(name) ? respawnState.apply(name) : snapshots.apply(name),
                        name -> null, startBudget);
            } catch (RuntimeException e) {
                spawnFailure = e;
            } finally {
                // Also when a start failed, for the nodes that did come up.
                cascadeReplaced(diff.changed(), retiredSids);
                failUnstarted(diff.added());
                failUnstarted(diff.changed());
            }
            if (spawnFailure != null) {
                if (!refusedRetires.isEmpty()) spawnFailure.addSuppressed(refusedRetireFailure(refusedRetires));
                throw spawnFailure;
            }
            // Only now: starting the new nodes and the cascade must not be skipped over this.
            if (!refusedRetires.isEmpty()) throw refusedRetireFailure(refusedRetires);
            return diff;
        } finally {
            graphSwapLock.unlock();
        }
    }

    /**
     * An unchanged node with no live FSM (a failed swap never started it, or it went Dead) is started
     * again; otherwise retrying the same graph would silently do nothing.
     */
    private GraphDiff respawningDeadNodes(GraphDiff diff, Set<String> respawned) {
        Set<String> added = new HashSet<>(diff.added());
        Set<String> changed = new HashSet<>(diff.changed());
        Set<String> unchanged = new HashSet<>();
        for (String name : diff.unchanged()) {
            ProcessFSM fsm = fsms.get(name);
            if (paused.containsKey(name)) {
                unchanged.add(name); // absent on purpose
            } else if (fsm == null) {
                added.add(name);
                respawned.add(name);
            } else if (fsm.currentState() instanceof State.Dead) {
                changed.add(name);
                respawned.add(name);
            } else {
                unchanged.add(name);
            }
        }
        return new GraphDiff(added, diff.removed(), changed, unchanged);
    }

    /**
     * Paused nodes have no FSM to retire their state. A removed one is retired and unpaused; a
     * changed one stays paused but loses its old state (it cold-inits on resume), and its reactive
     * consumers — paused too, by invariant — must re-init on resume.
     */
    private void retirePausedNodes(GraphDiff diff, Map<String, RuntimeException> refusedRetires) {
        for (String name : diff.removed()) {
            staleUnspawned.remove(name);
            synchronized (pausedRecord) {
                PausedNode p = paused.remove(name);
                if (p != null) {
                    retirePausedState(name, p, refusedRetires);
                    // So a later graph that adds the name back starts it normally.
                    appendQuietly(new LogResumed(0, System.currentTimeMillis(), name), name);
                }
            }
        }
        for (String name : diff.changed()) {
            synchronized (pausedRecord) {
                PausedNode p = paused.get(name);
                if (p == null) continue;
                retirePausedState(name, p, refusedRetires);
                paused.put(name, new PausedNode(null, false, p.forDependency()));
                appendQuietly(new LogPaused(0, System.currentTimeMillis(), name, false, p.forDependency()), name);
            }
            for (Edge edge : reverseDeps.getOrDefault(name, List.of())) {
                if (edge.reactive()) markStale(edge.consumer());
            }
        }
    }

    /** Mid re-init a node has no current Sid; its consumers were built on the last one it served. */
    private Map<String, Sid> lastServedSids(Set<String> names) {
        Map<String, Sid> sids = new HashMap<>();
        for (String name : names) {
            ProcessFSM fsm = fsms.get(name);
            Sid served = fsm != null ? fsm.lastServedSid() : null;
            if (served != null) sids.put(name, served);
        }
        return sids;
    }

    /** In reverse topological order of the old graph. */
    private void stopRemovedAndChanged(Graph prev, GraphDiff diff, Duration timeout,
                                       Map<String, RuntimeException> refusedRetires) {
        var order = new ArrayList<ProcessNode>(prev.topologicalOrder());
        Collections.reverse(order);
        for (ProcessNode oldNode : order) {
            String name = oldNode.name();
            if (diff.removed().contains(name) || diff.changed().contains(name)) {
                shutdownAndRemove(name, timeout, refusedRetires);
            }
        }
    }

    private void forgetStopped(GraphDiff diff) {
        // Only once no FSM answers to the name: a trigger arriving while the node stopped would
        // otherwise schedule a re-init that fires on whatever is added back under the name.
        for (String name : diff.removed()) cancelPendingReinit(name);
        for (String name : diff.changed()) cancelPendingReinit(name);
        for (String name : diff.removed()) {
            lastPromoted.remove(name);
            observerFailuresByProcess.remove(name);
            failWaitingQueries(name, unknownProcess(name));
            try {
                observer.onProcessRemoved(name);
            } catch (Throwable t) {
                observerThrew("onProcessRemoved", t);
            }
        }
    }

    private void cascadeReplaced(Set<String> changed, Map<String, Sid> retiredSids) {
        for (String name : changed) {
            Sid prevSid = retiredSids.get(name);
            ProcessFSM newFsm = fsms.get(name);
            // Read once: the node's dispatcher may already be re-initialising it and clearing its Sid.
            Sid current = newFsm == null ? null : newFsm.currentSid();
            if (prevSid != null && current != null
                    && !prevSid.equals(current) // warm-loaded the same state: nothing changed
                    && !current.equals(lastPromoted.get(name))) { // its own promotion already cascaded
                onSidPromotion(name, prevSid, current);
            }
        }
    }

    /** A node that did not start has no FSM to take its waiting queries. */
    private void failUnstarted(Set<String> names) {
        for (String name : names) {
            if (!fsms.containsKey(name) && !paused.containsKey(name)) {
                failWaitingQueries(name, "Process '" + name + "' did not start");
            }
        }
    }

    /**
     * Mark processes not started yet as due for a re-init: their persisted state was built on
     * something since replaced. They warm-load it and re-init once serving (RELEASE_FIRST:
     * cold-init). Survives a failed start, so a retried install still re-inits them.
     */
    public void requireReinit(Set<String> names) {
        for (String name : names) {
            ProcessFSM fsm = fsms.get(name);
            if (fsm == null || fsm.currentState() instanceof State.Dead) {
                staleUnspawned.add(name);
            }
        }
    }

    /** Whether {@code name} is running and serving right now. */
    public boolean isServing(String name) {
        ProcessFSM fsm = fsms.get(name);
        return fsm != null && fsm.currentState() instanceof State.Serving;
    }

    // ───────────────── pause / resume ─────────────────

    /**
     * Stop the named nodes, consumers first, without retiring their state, so {@link #resume}
     * warm-loads them. A {@link LogPaused} is written first, so the pause survives a restart.
     * Queries to a paused node are rejected. The caller guarantees that every live consumer of a
     * paused node is paused as well.
     *
     * @throws LeadershipLostException if the pause cannot be persisted (the node then keeps running)
     */
    public void pause(Set<String> names, Duration perNodeTimeout) {
        Objects.requireNonNull(names, "names");
        Objects.requireNonNull(perNodeTimeout, "perNodeTimeout");
        graphSwapLock.lock();
        try {
            var order = new ArrayList<ProcessNode>(graph.topologicalOrder());
            Collections.reverse(order);
            Set<String> pausedSoFar = new TreeSet<>();
            for (ProcessNode node : order) {
                if (!names.contains(node.name())) continue;
                PausedNode already = paused.get(node.name());
                if (already == null) {
                    pauseRunning(node, perNodeTimeout, pausedSoFar);
                } else if (already.forDependency()) {
                    pauseInOwnRight(node.name(), pausedSoFar);
                }
            }
        } finally {
            graphSwapLock.unlock();
        }
    }

    /** The operator now pauses a node the engine paused for a dependency: it no longer lapses with it. */
    private void pauseInOwnRight(String name, Set<String> pausedSoFar) {
        synchronized (pausedRecord) {
            PausedNode already = paused.get(name); // a trigger may have marked it stale meanwhile
            try {
                appendRequired(new LogPaused(0, System.currentTimeMillis(), name, already.stale(), false));
            } catch (LeadershipLostException e) {
                throw partialPause(e, pausedSoFar);
            }
            paused.put(name, new PausedNode(already.sid(), already.stale(), false, already.sidRetired()));
        }
    }

    private void pauseRunning(ProcessNode node, Duration timeout, Set<String> pausedSoFar) {
        String name = node.name();
        ProcessFSM fsm = fsms.get(name);
        // A Loading node's new state is already persisted (the old one retired): resume warm-loads
        // that one and a removal retires it. Mid init, it is the last state served.
        Sid sid = fsm == null ? null
                : fsm.currentState() instanceof State.Loading && fsm.currentSid() != null
                        ? fsm.currentSid() : fsm.lastServedSid();
        // An init in progress is redone on resume; a Loading node just finishes loading. A serving
        // node is stale if its re-init has not happened yet.
        boolean stale = fsm != null && (fsm.currentState() instanceof State.Serving
                ? fsm.reinitDue()
                : !(fsm.currentState() instanceof State.Loading));
        try {
            appendRequired(new LogPaused(0, System.currentTimeMillis(), name, stale));
        } catch (LeadershipLostException e) {
            throw partialPause(e, pausedSoFar);
        }
        pausedSoFar.add(name);
        // Under KEEP_OLD the serving state stays live through a re-init; only a written LogDead retires it.
        boolean sidRetired = sid != null && fsm != null && (fsm.retired(sid)
                || (stale && strategyOf(node) == ReinitStrategy.RELEASE_FIRST
                        && !(fsm.currentState() instanceof State.Serving)));
        PausedNode recorded = new PausedNode(sid, stale, false, sidRetired);
        paused.put(name, recorded); // routing reports "paused" from here on
        failWaitingQueries(name, pausedProcess(name)); // queued before it had an FSM
        ProcessFSM removed = fsms.remove(name);
        if (removed == null) {
            notifyPaused(name, "NotPresent");
            return;
        }
        awaitStopped(removed.shutdown(timeout).toCompletableFuture(), stopDeadline(timeout), name, timeout);
        correctPausedState(name, recorded, removed);
        notifyPaused(name, removed.currentState().name());
        if (removed.reinitInterrupted()) {
            markStale(name); // a trigger that reached the FSM as it stopped re-inits on resume
        }
    }

    /**
     * Restart paused nodes dependencies first, warm-loading from {@code snapshots} where a live
     * state exists. A node that became stale while paused re-inits once serving. A node that fails
     * to start stays paused, so the call can be retried.
     */
    public void resume(Set<String> names, Function<String, RestoreSnapshot> snapshots) {
        resume(names, snapshots, name -> null);
    }

    /**
     * As {@link #resume(Set, Function)}; {@code candidates} gives a replacement persisted but not yet
     * loaded when the node was paused: it is loaded in the background once the old state serves.
     */
    public void resume(Set<String> names, Function<String, RestoreSnapshot> snapshots,
                       Function<String, RestoreSnapshot> candidates) {
        Objects.requireNonNull(names, "names");
        Objects.requireNonNull(snapshots, "snapshots");
        Objects.requireNonNull(candidates, "candidates");
        graphSwapLock.lock();
        try {
            Duration budget = startBudget();
            Set<String> resumedSoFar = new TreeSet<>();
            for (ProcessNode node : graph.topologicalOrder()) {
                PausedNode p = names.contains(node.name()) ? paused.get(node.name()) : null;
                if (p != null) resumeOne(node, p, snapshots, candidates, budget, resumedSoFar);
            }
        } finally {
            graphSwapLock.unlock();
        }
    }

    private void resumeOne(ProcessNode node, PausedNode p, Function<String, RestoreSnapshot> snapshots,
                           Function<String, RestoreSnapshot> candidates, Duration budget,
                           Set<String> resumedSoFar) {
        String name = node.name();
        RestoreSnapshot snap = snapshots.apply(name);
        boolean wasStale = staleUnspawned.contains(name);
        RestoreSnapshot candidate = snap == null ? null : candidates.apply(name);
        // RELEASE_FIRST never loads a persisted replacement beside its old state: it re-inits instead.
        boolean coldInit = snap == null || (strategyOf(node) == ReinitStrategy.RELEASE_FIRST
                && (wasStale || (candidate != null && candidate.clock() > snap.clock())));
        Sid promotedBefore = lastPromoted.get(name);
        try {
            spawnNode(node, snap, candidate, budget); // startNode cold-inits a RELEASE_FIRST one
        } catch (RuntimeException e) {
            ProcessFSM failed = fsms.remove(name);
            if (failed != null) {
                // A retry must not start a new FSM while this one is still being cancelled and cleaned up.
                Duration cleanup = configSource.get().cleanupTimeout();
                awaitStopped(failed.shutdown(cleanup).toCompletableFuture(), stopDeadline(cleanup), name, cleanup);
            }
            throw e;
        }
        PausedNode latest = recordResumed(name, coldInit || wasStale, resumedSoFar);
        resumedSoFar.add(name);
        boolean stale = p.stale() || (latest != null && latest.stale());
        Sid fresh = fsms.get(name).currentSid();
        boolean promotionCascaded = promotedBefore != null && !promotedBefore.equals(fresh);
        if (coldInit) {
            // Fresh state needs no second re-init, but reactive consumers built on the older one are stale.
            if (!promotionCascaded) cascadeFreshState(name, p, fresh);
        } else if (stale) {
            handleTrigger(name, new ReinitCause.Triggered("changed while paused"));
        }
    }

    /**
     * Write the {@link LogResumed} once the node serves again, so a crash before it brings the node
     * back paused. If the write fails or is refused, the resume is undone: a node the log still
     * calls paused must not serve.
     *
     * @return the paused record as it stood when it was dropped (a trigger may have marked it stale)
     */
    private PausedNode recordResumed(String name, boolean mustReinit, Set<String> resumedSoFar) {
        boolean persisted;
        PausedNode latest = null;
        RuntimeException appendFailed = null;
        synchronized (pausedRecord) {
            try {
                persisted = logBackend.append(new LogResumed(0, System.currentTimeMillis(), name),
                        instanceId).isPresent();
            } catch (RuntimeException e) {
                persisted = false;
                appendFailed = e;
            }
            if (persisted) latest = paused.remove(name);
        }
        // Outside the monitor: undoing waits for the node to stop.
        if (appendFailed != null) {
            undoResume(name, mustReinit);
            throw appendFailed;
        }
        if (!persisted) {
            undoResume(name, mustReinit);
            throw new LeadershipLostException("GraphMachine[" + instanceId
                    + "] lost leadership while persisting LogResumed for " + name
                    + "; it stays paused"
                    + (resumedSoFar.isEmpty() ? "" : "; already resumed: " + resumedSoFar));
        }
        return latest;
    }

    private void cascadeFreshState(String name, PausedNode p, Sid fresh) {
        long previousClock = p.sid() != null ? p.sid().clock() : fresh.clock();
        for (Edge edge : reverseDeps.getOrDefault(name, List.of())) {
            if (!edge.reactive()) continue;
            if (paused.containsKey(edge.consumer())) {
                markStale(edge.consumer());
            } else if (fsms.containsKey(edge.consumer())) {
                handleTrigger(edge.consumer(), new ReinitCause.DependencyChanged(
                        name, previousClock, fresh.clock()));
            }
        }
    }

    public boolean isPaused(String processName) {
        return paused.containsKey(processName);
    }

    public Set<String> pausedNames() {
        return new HashSet<>(paused.keySet());
    }

    /** Paused nodes the engine paused only because a dependency of theirs was paused. */
    public Set<String> pausedForDependencyNames() {
        Set<String> names = new HashSet<>();
        paused.forEach((name, p) -> {
            if (p.forDependency()) names.add(name);
        });
        return names;
    }

    /** Whether the paused node re-initialises on resume. */
    public boolean isPausedStale(String processName) {
        PausedNode p = paused.get(processName);
        return p != null && p.stale();
    }

    /** Live Sid the paused node keeps; {@code null} if it has none (never initialised, or paused mid re-init). */
    public Sid pausedSid(String processName) {
        PausedNode p = paused.get(processName);
        return p == null || p.sidRetired() ? null : p.sid();
    }

    /** A paused node has no FSM, but an observer tracking states must learn it left its last one. */
    private void notifyPaused(String name, String from) {
        try {
            observer.onStateTransition(name, from, "Paused");
        } catch (Throwable t) {
            observerThrew(name, "onStateTransition", t);
        }
    }

    /**
     * The Sid and staleness of a pause are decided before the node stops (its LogPaused goes first),
     * but an init in progress may still finish before the stop lands. Then its new state is live:
     * resume must warm-load it without a second init, and a removal must retire it instead.
     */
    private void correctPausedState(String name, PausedNode recorded, ProcessFSM stopped) {
        Sid reached = stopped.currentSid();
        if (reached == null) {
            // Stopped mid init: the recorded state matters only if the re-init already retired it.
            reached = recorded.sid();
            if (reached == null || !stopped.retired(reached)) return;
        }
        boolean retired = stopped.retired(reached);
        boolean stale = retired || (recorded.stale() && stopped.reinitInterrupted());
        if (reached.equals(recorded.sid()) && stale == recorded.stale() && retired == recorded.sidRetired()) return;
        synchronized (pausedRecord) {
            if (paused.get(name) != recorded) return; // a trigger's stale mark must stand
            paused.put(name, new PausedNode(reached, stale, recorded.forDependency(), retired));
            if (stale != recorded.stale()) {
                appendQuietly(new LogPaused(0, System.currentTimeMillis(), name, stale, recorded.forDependency()),
                        name);
            }
        }
    }

    /**
     * Record that a paused node must re-init on resume.
     *
     * @return {@code false} if the node is not paused, so the caller must deliver the re-init otherwise
     */
    private boolean markStale(String processName) {
        synchronized (pausedRecord) {
            PausedNode before = paused.get(processName);
            if (before == null) return false;
            if (before.stale()) return true;
            paused.put(processName, new PausedNode(before.sid(), true, before.forDependency(), before.sidRetired()));
            appendQuietly(new LogPaused(0, System.currentTimeMillis(), processName, true, before.forDependency()),
                    processName);
            return true;
        }
    }

    /** Whether a re-init of {@code processName} waits out its dedup window. */
    public boolean reinitPending(String processName) {
        synchronized (dedupLock) {
            return pendingCause.containsKey(processName);
        }
    }

    private void cancelPendingReinit(String processName) {
        synchronized (dedupLock) {
            ScheduledFuture<?> task = pendingTask.remove(processName);
            if (task != null) task.cancel(false);
            pendingCause.remove(processName);
            pendingCounts.remove(processName);
        }
    }

    /**
     * Retire the state of a paused node. A failure goes into {@code failures} as for a live node:
     * {@code null} when the log refused the append, else the exception.
     *
     * @return {@code false} if the state is still live in the log
     */
    private boolean retirePausedState(String name, PausedNode p, Map<String, RuntimeException> failures) {
        if (p.sid() == null || p.sidRetired()) return true;
        var dead = new LogDead(0, System.currentTimeMillis(), p.sid());
        try {
            if (logBackend.append(dead, instanceId).isEmpty()) {
                log.error("GraphMachine[{}] lost leadership while retiring the state of {}", instanceId, name);
                failures.put(name, null);
                return false;
            }
            return true;
        } catch (RuntimeException e) {
            log.error("GraphMachine[{}] could not retire the state of {}: {}", instanceId, name, e.toString());
            failures.put(name, e);
            return false;
        }
    }

    /**
     * Stop a node whose resume could not be persisted, leaving it paused as it was. Waits for the
     * cleanup, so a retried resume does not load a second {@code Process} beside this one.
     */
    private void undoResume(String name, boolean wasStale) {
        ProcessFSM started = fsms.remove(name);
        if (started == null) return;
        Duration timeout = configSource.get().cleanupTimeout();
        try {
            started.shutdown(timeout).toCompletableFuture().get(timeout.toMillis() + 500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("GraphMachine[{}] could not stop {} after a refused resume: {}",
                    instanceId, name, e.toString());
        }
        // startNode consumed the flag; the node must still re-init when the resume is retried.
        if (wasStale) staleUnspawned.add(name);
    }

    /** That state is still live, so a later start would warm-load it: the change is only half applied. */
    private RuntimeException refusedRetireFailure(Map<String, RuntimeException> retireFailures) {
        String head = "GraphMachine[" + instanceId + "] applied the graph change but could not retire"
                + " the state of " + retireFailures.keySet();
        // Only a refused append means a takeover; any other failure may be transient, and advising a
        // takeover would send an operator down the wrong path.
        RuntimeException other = null;
        boolean everWrote = false;
        for (RuntimeException cause : retireFailures.values()) {
            if (cause == null || cause instanceof LeadershipLostException) {
                var lost = new LeadershipLostException(head + ": no longer the leader of the log");
                if (cause != null) lost.initCause(cause);
                return lost;
            }
            if (other == null) other = cause;
            everWrote |= !(cause instanceof ShutdownNotFinished);
        }
        return new IllegalStateException(head
                + (everWrote ? ": the log write failed" : ": the shutdown did not finish, so it never tried"),
                other);
    }

    /** Each node is paused by its own record; say which already are, since this instance cannot undo them. */
    private LeadershipLostException partialPause(LeadershipLostException cause, Set<String> pausedSoFar) {
        return new LeadershipLostException(cause.getMessage()
                + (pausedSoFar.isEmpty() ? "; nothing was paused" : "; already paused: " + pausedSoFar));
    }

    private void appendRequired(LogEvent event) {
        if (logBackend.append(event, instanceId).isEmpty()) {
            throw new LeadershipLostException("GraphMachine[" + instanceId
                    + "] lost leadership while persisting " + event.getClass().getSimpleName());
        }
    }

    private void appendQuietly(LogEvent event, String processName) {
        try {
            if (logBackend.append(event, instanceId).isEmpty()) {
                log.error("GraphMachine[{}] lost leadership while persisting {} for {}",
                        instanceId, event.getClass().getSimpleName(), processName);
            }
        } catch (RuntimeException e) {
            log.error("GraphMachine[{}] could not persist {} for {}; a restart may not reflect it: {}",
                    instanceId, event.getClass().getSimpleName(), processName, e.toString());
        }
    }

    private static long stopDeadline(Duration timeout) {
        return System.nanoTime() + timeout.toNanos() + STOP_GRACE_NANOS;
    }

    /**
     * Wait for a node to stop. An interrupt does not cut the wait short (it is restored at the end):
     * dependencies would be torn down under running consumers, or pause would return early.
     *
     * @return whether it stopped before the deadline
     */
    private boolean awaitStopped(CompletableFuture<?> stopping, long deadlineNanos, String name, Duration budget) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    stopping.get(Math.max(1, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
                    return true;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (TimeoutException | ExecutionException e) {
                    // Usually a cleanUp over its time; the state is already retired, since LogDead comes first.
                    log.warn("GraphMachine[{}] {} did not finish stopping within its cleanup budget of {}: {}",
                            instanceId, name, budget, describeCauses(e));
                    return false;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /** A throwable with its cause chain: an {@code ExecutionException} alone says nothing. */
    private static String describeCauses(Throwable t) {
        var text = new StringBuilder();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable current = t; current != null && seen.add(current); current = current.getCause()) {
            if (text.length() > 0) text.append(" <- ");
            text.append(current);
        }
        return text.toString();
    }

    /** The retire was never attempted: the shutdown ran out of time first. */
    private static final class ShutdownNotFinished extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        ShutdownNotFinished(String message) {
            super(message);
        }
    }

    private static CompletionStage<Object> rejected(RuntimeException e) {
        var failed = new CompletableFuture<Object>();
        failed.completeExceptionally(e);
        return failed;
    }

    /**
     * @param retireFailures collects each process whose state could not be retired, mapped to the
     *                       exception, or to {@code null} when the log refused the append
     */
    private void shutdownAndRemove(String name, Duration timeout, Map<String, RuntimeException> retireFailures) {
        ProcessFSM fsm = fsms.remove(name);
        if (fsm == null) return;
        boolean stopped = awaitStopped(fsm.shutdownReplace(timeout).toCompletableFuture(),
                stopDeadline(timeout), name, timeout);
        Sid orphan = fsm.unretiredOwnSid();
        if (orphan != null) {
            // A dead FSM cannot write any more; unretired, a restart would warm-load the state.
            retireOrphan(name, orphan, retireFailures);
            return;
        }
        if (fsm.retireOutcome() == ProcessFSM.RetireOutcome.FAILED) {
            retireFailures.put(name, fsm.retireFailureCause());
        } else if (!stopped && fsm.retireOutcome() == ProcessFSM.RetireOutcome.UNKNOWN) {
            // The wait ran out before the FSM got to the retire. A slow cleanUp is not this case:
            // LogDead is written before the cleanup starts.
            retireFailures.put(name, new ShutdownNotFinished(
                    "the shutdown of " + name + " did not finish within " + timeout
                            + ", and it had not recorded the state as retired"));
        }
    }

    private void retireOrphan(String name, Sid orphan, Map<String, RuntimeException> retireFailures) {
        var dead = new LogDead(0, System.currentTimeMillis(), orphan);
        try {
            if (logBackend.append(dead, instanceId).isEmpty()) {
                log.error("GraphMachine[{}] lost leadership while retiring {} of the dead {}",
                        instanceId, orphan, name);
                retireFailures.put(name, null);
            } else {
                log.info("GraphMachine[{}] retired {}, left behind by the dead {}", instanceId, orphan, name);
            }
        } catch (RuntimeException e) {
            log.error("GraphMachine[{}] could not retire {} of the dead {}: {}",
                    instanceId, orphan, name, e.toString());
            retireFailures.put(name, e);
        }
    }

    // ───────────────── starting nodes ─────────────────

    private Duration startBudget() {
        EngineConfig config = configSource.get();
        return config.initTimeout().plus(config.loadTimeout());
    }

    private void spawnNode(ProcessNode node, RestoreSnapshot snap, RestoreSnapshot candidate, Duration startBudget) {
        awaitingStart.add(node.name());
        try {
            ProcessFSM fsm = startNode(node, snap, candidate);
            awaitServing(fsm, System.nanoTime() + startBudget.toNanos(), startBudget);
        } finally {
            awaitingStart.remove(node.name());
        }
    }

    /** Whether a control operation started {@code processName} and is waiting for it to serve. */
    public boolean isAwaitingStart(String processName) {
        return awaitingStart.contains(processName);
    }

    private ReinitStrategy strategyOf(ProcessNode node) {
        return node.reinitStrategy() != null ? node.reinitStrategy() : configSource.get().reinitStrategy();
    }

    /**
     * Create and start the FSM of {@code node}: warm-load {@code snap}, or cold-init. A
     * {@code candidate} (a replacement of {@code snap} persisted but never loaded) is loaded in the
     * background once {@code snap} serves.
     */
    private ProcessFSM startNode(ProcessNode node, RestoreSnapshot snap, RestoreSnapshot candidate) {
        String name = node.name();
        ProcessFSM fsm = new ProcessFSM(name, node, configSource, logBackend, instanceId, scheduler, this, observer);
        fsm.setSidPromotionListener(this::onSidPromotion);
        fsm.setQueryReparker(q -> reparkQuery(name, q));
        fsm.shareObserverFailures(observerFailuresByProcess.computeIfAbsent(name, k -> new ConcurrentHashMap<>()));
        List<Envelope.Query> waiting;
        boolean reinitOnceServing = false;
        synchronized (waitingForFsm) { // against a trigger or query that finds no FSM yet
            if (snap != null && candidate != null && candidate.clock() > snap.clock()
                    && strategyOf(node) == ReinitStrategy.RELEASE_FIRST) {
                log.info("GraphMachine[{}] {} has an unfinished re-init; cold-initialising (RELEASE_FIRST)",
                        instanceId, name);
                staleUnspawned.remove(name);
                snap = null;
                candidate = null;
            }
            if (staleUnspawned.remove(name) && snap != null) {
                if (strategyOf(node) == ReinitStrategy.RELEASE_FIRST) {
                    log.info("GraphMachine[{}] {} was triggered, or depends on a producer that changed, before it "
                            + "started; cold-initialising", instanceId, name);
                    snap = null;
                    candidate = null;
                } else {
                    log.info("GraphMachine[{}] {} was triggered, or depends on a producer that changed, before it "
                            + "started; it serves its persisted state and re-initialises in the background",
                            instanceId, name);
                    reinitOnceServing = true;
                }
            }
            fsms.put(name, fsm);
            waiting = waitingForFsm.remove(name);
        }
        // A start that overran its budget (flag restored) and then served anyway must not leave the
        // flag behind to force a surprise cold init on a later pause/resume.
        fsm.servingReady().thenRun(() -> {
            if (fsms.get(name) == fsm) staleUnspawned.remove(name);
        });
        if (reinitOnceServing) {
            fsm.servingReady().thenRun(() -> {
                if (fsms.get(name) != fsm) return;
                try {
                    handleTrigger(name, new ReinitCause.Triggered("changed before it started"));
                } catch (RuntimeException e) {
                    log.debug("GraphMachine[{}] re-init of {} after its start dropped: {}", instanceId, name,
                            e.toString());
                }
            });
        }
        logStart(name, snap);
        if (snap != null) {
            fsm.spawnLoad(snap.clock(), snap.properties());
            if (candidate != null && candidate.clock() > snap.clock()) {
                fsm.replaceFrom(candidate.clock(), candidate.properties());
            }
        } else {
            fsm.spawnInit();
        }
        if (waiting != null) {
            for (Envelope.Query w : waiting) {
                if (w.reply().isDone()) continue;
                fsm.submitAnnouncedQuery(w); // same id and reply: announced once, reported once
            }
        }
        return fsm;
    }

    /** Runs on a starter or the caller's thread: carries the MDC of the process's own log lines. */
    private void logStart(String name, RestoreSnapshot snap) {
        String previousEngine = MDC.get(ProcessFSM.MDC_ENGINE);
        String previousProcess = MDC.get(ProcessFSM.MDC_PROCESS);
        MDC.put(ProcessFSM.MDC_ENGINE, instanceId);
        MDC.put(ProcessFSM.MDC_PROCESS, name);
        try {
            if (snap != null) {
                log.info("GraphMachine[{}] warm start for {} from clock {}", instanceId, name, snap.clock());
            } else {
                log.info("GraphMachine[{}] cold start for {}", instanceId, name);
            }
        } finally {
            restoreMdc(ProcessFSM.MDC_ENGINE, previousEngine);
            restoreMdc(ProcessFSM.MDC_PROCESS, previousProcess);
        }
    }

    private static void restoreMdc(String key, String previous) {
        if (previous == null) MDC.remove(key);
        else MDC.put(key, previous);
    }

    /** Wait until {@code fsm} serves; gives up at the deadline, on terminal failure, or on {@link #abortStartup()}. */
    private void awaitServing(ProcessFSM fsm, long deadlineNanos, Duration budget) {
        String name = fsm.processName();
        while (true) {
            if (startupAborted) {
                throw new IllegalStateException("Engine closed while '" + name + "' was starting");
            }
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw new RuntimeException(notServingMessage(name, budget, fsm));
            }
            try {
                fsm.servingReady().toCompletableFuture()
                        .get(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS);
                return;
            } catch (TimeoutException e) {
                // poll the deadline and the abort flag again
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for '" + name
                        + "' to start; the call can be retried", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                if (cause instanceof RuntimeException re) throw re;
                throw new RuntimeException("Node '" + name + "' failed to start", cause);
            }
        }
    }

    /** The state tells whether the node is still working (Initializing/Loading) or gave up. */
    private static String notServingMessage(String name, Duration budget, ProcessFSM fsm) {
        String last = fsm != null ? fsm.lastFailure() : null;
        return "Node '" + name + "' did not reach Serving within " + budget
                + (fsm != null ? " (state=" + fsm.currentState().name() + ")" : "")
                + (last != null ? "; last failure: " + last : "");
    }

    /**
     * A callback about one process counts against that process's throttle, shared with its FSMs.
     * After the process has left the graph it counts in a throwaway map, so as not to bring back
     * the entry its removal dropped.
     */
    private void observerThrew(String processName, String callback, Throwable t) {
        Map<String, AtomicLong> counts = graph.nodes().containsKey(processName)
                ? observerFailuresByProcess.computeIfAbsent(processName, k -> new ConcurrentHashMap<>())
                : new ConcurrentHashMap<>();
        long n = counts.computeIfAbsent(callback, k -> new AtomicLong()).incrementAndGet();
        if (Long.bitCount(n) == 1) {
            log.warn("GraphMachine[{}] observer.{} threw for {} (failure #{} of this callback for it; repeats are"
                    + " logged at doubling intervals): {}", instanceId, callback, processName, n, t.toString());
        } else {
            log.debug("GraphMachine[{}] observer.{} threw for {} (failure #{}): {}",
                    instanceId, callback, processName, n, t.toString());
        }
    }

    /** WARN on the 1st, 2nd, 4th, 8th… failure of each observer callback, DEBUG otherwise. */
    private void observerThrew(String callback, Throwable t) {
        long n = observerFailures.computeIfAbsent(callback, k -> new AtomicLong()).incrementAndGet();
        if (Long.bitCount(n) == 1) {
            log.warn("GraphMachine[{}] observer.{} threw (failure #{}; repeats are logged at doubling intervals): {}",
                    instanceId, callback, n, t.toString());
        } else {
            log.debug("GraphMachine[{}] observer.{} threw (failure #{}): {}", instanceId, callback, n, t.toString());
        }
    }

    /** Make pending and future waits for nodes to serve fail at once (the engine is closing). */
    public void abortStartup() {
        startupAborted = true;
    }

    private static Map<String, List<Edge>> buildReverseDeps(Graph g) {
        Map<String, List<Edge>> result = new HashMap<>();
        for (ProcessNode consumer : g.nodes().values()) {
            for (Dependency dep : consumer.dependencies()) {
                boolean reactive = dep instanceof Dependency.Reactive;
                result.computeIfAbsent(dep.name(), k -> new ArrayList<>())
                        .add(new Edge(consumer.name(), reactive));
            }
        }
        return result;
    }

    public void startAll() {
        startAll(Map.of(), Set.of());
    }

    /**
     * Start every node except those in {@code pausedAtStart} (name → stale flag), which come up
     * paused with their persisted state kept for {@link #resume}; those also in
     * {@code pausedForDependency} are paused only because a dependency is. A node starts once its
     * own dependencies serve, so independent nodes start concurrently and a failure holds back only
     * its dependents. Returns once every node has settled; if any failed, throws the failure of the
     * first one (in topological order) whose dependencies did serve.
     */
    public void startAll(Map<String, Boolean> pausedAtStart, Set<String> pausedForDependency) {
        startAll(pausedAtStart, pausedForDependency, name -> null);
    }

    /**
     * As {@link #startAll(Map, Set)}; {@code candidates} gives each node's replacement persisted
     * before the restart and never loaded: it is loaded in the background once the node serves.
     */
    public void startAll(Map<String, Boolean> pausedAtStart, Set<String> pausedForDependency,
                         Function<String, RestoreSnapshot> candidates) {
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(pausedAtStart, "pausedAtStart");
        Objects.requireNonNull(pausedForDependency, "pausedForDependency");
        Duration perNodeBudget = startBudget();
        List<ProcessNode> order = graph.topologicalOrder();
        for (ProcessNode node : order) {
            Boolean stale = pausedAtStart.get(node.name());
            if (stale == null) continue;
            RestoreSnapshot snap = restoreLookup.apply(node.name());
            paused.put(node.name(), new PausedNode(snap != null ? new Sid(node.name(), snap.clock()) : null, stale,
                    pausedForDependency.contains(node.name())));
            log.info("GraphMachine[{}] {} stays paused", instanceId, node.name());
        }
        // A paused node counts as settled: its consumers are paused too, by invariant.
        startByDependencies(order, name -> !pausedAtStart.containsKey(name), restoreLookup, candidates, perNodeBudget);
    }

    /**
     * Start the nodes of {@code order} that {@code toStart} accepts, each once all its dependencies
     * serve; a rejected node counts as settled. Returns once everything has settled; if anything
     * failed, throws the failure of the first node (in topological order) whose dependencies served.
     * Shared by the initial install and every graph change.
     */
    private void startByDependencies(List<ProcessNode> order, Predicate<String> toStart,
                                     Function<String, RestoreSnapshot> snapshotFor,
                                     Function<String, RestoreSnapshot> candidateFor, Duration perNodeBudget) {
        Map<String, CompletableFuture<Void>> serving = new HashMap<>();
        // Set when the caller is interrupted: nodes not started yet are left for a retry.
        var stopStarting = new AtomicBoolean();
        try (var starter = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("fom-start-", 0).factory())) {
            for (ProcessNode node : order) {
                String name = node.name();
                if (!toStart.test(name)) {
                    serving.put(name, CompletableFuture.completedFuture(null));
                    continue;
                }
                CompletableFuture<?>[] dependencies = node.dependencies().stream()
                        .map(dep -> serving.get(dep.name()))
                        .toArray(CompletableFuture<?>[]::new);
                serving.put(name, CompletableFuture.allOf(dependencies).thenComposeAsync(
                        ignored -> startOnceReady(node, snapshotFor, candidateFor, perNodeBudget, stopStarting),
                        starter));
            }
            awaitStartup(order, serving, perNodeBudget, stopStarting);
        }
    }

    private CompletableFuture<Void> startOnceReady(ProcessNode node, Function<String, RestoreSnapshot> snapshotFor,
                                                   Function<String, RestoreSnapshot> candidateFor,
                                                   Duration perNodeBudget, AtomicBoolean stopStarting) {
        String name = node.name();
        if (startupAborted) {
            throw new IllegalStateException("Engine closed while '" + name + "' was waiting to start");
        }
        if (stopStarting.get()) {
            throw new NotStartedException(name);
        }
        awaitingStart.add(name);
        // startNode consumes the "must re-init" flag. If the start fails it comes back, or a retry
        // would warm-load the very state it was meant to replace.
        boolean wasStale = staleUnspawned.contains(name);
        ProcessFSM fsm;
        try {
            fsm = startNode(node, snapshotFor.apply(name), candidateFor.apply(name));
        } catch (RuntimeException | Error e) {
            awaitingStart.remove(name);
            if (wasStale) staleUnspawned.add(name);
            throw e;
        }
        return fsm.servingReady().toCompletableFuture().copy()
                .orTimeout(perNodeBudget.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((ok, failure) -> {
                    awaitingStart.remove(name);
                    if (failure != null && wasStale) staleUnspawned.add(name);
                });
    }

    /** A node that was not started because the caller was interrupted; a retry starts it. */
    private static final class NotStartedException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        NotStartedException(String name) {
            super("'" + name + "' was not started: the caller was interrupted");
        }
    }

    private void awaitStartup(List<ProcessNode> order, Map<String, CompletableFuture<Void>> serving, Duration budget,
                              AtomicBoolean stopStarting) {
        CompletableFuture<Void> all = CompletableFuture.allOf(serving.values().toArray(CompletableFuture<?>[]::new));
        boolean interrupted = awaitSettled(order, serving, all, budget, stopStarting);
        if (interrupted) {
            List<String> notStarted = order.stream().map(ProcessNode::name)
                    .filter(name -> !fsms.containsKey(name) && !paused.containsKey(name))
                    .toList();
            Thread.currentThread().interrupt();
            if (!notStarted.isEmpty()) {
                throw new IllegalStateException("Interrupted while starting the graph; not started,"
                        + " a retried newGraph starts them: " + notStarted);
            }
            if (all.isDone() && !all.isCompletedExceptionally()) return;
            // otherwise a node failed on its own: report it below, with the flag still set
        } else if (!all.isCompletedExceptionally()) {
            return;
        }
        logBlockedNodes(order, serving);
        throwFirstFailure(order, serving, budget);
    }

    /**
     * Wait until every start has settled. On an interrupt, start nothing new but let what is
     * already starting finish, bounded by one start budget.
     *
     * @return whether the caller was interrupted
     */
    private boolean awaitSettled(List<ProcessNode> order, Map<String, CompletableFuture<Void>> serving,
                                 CompletableFuture<Void> all, Duration budget, AtomicBoolean stopStarting) {
        boolean interrupted = false;
        long interruptedDeadline = 0;
        while (true) {
            if (startupAborted) {
                String pending = order.stream()
                        .map(ProcessNode::name)
                        .filter(name -> !serving.get(name).isDone())
                        .findFirst()
                        .orElse(null);
                if (interrupted) Thread.currentThread().interrupt();
                throw new IllegalStateException(pending != null
                        ? "Engine closed while '" + pending + "' was starting"
                        : "Engine closed while the graph was starting");
            }
            if (interrupted && System.nanoTime() - interruptedDeadline > 0) return true;
            try {
                all.get(50, TimeUnit.MILLISECONDS);
                return interrupted;
            } catch (TimeoutException e) {
                // poll the abort flag again
            } catch (InterruptedException e) {
                if (!interrupted) {
                    interrupted = true;
                    stopStarting.set(true);
                    interruptedDeadline = System.nanoTime() + budget.toNanos();
                }
            } catch (ExecutionException e) {
                return interrupted; // settled, with a failure
            }
        }
    }

    /** The exception shows only the root failure; say which failed dependency held back each node. */
    private void logBlockedNodes(List<ProcessNode> order, Map<String, CompletableFuture<Void>> serving) {
        for (ProcessNode node : order) {
            if (!serving.get(node.name()).isCompletedExceptionally()) continue;
            node.dependencies().stream()
                    .map(Dependency::name)
                    .filter(dep -> serving.get(dep).isCompletedExceptionally())
                    .findFirst()
                    .ifPresent(dep -> log.warn("GraphMachine[{}] not starting {}: its dependency {} did not start",
                            instanceId, node.name(), dep));
        }
    }

    private void throwFirstFailure(List<ProcessNode> order, Map<String, CompletableFuture<Void>> serving,
                                   Duration budget) {
        for (ProcessNode node : order) {
            CompletableFuture<Void> started = serving.get(node.name());
            if (!started.isCompletedExceptionally()) continue;
            boolean dependenciesServed = node.dependencies().stream()
                    .noneMatch(dep -> serving.get(dep.name()).isCompletedExceptionally());
            if (!dependenciesServed) continue;
            Throwable cause;
            try {
                started.join();
                continue;
            } catch (CompletionException e) {
                cause = e.getCause() != null ? e.getCause() : e;
            } catch (CancellationException e) {
                cause = e;
            }
            if (cause instanceof TimeoutException) {
                throw new RuntimeException(notServingMessage(node.name(), budget, fsms.get(node.name())), cause);
            }
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException("Node '" + node.name() + "' failed to start", cause);
        }
    }

    /**
     * Start a {@code Dead} process again without a graph swap. Its FSM is replaced at once, so
     * queries wait for it instead of being rejected; it warm-loads {@code state} if that has an entry
     * for it. Once it serves, its reactive consumers re-init if its Sid changed, and nodes that never
     * started because they depend on it are started.
     *
     * @return {@code false} if the process is not Dead, or a graph swap holds the machine
     */
    public boolean restartDead(String processName,
                               Function<Graph, Function<String, RestoreSnapshot>> stateFor) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(stateFor, "stateFor");
        if (!graphSwapLock.tryLock()) return false;
        ProcessFSM fresh;
        try {
            ProcessFSM old = fsms.get(processName);
            ProcessNode node = graph.nodes().get(processName);
            if (old == null || node == null || !(old.currentState() instanceof State.Dead)) return false;
            RestoreSnapshot snap = stateFor.apply(graph).apply(processName);
            fsms.remove(processName);
            fresh = startNode(node, snap, null);
        } finally {
            graphSwapLock.unlock();
        }
        Duration budget = startBudget();
        Thread.ofVirtual().name("fom-restart-" + processName).start(() -> {
            try {
                awaitServing(fresh, System.nanoTime() + budget.toNanos(), budget);
                startMissing(stateFor);
            } catch (RuntimeException e) {
                if (fsms.get(processName) != fresh) {
                    // A swap or another restart replaced it and owns the outcome.
                    log.debug("GraphMachine[{}] restart of {} was superseded: {}", instanceId, processName, e.toString());
                } else {
                    log.warn("GraphMachine[{}] restarting {} failed: {}", instanceId, processName, e.toString());
                }
            }
        });
        return true;
    }

    /** Start nodes that have no FSM (a dependency failed when they were due) once all their dependencies serve. */
    private void startMissing(Function<Graph, Function<String, RestoreSnapshot>> stateFor) {
        Duration budget = startBudget();
        graphSwapLock.lock();
        try {
            Function<String, RestoreSnapshot> state = null;
            for (ProcessNode node : graph.topologicalOrder()) {
                String name = node.name();
                if (fsms.containsKey(name) || paused.containsKey(name)) continue;
                boolean dependenciesServe = node.dependencies().stream().allMatch(dep -> {
                    ProcessFSM fsm = fsms.get(dep.name());
                    return fsm != null && fsm.currentState() instanceof State.Serving;
                });
                if (!dependenciesServe) continue;
                if (state == null) state = stateFor.apply(graph);
                try {
                    spawnNode(node, state.apply(name), null, budget);
                } catch (RuntimeException e) {
                    log.warn("GraphMachine[{}] starting {} failed: {}", instanceId, name, e.toString());
                }
            }
        } finally {
            graphSwapLock.unlock();
        }
    }

    /** The graph this machine runs now (a swap installs it before starting its nodes). */
    public Graph graph() {
        return graph;
    }

    /** Names of the nodes of the graph this machine runs now. */
    public Set<String> graphNodeNames() {
        return new HashSet<>(graph.nodes().keySet());
    }

    /** Graph nodes that are neither running nor paused: being (re)started, or waiting for a failed dependency. */
    public Set<String> startingNames() {
        Set<String> names = new HashSet<>(graph.nodes().keySet());
        names.removeAll(fsms.keySet());
        names.removeAll(paused.keySet());
        return names;
    }

    // ───────────────── triggers and cascade ─────────────────

    /**
     * Schedule a re-init of {@code processName} after the dedup window
     * ({@link EngineConfig#dedupWindow}); calls within the window collapse into one.
     */
    public void handleTrigger(String processName, ReinitCause cause) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(cause, "cause");
        if (!fsms.containsKey(processName)) {
            if (markStale(processName)) return; // paused: re-inits on resume
            if (!graph.nodes().containsKey(processName)) {
                throw new IllegalArgumentException(unknownProcess(processName));
            }
            synchronized (waitingForFsm) {
                if (!fsms.containsKey(processName)) {
                    staleUnspawned.add(processName);
                    log.debug(strategyOf(graph.nodes().get(processName)) == ReinitStrategy.RELEASE_FIRST
                                    ? "GraphMachine[{}] trigger for {} while it is being started: it will cold-init"
                                    : "GraphMachine[{}] trigger for {} while it is being started: it re-initialises "
                                            + "once serving",
                            instanceId, processName);
                    return;
                }
            }
        }
        synchronized (dedupLock) {
            pendingCause.merge(processName, cause, (existing, fresh) -> existing);
            pendingCounts.merge(processName, 1, Integer::sum);
            if (pendingTask.containsKey(processName)) {
                return;
            }
            long delayMs = configSource.get().dedupWindow().toMillis();
            ScheduledFuture<?> task;
            try {
                task = scheduler.schedule(() -> fireDedup(processName), delayMs, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException closing) {
                // An Engine.trigger already has its LogTrigger, which the next start replays.
                pendingCause.remove(processName);
                pendingCounts.remove(processName);
                log.debug("GraphMachine[{}] trigger for {} arrived as the engine closed; the next start applies it",
                        instanceId, processName);
                return;
            }
            pendingTask.put(processName, task);
        }
    }

    private void fireDedup(String processName) {
        ReinitCause cause;
        int collapsed;
        synchronized (dedupLock) {
            cause = pendingCause.remove(processName);
            Integer count = pendingCounts.remove(processName);
            collapsed = count == null ? 0 : count;
            pendingTask.remove(processName);
        }
        if (cause == null) return; // cancelled: its process was removed
        ProcessFSM fsm = fsms.get(processName);
        if (fsm == null) {
            markStale(processName); // paused before the window closed
            return;
        }
        if (collapsed > 1) {
            try {
                observer.onDedupCollapsed(processName, collapsed);
            } catch (Throwable t) { // must never drop the re-init
                observerThrew("onDedupCollapsed", t);
            }
        }
        fsm.submitReinit(cause);
        ProcessFSM current = fsms.get(processName);
        if (current == fsm) return;
        // Paused or replaced while the request was on its way: don't lose it.
        if (current == null) {
            markStale(processName);
        } else {
            current.submitReinit(cause); // e.g. a Dead process restarted with its old state
        }
    }

    /** Runs after every Sid promotion. */
    private void onSidPromotion(String processName, Sid previousSid, Sid newSid) {
        Sid promotedBefore = lastPromoted.put(processName, newSid);
        if (previousSid == null && promotedBefore != null) {
            // A fresh FSM of a process served before: the change is relative to the Sid it had.
            if (promotedBefore.equals(newSid)) return; // warm-loaded the same state
            previousSid = promotedBefore;
        }
        try {
            observer.onSidPromotion(processName, previousSid, newSid);
        } catch (Throwable t) { // must never stop the cascade
            observerThrew("onSidPromotion", t);
        }
        if (previousSid == null) return; // first init: consumers start on this state
        List<String> running = new ArrayList<>();
        for (Edge edge : reverseDeps.getOrDefault(processName, List.of())) {
            if (!edge.reactive()) continue;
            if (fsms.get(edge.consumer()) == null) {
                // Synchronously, so a start waiting on this producer cannot warm-load the consumer first.
                if (!markStale(edge.consumer())) {
                    staleUnspawned.add(edge.consumer());
                }
                continue;
            }
            running.add(edge.consumer());
        }
        if (running.isEmpty()) return;
        if (shuttingDown) return; // a restart re-inits consumers built on an older producer state anyway
        // Off the producer's dispatcher: a log append per consumer would stall its queries.
        Sid previous = previousSid;
        Thread cascade = Thread.ofVirtual().name("fom-cascade-" + processName).unstarted(() -> {
            try {
                for (String consumer : running) {
                    if (shuttingDown) return; // see shutdownAll
                    cascadeTo(consumer, processName, previous, newSid);
                }
            } finally {
                cascades.remove(Thread.currentThread());
            }
        });
        cascades.add(cascade);
        cascade.start();
    }

    private void cascadeTo(String consumer, String processName, Sid previousSid, Sid newSid) {
        ProcessFSM consumerFsm = fsms.get(consumer);
        // Only a live Sid gets the marker; a consumer mid-init still gets the trigger. An init after
        // failed loads still holds the Sid it could not load, already retired.
        Sid consumerSid = consumerFsm == null ? null : consumerFsm.currentSid();
        if (consumerSid != null && consumerFsm.retired(consumerSid)) consumerSid = null;
        if (consumerSid != null) {
            try {
                if (logBackend.append(new LogDependencyChanged(
                        0, System.currentTimeMillis(), consumerSid, processName,
                        previousSid.clock(), newSid.clock()), instanceId).isEmpty()) {
                    log.warn("GraphMachine[{}] lost leadership while persisting "
                            + "LogDependencyChanged for {}", instanceId, consumer);
                }
            } catch (RuntimeException e) {
                log.warn("GraphMachine[{}] could not persist LogDependencyChanged for {}: {}",
                        instanceId, consumer, e.toString());
            }
        }
        try {
            handleTrigger(consumer, new ReinitCause.DependencyChanged(
                    processName, previousSid.clock(), newSid.clock()));
        } catch (IllegalArgumentException removedMeanwhile) {
            // Go on: the consumers after it in the list still need their re-init.
            log.debug("GraphMachine[{}] cascade skipped {}: {}", instanceId, consumer,
                    removedMeanwhile.getMessage());
        }
    }

    // ───────────────── queries ─────────────────

    @Override
    public CompletionStage<Object> route(String processName, Object query, long deadlineEpochMillis,
                                         UUID parentQueryId) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(query, "query");
        return submitOrWait(processName, query, deadlineEpochMillis, parentQueryId, true);
    }

    public ProcessFSM fsm(String processName) {
        return fsms.get(processName);
    }

    /** Whether {@code processName} is part of the graph: running, paused, or being started. */
    public boolean knows(String processName) {
        return fsms.containsKey(processName) || paused.containsKey(processName)
                || graph.nodes().containsKey(processName);
    }

    /** Whether the process's FSM has terminated ({@code Dead}). */
    public boolean isDead(String processName) {
        ProcessFSM fsm = fsms.get(processName);
        return fsm != null && fsm.currentState() instanceof State.Dead;
    }

    public boolean contains(String processName) {
        return fsms.containsKey(processName);
    }

    public Set<String> processNames() {
        return new HashSet<>(fsms.keySet());
    }

    public CompletionStage<Object> submitQuery(String processName, Object query, long deadlineEpochMillis,
                                               UUID parentQueryId) {
        return submitOrWait(processName, query, deadlineEpochMillis, parentQueryId, false);
    }

    private static String unknownProcess(String name) {
        return "Unknown process: '" + name + "'";
    }

    private static String pausedProcess(String name) {
        return "Process '" + name + "' is paused";
    }

    /**
     * Submit to the process's FSM. A graph node with no FSM right now (starting, or mid swap) gets
     * the query once its FSM exists, the way an initialising process stashes it.
     */
    private CompletionStage<Object> submitOrWait(String processName, Object query, long deadlineEpochMillis,
                                                 UUID parentQueryId, boolean nested) {
        ProcessFSM fsm;
        Envelope.Query parked = null;
        synchronized (waitingForFsm) {
            fsm = fsms.get(processName);
            if (fsm == null) {
                if (paused.containsKey(processName)) {
                    return rejected(new QueryRejectedException(pausedProcess(processName)));
                }
                if (!graph.nodes().containsKey(processName)) {
                    String message = unknownProcess(processName);
                    return rejected(nested ? new QueryException(message) : new QueryRejectedException(message));
                }
                parked = new Envelope.Query(UUID.randomUUID(), query, new CompletableFuture<>(), deadlineEpochMillis);
                waitingForFsm.computeIfAbsent(processName, k -> new ArrayList<>()).add(parked);
            }
        }
        if (fsm != null) {
            CompletionStage<Object> reply = fsm.submitQuery(query, deadlineEpochMillis, parentQueryId);
            ProcessFSM.linkToIssuingQuery(reply.toCompletableFuture());
            return reply;
        }
        // Outside the monitor: observer callbacks are user code, and completing the reply runs its hooks.
        reportParkedQuery(processName, parked, query.getClass(), parentQueryId);
        watchParkedQuery(processName, parked);
        ProcessFSM.linkToIssuingQuery(parked.reply());
        return parked.reply();
    }

    /**
     * Announced here rather than by the FSM that eventually serves it, so a query that only ever
     * times out waiting is still visible. The FSM takes it over with its id, so it counts once.
     */
    private void reportParkedQuery(String processName, Envelope.Query parked, Class<?> messageType,
                                   UUID parentQueryId) {
        UUID queryId = parked.queryId();
        Instant sentAt = Instant.now();
        safeOnQuerySent(processName, queryId, messageType, parentQueryId);
        if (observer == EngineObserver.NOOP) return;
        // On an engine thread, never the caller's: see ProcessFSM.submitQuery.
        parked.reply().whenCompleteAsync((result, failure) -> {
            if (failure == null) {
                safeOnQueryCompleted(processName, queryId, Duration.between(sentAt, Instant.now()));
            } else {
                Throwable cause = unwrap(failure);
                safeOnQueryFailed(processName, queryId, failureReason(cause), cause);
            }
        }, ProcessFSM.OBSERVER_THREADS);
    }

    /**
     * Drop a parked query from the queue however its reply settles, and fail it at its deadline
     * (the node it waits for may never come).
     */
    private void watchParkedQuery(String processName, Envelope.Query parked) {
        CompletableFuture<Object> reply = parked.reply();
        reply.whenComplete((r, e) -> {
            synchronized (waitingForFsm) {
                List<Envelope.Query> list = waitingForFsm.get(processName);
                if (list != null) {
                    list.removeIf(w -> w.reply() == reply);
                    if (list.isEmpty()) waitingForFsm.remove(processName);
                }
            }
        });
        // 0: no deadline (a query issued during init or load, see QueryableContextImpl.forPhase).
        if (parked.deadlineEpochMillis() <= 0) return;
        long delay = parked.deadlineEpochMillis() - System.currentTimeMillis();
        if (delay >= TimeUnit.DAYS.toMillis(365)) return; // effectively no deadline
        if (delay <= 0) {
            reply.completeExceptionally(parkedDeadline(processName));
            return;
        }
        try {
            // Completing the reply runs the caller's continuations, so not on the scheduler thread;
            // and not on the common pool, which user code may block (see QueryDeadlines).
            ScheduledFuture<?> timeout = scheduler.schedule(
                    () -> Thread.ofVirtual().name("fom-query-deadline").start(
                            () -> reply.completeExceptionally(parkedDeadline(processName))),
                    delay, TimeUnit.MILLISECONDS);
            reply.whenComplete((r, e) -> timeout.cancel(false));
        } catch (RejectedExecutionException closing) {
            reply.completeExceptionally(new QueryRejectedException("Engine is shutting down"));
        }
    }

    private static TimeoutException parkedDeadline(String processName) {
        return new TimeoutException(processName + " did not start before the query deadline");
    }

    private void failWaitingQueries(String processName, String reason) {
        List<Envelope.Query> waiting;
        synchronized (waitingForFsm) {
            waiting = waitingForFsm.remove(processName);
        }
        if (waiting == null) return;
        for (Envelope.Query w : waiting) w.reply().completeExceptionally(new QueryRejectedException(reason));
    }

    private void failAllWaitingQueries(String reason) {
        Set<String> names;
        synchronized (waitingForFsm) {
            names = new HashSet<>(waitingForFsm.keySet());
        }
        for (String name : names) failWaitingQueries(name, reason);
    }

    /**
     * A query the FSM being swapped out could not serve: park it for the replacement, so it waits
     * like one to a node not started yet. Whoever announced it still reports its outcome.
     */
    private void reparkQuery(String processName, Envelope.Query q) {
        if (q.reply().isDone()) return;
        ProcessFSM current;
        boolean parked = false;
        String refusal = null;
        synchronized (waitingForFsm) {
            current = fsms.get(processName);
            if (current == null) {
                if (graph.nodes().containsKey(processName) && !paused.containsKey(processName)) {
                    waitingForFsm.computeIfAbsent(processName, k -> new ArrayList<>()).add(q);
                    parked = true;
                } else {
                    refusal = paused.containsKey(processName)
                            ? pausedProcess(processName)
                            : unknownProcess(processName);
                }
            }
        }
        // Outside the monitor: completing a reply runs its hooks.
        if (parked) {
            watchParkedQuery(processName, q);
        } else if (refusal != null) {
            q.reply().completeExceptionally(new QueryRejectedException(refusal));
        } else {
            current.submitAnnouncedQuery(q); // the replacement is already installed
        }
    }

    private void safeOnQuerySent(String processName, UUID queryId, Class<?> messageType, UUID parentQueryId) {
        try {
            observer.onQuerySent(processName, queryId, messageType, parentQueryId);
        } catch (Throwable t) {
            observerThrew(processName, "onQuerySent", t);
        }
    }

    private void safeOnQueryCompleted(String processName, UUID queryId, Duration duration) {
        try {
            observer.onQueryCompleted(processName, queryId, duration);
        } catch (Throwable t) {
            observerThrew(processName, "onQueryCompleted", t);
        }
    }

    private void safeOnQueryFailed(String processName, UUID queryId, String reason, Throwable cause) {
        try {
            observer.onQueryFailed(processName, queryId, reason, cause);
        } catch (Throwable t) {
            observerThrew(processName, "onQueryFailed", t);
        }
    }

    private static String failureReason(Throwable cause) {
        return switch (cause) {
            case TimeoutException ignored -> "timeout";
            case CancellationException ignored -> "cancelled";
            case QueryRejectedException ignored -> "rejected";
            case InitInProgressException ignored -> "init-in-progress";
            default -> "exception";
        };
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            return t.getCause() != null ? t.getCause() : t;
        }
        return t;
    }

    // ───────────────── shutdown ─────────────────

    public void shutdownAll(Duration perNodeTimeout) {
        // A cascade record landing after a consumer's own re-init would make the next start replay a
        // re-init nobody needs. What is skipped is safe: a consumer older than its producer re-inits at startup.
        shuttingDown = true;
        // While deeper levels drain, no producer may start a re-init and run user code after close() began.
        for (ProcessFSM fsm : fsms.values()) fsm.stopReinits();
        awaitCascades(perNodeTimeout);
        failAllWaitingQueries("Engine is shutting down");
        List<List<ProcessFSM>> levels = levelsByDepth(graph.topologicalOrder());
        synchronized (dedupLock) {
            pendingTask.values().forEach(t -> t.cancel(false));
            pendingTask.clear();
            pendingCause.clear();
        }
        // Consumers before their producers, a whole level at a time: close takes levels x budget, not nodes x budget.
        for (int level = levels.size() - 1; level >= 0; level--) {
            Map<ProcessFSM, CompletableFuture<Void>> stopping = new LinkedHashMap<>();
            for (ProcessFSM fsm : levels.get(level)) {
                stopping.put(fsm, fsm.shutdown(perNodeTimeout).toCompletableFuture());
            }
            long deadline = stopDeadline(perNodeTimeout);
            for (var entry : stopping.entrySet()) {
                awaitStopped(entry.getValue(), deadline, entry.getKey().processName(), perNodeTimeout);
            }
        }
        // Again: a compute still draining may have parked a nested query, and deadlines stop firing soon.
        failAllWaitingQueries("Engine is shutting down");
        // Engine.shutdown() leaves the engine usable: a later newGraph must cascade again.
        shuttingDown = false;
    }

    private void awaitCascades(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Thread cascade : List.copyOf(cascades)) {
            long left = deadline - System.nanoTime();
            if (left <= 0) break;
            try {
                cascade.join(Duration.ofNanos(left));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /** Running FSMs grouped by depth (0: no dependencies). */
    private List<List<ProcessFSM>> levelsByDepth(List<ProcessNode> order) {
        Map<String, Integer> depth = new HashMap<>();
        int maxDepth = 0;
        for (ProcessNode node : order) {
            int d = 0;
            for (Dependency dep : node.dependencies()) d = Math.max(d, depth.getOrDefault(dep.name(), 0) + 1);
            depth.put(node.name(), d);
            maxDepth = Math.max(maxDepth, d);
        }
        List<List<ProcessFSM>> levels = new ArrayList<>();
        for (int i = 0; i <= maxDepth; i++) levels.add(new ArrayList<>());
        for (ProcessNode node : order) {
            ProcessFSM fsm = fsms.get(node.name());
            if (fsm != null) levels.get(depth.get(node.name())).add(fsm);
        }
        return levels;
    }

    @Override
    public void close() {
        shutdownAll(configSource.get().cleanupTimeout());
    }

    public record RestoreSnapshot(long clock, Map<String, byte[]> properties) {

        public RestoreSnapshot {
            Objects.requireNonNull(properties, "properties");
        }
    }

    private record Edge(String consumer, boolean reactive) { }
}
