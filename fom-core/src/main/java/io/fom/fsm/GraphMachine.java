package io.fom.fsm;

import io.fom.Dependency;
import io.fom.EngineConfig;
import io.fom.Graph;
import io.fom.ProcessNode;
import io.fom.Sid;
import io.fom.api.EngineObserver;
import io.fom.api.QueryException;
import io.fom.api.QueryRejectedException;
import io.fom.log.LogBackend;
import io.fom.log.LogDependencyChanged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Owner of all {@link ProcessFSM}s for a single graph. Implements
 * {@link ProcessRouter} for cross-process queries; orchestrates trigger
 * dispatch with a per-process dedup window; fires the reactive cascade
 * after every Sid promotion.
 *
 * <p>Spawn order is topological — dependencies first — and each FSM must
 * reach {@code Serving} before the next is started, so a cold-init consumer
 * can issue {@code ctx.query(dep, ...)} against an already-live dependency.</p>
 *
 * <p>Shutdown order is reverse-topological.</p>
 */
public final class GraphMachine implements ProcessRouter, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GraphMachine.class);

    private volatile Graph graph;
    private final java.util.function.Supplier<EngineConfig> configSource;
    private final LogBackend logBackend;
    private final String instanceId;
    private final ScheduledExecutorService scheduler;
    private final EngineObserver observer;
    private final Function<String, RestoreSnapshot> restoreLookup;

    // ConcurrentHashMap: read lock-free from query/dedup/promotion threads while
    // startAll()/applyGraphChange() mutate it. graphSwapLock still serialises the
    // multi-step swap; this guards single-entry visibility and CME-freedom.
    private final Map<String, ProcessFSM> fsms = new ConcurrentHashMap<>();

    /** For each process name → set of (consumerName, kind) that depend on it. */
    private volatile Map<String, List<Edge>> reverseDeps;

    /** Guards {@link #applyGraphChange} from concurrent invocations. */
    private final Object graphSwapLock = new Object();

    /** Dedup window state — guarded by {@code dedupLock}. */
    private final Object dedupLock = new Object();
    private final Map<String, ReinitCause> pendingCause = new HashMap<>();
    private final Map<String, Integer> pendingCounts = new HashMap<>();
    private final Map<String, ScheduledFuture<?>> pendingTask = new HashMap<>();

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
                        java.util.function.Supplier<EngineConfig> configSource,
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

    public Graph currentGraph() {
        return graph;
    }

    public int mailboxSize(String processName) {
        ProcessFSM fsm = fsms.get(processName);
        return fsm == null ? 0 : fsm.mailboxSize();
    }

    /**
     * Diff-based in-place graph swap (TZ §6.4). Identifies added/removed/changed
     * nodes via {@link io.fom.GraphDiff#compute}, shuts down removed and changed
     * FSMs (with {@code LogDead} written for the retired Sid), then spawns
     * fresh FSMs for added and changed nodes. Unchanged FSMs keep their state;
     * the reactive cascade fires automatically as each replacement Sid reaches
     * {@code Serving}.
     *
     * @return the computed {@link io.fom.GraphDiff}; {@code hasChanges()} is
     *         {@code false} when the new graph is structurally identical.
     */
    public io.fom.GraphDiff applyGraphChange(io.fom.Graph next,
                                             Function<String, RestoreSnapshot> snapshots) {
        Objects.requireNonNull(next, "next");
        Objects.requireNonNull(snapshots, "snapshots");
        synchronized (graphSwapLock) {
            io.fom.Graph prev = this.graph;
            io.fom.GraphDiff diff = io.fom.GraphDiff.compute(prev, next);

            // Swap reference first so cross-process queries route against the new topology.
            this.graph = next;
            this.reverseDeps = buildReverseDeps(next);

            Duration perNodeTimeout = configSource.get().defaultCleanupTimeout();
            Duration perNodeStartBudget = configSource.get().defaultInitTimeout()
                    .plus(configSource.get().defaultLoadTimeout());

            // Capture retired Sids BEFORE shutdown so we can fire cascade manually
            // after the replacement reaches Serving (the new FSM's first promotion has
            // previousSid=null, which the listener skips — but from the graph's
            // perspective the Sid changed and reactive consumers must re-init).
            Map<String, Sid> retiredSids = new HashMap<>();
            for (String name : diff.changed()) {
                ProcessFSM fsm = fsms.get(name);
                if (fsm != null && fsm.currentSid() != null) {
                    retiredSids.put(name, fsm.currentSid());
                }
            }

            // Shutdown removed + changed FSMs in reverse topological order of the OLD graph.
            var prevOrderReversed = new ArrayList<io.fom.ProcessNode>(prev.topologicalOrder());
            java.util.Collections.reverse(prevOrderReversed);
            for (io.fom.ProcessNode oldNode : prevOrderReversed) {
                if (diff.removed().contains(oldNode.name()) || diff.changed().contains(oldNode.name())) {
                    shutdownAndRemove(oldNode.name(), perNodeTimeout);
                }
            }

            // Spawn added + changed FSMs in topological order of the NEW graph.
            for (io.fom.ProcessNode node : next.topologicalOrder()) {
                if (diff.added().contains(node.name()) || diff.changed().contains(node.name())) {
                    spawnNode(node, snapshots.apply(node.name()), perNodeStartBudget);
                }
            }

            // Fire reactive cascade for replaced (changed) nodes with their real
            // previous Sid so onSidPromotion's null-check doesn't suppress it.
            for (String name : diff.changed()) {
                Sid prevSid = retiredSids.get(name);
                ProcessFSM newFsm = fsms.get(name);
                if (prevSid != null && newFsm != null && newFsm.currentSid() != null) {
                    onSidPromotion(name, prevSid, newFsm.currentSid());
                }
            }
            return diff;
        }
    }

    private void shutdownAndRemove(String name, Duration timeout) {
        ProcessFSM fsm = fsms.remove(name);
        if (fsm == null) return;
        try {
            fsm.shutdownReplace(timeout)
                    .toCompletableFuture()
                    .get(timeout.toMillis() + 500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("GraphMachine[{}] shutdown-for-replace of {} failed: {}",
                    instanceId, name, e.toString());
        }
    }

    private void spawnNode(io.fom.ProcessNode node, RestoreSnapshot snap, Duration startBudget) {
        ProcessFSM fsm = new ProcessFSM(
                node.name(), node, configSource, logBackend, instanceId, scheduler, this, observer);
        fsm.setSidPromotionListener(this::onSidPromotion);
        fsms.put(node.name(), fsm);
        if (snap != null) {
            log.info("GraphMachine[{}] (re-)spawn warm-load {} from clock {}",
                    instanceId, node.name(), snap.clock());
            fsm.spawnLoad(snap.clock(), snap.properties());
        } else {
            log.info("GraphMachine[{}] (re-)spawn cold-init {}", instanceId, node.name());
            fsm.spawnInit();
        }
        try {
            fsm.servingReady()
                    .toCompletableFuture()
                    .get(startBudget.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException(
                    "(Re-)spawned node '" + node.name() + "' did not reach Serving within " + startBudget, e);
        }
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
        List<ProcessNode> order = graph.topologicalOrder();
        Duration perNodeBudget = configSource.get().defaultInitTimeout().plus(configSource.get().defaultLoadTimeout());

        for (ProcessNode node : order) {
            ProcessFSM fsm = new ProcessFSM(
                    node.name(), node, configSource, logBackend, instanceId, scheduler, this, observer);
            fsm.setSidPromotionListener(this::onSidPromotion);
            fsms.put(node.name(), fsm);

            RestoreSnapshot snap = restoreLookup.apply(node.name());
            if (snap != null) {
                log.info("GraphMachine[{}] warm start for {} from clock {}",
                        instanceId, node.name(), snap.clock());
                fsm.spawnLoad(snap.clock(), snap.properties());
            } else {
                log.info("GraphMachine[{}] cold start for {}", instanceId, node.name());
                fsm.spawnInit();
            }

            try {
                fsm.servingReady()
                        .toCompletableFuture()
                        .get(perNodeBudget.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new RuntimeException(
                        "Node '" + node.name() + "' did not reach Serving within " + perNodeBudget, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for " + node.name(), e);
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                if (cause instanceof RuntimeException re) throw re;
                throw new RuntimeException(cause);
            }
        }
    }

    /**
     * Schedule a re-init for {@code processName} after the dedup window
     * (see {@link EngineConfig#dedupWindow}). Calls accumulating within the
     * window collapse into one firing.
     */
    public void handleTrigger(String processName, ReinitCause cause) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(cause, "cause");
        if (!fsms.containsKey(processName)) {
            throw new IllegalArgumentException("Unknown process: '" + processName + "'");
        }
        synchronized (dedupLock) {
            pendingCause.merge(processName, cause, (existing, fresh) -> existing);
            pendingCounts.merge(processName, 1, Integer::sum);
            if (pendingTask.containsKey(processName)) {
                return;
            }
            long delayMs = configSource.get().dedupWindow().toMillis();
            ScheduledFuture<?> task = scheduler.schedule(
                    () -> fireDedup(processName), delayMs, TimeUnit.MILLISECONDS);
            pendingTask.put(processName, task);
        }
    }

    private void fireDedup(String processName) {
        ReinitCause cause;
        int collapsed;
        synchronized (dedupLock) {
            cause = pendingCause.remove(processName);
            collapsed = pendingCounts.remove(processName);
            pendingTask.remove(processName);
        }
        if (cause == null) return;
        ProcessFSM fsm = fsms.get(processName);
        if (fsm == null) return;
        if (collapsed > 1) {
            observer.onDedupCollapsed(processName, collapsed);
        }
        fsm.submitReinit(cause);
    }

    /** Reactive-dependency cascade: invoked after every Sid promotion. */
    private void onSidPromotion(String processName, Sid previousSid, Sid newSid) {
        observer.onSidPromotion(processName, previousSid, newSid);
        if (previousSid == null) {
            // First cold init — no cascade needed (consumers haven't been spawned yet,
            // or they observed the original Sid as their initial state).
            return;
        }
        List<Edge> consumers = reverseDeps.getOrDefault(processName, List.of());
        for (Edge edge : consumers) {
            if (!edge.reactive()) continue;
            ProcessFSM consumerFsm = fsms.get(edge.consumer());
            if (consumerFsm == null) continue;
            Sid consumerSid = consumerFsm.currentSid();
            if (consumerSid == null) continue;
            long now = System.currentTimeMillis();
            logBackend.append(new LogDependencyChanged(
                    0, now, consumerSid, processName, previousSid.clock(), newSid.clock()), instanceId);
            handleTrigger(edge.consumer(), new ReinitCause.DependencyChanged(
                    processName, previousSid.clock(), newSid.clock()));
        }
    }

    @Override
    public CompletionStage<Object> route(String processName, Object query, long deadlineEpochMillis) {
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(query, "query");
        ProcessFSM fsm = fsms.get(processName);
        if (fsm == null) {
            var failed = new CompletableFuture<Object>();
            failed.completeExceptionally(new QueryException("Unknown process: '" + processName + "'"));
            return failed;
        }
        return fsm.submitQuery(query, deadlineEpochMillis);
    }

    public ProcessFSM fsm(String processName) {
        return fsms.get(processName);
    }

    public boolean contains(String processName) {
        return fsms.containsKey(processName);
    }

    public Set<String> processNames() {
        return new HashSet<>(fsms.keySet());
    }

    public CompletionStage<Object> submitQuery(String processName, Object query, long deadlineEpochMillis) {
        ProcessFSM fsm = fsms.get(processName);
        if (fsm == null) {
            var failed = new CompletableFuture<Object>();
            failed.completeExceptionally(new QueryRejectedException("Unknown process: '" + processName + "'"));
            return failed;
        }
        return fsm.submitQuery(query, deadlineEpochMillis);
    }

    public void shutdownAll(Duration perNodeTimeout) {
        List<ProcessNode> order = graph.topologicalOrder();
        List<ProcessFSM> reversed = new ArrayList<>(order.size());
        for (int i = order.size() - 1; i >= 0; i--) {
            ProcessFSM fsm = fsms.get(order.get(i).name());
            if (fsm != null) reversed.add(fsm);
        }
        // Cancel any pending dedup tasks to avoid firing during shutdown.
        synchronized (dedupLock) {
            pendingTask.values().forEach(t -> t.cancel(false));
            pendingTask.clear();
            pendingCause.clear();
        }
        for (ProcessFSM fsm : reversed) {
            try {
                fsm.shutdown(perNodeTimeout)
                        .toCompletableFuture()
                        .get(perNodeTimeout.toMillis() + 500, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.warn("GraphMachine[{}] shutdown of {} failed: {}",
                        instanceId, fsm.processName(), e.toString());
            }
        }
    }

    @Override
    public void close() {
        shutdownAll(configSource.get().defaultCleanupTimeout());
    }

    public record RestoreSnapshot(int clock, Map<String, byte[]> properties) {

        public RestoreSnapshot {
            Objects.requireNonNull(properties, "properties");
        }
    }

    private record Edge(String consumer, boolean reactive) implements Serializable { }
}
