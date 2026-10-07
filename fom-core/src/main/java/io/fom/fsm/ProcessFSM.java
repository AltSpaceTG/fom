package io.fom.fsm;

import io.fom.EngineConfig;
import io.fom.ProcessNode;
import io.fom.ReinitStrategy;
import io.fom.Sid;
import io.fom.api.AttemptCancelledException;
import io.fom.api.EngineObserver;
import io.fom.api.InitInProgressException;
import io.fom.api.InitializationTimeoutException;
import io.fom.api.LeadershipLostException;
import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.api.QueryRejectedException;
import io.fom.api.UndeclaredDependencyException;
import io.fom.log.LogBackend;
import io.fom.log.LogCleanedUp;
import io.fom.log.LogDead;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLoaded;
import io.fom.log.LogTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * One process's state machine. A single virtual-thread dispatcher reads {@link Envelope}s from the
 * mailbox and handles them against the current {@link State}; {@code init}, {@code load},
 * {@code compute} and {@code cleanUp} run on other threads, so user code never blocks it.
 *
 * <p>Lifecycle: {@code NotPresent → Initializing → Loading → Serving → CleaningUp → Dead}.
 * A failed {@code init} is retried with backoff until {@code init.timeout} is spent; a failed
 * {@code load} up to {@code maxLoadRetries} times, then the FSM falls back to a fresh init.</p>
 *
 * <p>A re-init ({@link ReinitStrategy#KEEP_OLD}) stays in {@code Serving}: the new version is
 * initialised and loaded as a {@link State.Replacement} while the old one answers queries, and
 * replaces it in one dispatcher step once loaded. If it fails, the old version keeps serving.</p>
 */
public final class ProcessFSM implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ProcessFSM.class);

    /** MDC key naming the engine instance on every line the dispatcher logs. */
    public static final String MDC_ENGINE = "fom.engine";
    /** MDC key naming the process on every line its dispatcher logs. */
    public static final String MDC_PROCESS = "fom.process";

    /** Where query observer callbacks run: never on the caller's thread, never on a shared pool. */
    static final Executor OBSERVER_THREADS = task -> Thread.ofVirtual().name("fom-observer").start(task);

    /** A shutdown reply waits this much past the cleanup budget, so a cleanup using all of it still reports. */
    private static final long SHUTDOWN_REPLY_GRACE_MS = 250;
    /** The budget watchdog fires a little after the worker's own check, which normally wins. */
    private static final long BUDGET_WATCH_GRACE_MS = 50;
    /** How long a cleanUp is still waited for once draining queries used up its budget. */
    private static final long CLEANUP_GRACE_MS = 100;

    // A replacement load's commit gate: whoever moves it off OPEN decides the attempt.
    private static final int COMMIT_OPEN = 0;
    private static final int COMMIT_RUNNING = 1;
    private static final int COMMIT_CLOSED = 2;
    private static final int COMMIT_WRITTEN = 3;
    private static final int COMMIT_FAILED = 4;

    /** The FSM whose dispatcher runs on this thread; see {@link #dispatchedProcess}. */
    private static final ThreadLocal<ProcessFSM> DISPATCHING = new ThreadLocal<>();
    /** The FSM whose log writer runs on this thread. */
    private static final ThreadLocal<ProcessFSM> WRITING = new ThreadLocal<>();
    /** The query whose compute runs on this thread. */
    private static final ThreadLocal<Envelope.Query> ISSUING = new ThreadLocal<>();
    /** Tasks queued by {@link #trampolined} on this thread. */
    private static final ThreadLocal<ArrayDeque<Runnable>> CANCELLING = new ThreadLocal<>();

    /** A loaded version of the process and the queries it is answering. */
    private static final class Generation {
        final Sid sid;
        final Process process;
        final AtomicInteger inFlight = new AtomicInteger();
        /** Replies of the queries counted by {@link #inFlight}: failed if they outlive cleanup. */
        final Set<CompletableFuture<Object>> replies = ConcurrentHashMap.newKeySet();

        Generation(Sid sid, Process process) {
            this.sid = Objects.requireNonNull(sid, "sid");
            this.process = Objects.requireNonNull(process, "process");
        }
    }

    /** @see #retireOutcome() */
    public enum RetireOutcome {
        /** The FSM has not got as far as retiring anything (it may never have been asked to). */
        UNKNOWN,
        /** There was nothing to retire, or another cleanup had already written the {@code LogDead}. */
        NOT_NEEDED,
        /** The {@code LogDead} is in the log: the state is retired. */
        PERSISTED,
        /** The append was refused or failed; see {@link #retireFailureCause()}. */
        FAILED
    }

    /** A state and the Sid to report with it, read together. */
    public record View(State state, Sid sid) { }

    /** The init or load in progress, cold or as a replacement: what its results are matched against. */
    private record Phase(boolean init, long cycle, int attempt, Map<String, byte[]> properties, Sid loadSid) { }

    private final String processName;
    private final ProcessNode node;
    private final Supplier<EngineConfig> configSource;
    private final LogBackend logBackend;
    private final String instanceId;
    private final ScheduledExecutorService scheduler;
    private final ProcessRouter router;
    private final EngineObserver observer;
    private final ExecutorService workerExecutor;
    /** Writes the replacement's records in order and off the dispatcher; its one thread ends when idle. */
    private final ThreadPoolExecutor logWriter;
    private final Thread dispatcher;

    private final LinkedBlockingQueue<Envelope> mailbox = new LinkedBlockingQueue<>();
    private final ArrayDeque<Envelope.Query> stash = new ArrayDeque<>();
    private final CompletableFuture<Void> servingReady = new CompletableFuture<>();

    private volatile State state = State.NotPresent.INSTANCE;
    private volatile Sid currentSid;
    /** Whether {@link #currentSid} came from an init of ours rather than from the log at startup. */
    private boolean currentSidIsOurs;
    /** The generation answering queries; {@code null} outside Serving. Written by the dispatcher. */
    private volatile Generation live;
    /** Generations being drained and cleaned up, by Sid; removed on their {@code CleanupDone}. */
    private final Map<Sid, Generation> retiring = new ConcurrentHashMap<>();
    /** Set once the dispatcher has stopped reading the mailbox; see {@link #post}. */
    private volatile boolean dispatcherExited;
    /** Pending Shutdown reply, completed at Dead. */
    private CompletableFuture<Void> pendingShutdownReply;

    // Init/load attempts. Written by the dispatcher unless noted.
    /** Numbers (re)initialisation cycles, so a result of an abandoned one is dropped. */
    private long cycles;
    /** Handle of the running init/load attempt; cancelling it stops the attempt. */
    private volatile CompletableFuture<?> runningAttempt;
    /** The task running the current init/load attempt: interrupted on an explicit stop. */
    private Future<?> runningWorker;
    /**
     * Open while the current attempt has had its start reported but not its end. Whoever reports the
     * end first closes it (worker, dispatcher or a stop), so every start gets exactly one end callback.
     */
    private volatile AtomicBoolean attemptOpen;
    /** Start of the current (re)initialisation; the init budget counts from here. */
    private Instant initStartedAt;
    private Instant initAttemptStartedAt;
    private Instant loadAttemptStartedAt;
    /** Failed attempts in the latest (re)initialisation cycle and the latest failure, for introspection. */
    private volatile int initFailures;
    private volatile int loadFailures;
    private volatile String lastFailure;
    /** Load failures that fell back to a fresh init in the current (re)initialisation. */
    private int loadFallbacks;
    /** The Sid whose load failed while the init that follows it runs; {@code null} otherwise. */
    private volatile Sid loadFallbackSid;
    /** The pending init/load retry tick. */
    private volatile ScheduledFuture<?> retryTickTask;

    // Re-init, both strategies.
    /** A re-init asked for while a start or another re-init runs; applied after it. */
    private volatile ReinitCause pendingReinitCause;
    /** The Sid being replaced in a cold re-init or load fallback, reported to the promotion listener. */
    private volatile Sid supersededSid;
    /** Whether this FSM's state could have been seen: it was warm-loaded or has served. */
    private volatile boolean stateVisible;
    /** A re-init was requested or under way when this FSM shut down, so it never happened here. */
    private volatile boolean reinitInterrupted;
    /** The Sid whose re-init was already given up on; a repeat needs no second report. */
    private Sid reinitDroppedFor;
    /** RELEASE_FIRST: consecutive failures to persist the re-init's {@code LogDead}. */
    private int reinitDeadFailures;
    /** RELEASE_FIRST: a re-init retry is scheduled; requests arriving meanwhile join it. */
    private final AtomicBoolean reinitRetryScheduled = new AtomicBoolean();
    /** The pending automatic re-init retry. */
    private volatile ScheduledFuture<?> reinitRetryTask;
    /** This node's own re-init strategy, or {@code null} to follow the engine config. */
    private volatile ReinitStrategy strategyOverride;

    // Replacement (KEEP_OLD). Dispatcher thread unless noted.
    /** A re-init was asked for and the serving version is still the old one (it failed or was cancelled). */
    private volatile boolean stale;
    /** Another instance owns the log: re-inits are dropped from here on. */
    private boolean leadershipLost;
    /** The engine is closing: no re-init starts any more. */
    private boolean reinitsStopped;
    /** Identifies the scheduled automatic retry; bumped by every new replacement. */
    private long retryToken;
    /** Replacements that failed in a row, for the retry backoff. */
    private int replacementFailures;
    /** What the replacement in flight was asked for; its retry asks again. */
    private ReinitCause replacementCause;
    /** The replacement in flight was cancelled by {@code cancelInit}: not retried. */
    private boolean replacementCancelled;
    /** A persisted replacement to load once the version it replaces serves. */
    private Envelope.ReplaceFrom pendingCandidate;
    /** Commit gate of the replacement's current load attempt; {@code null} for a cold load. */
    private AtomicInteger commitGate;
    /** Cycle of the replacement in flight, or -1: the writer skips a candidate of any other cycle. */
    private volatile long replacementCycle = -1;
    /** Held to change {@link #replacementCycle}, and by the writer to post a candidate only while it is current. */
    private final Object cycleLock = new Object();
    /** Writer thread only: a candidate this FSM wrote that is neither loaded nor retired. */
    private Sid openCandidate;
    /** A candidate whose {@code LogDead} could not be written; retired with the process if it is removed. */
    private volatile Sid unretiredCandidate;

    // Stopping and retiring.
    /** Stopped because it was asked to (pause, removal, swap, close), not because it failed. */
    private volatile boolean stoppedOnRequest;
    /** Set once a swap starts replacing this FSM: queries belong to the replacement. */
    private volatile boolean replaced;
    /** Takes queries this FSM cannot serve because a swap replaces it. */
    private volatile Consumer<Envelope.Query> queryReparker;
    private volatile SidPromotionListener sidPromotionListener;
    /** Set when the log refused this process's {@code LogDead}: its state was never retired. */
    private volatile boolean retireRefused;
    /** Why the retire failed, or {@code null} when the log simply refused it (a takeover). */
    private volatile RuntimeException retireFailureCause;
    private volatile RetireOutcome retireOutcome = RetireOutcome.UNKNOWN;
    /** Sids this FSM has retired with a {@code LogDead}. */
    private final Set<Sid> retiredSids = ConcurrentHashMap.newKeySet();
    /** Failures per observer callback; shared per process name by the graph, see {@link #observerThrew}. */
    private volatile ConcurrentHashMap<String, AtomicLong> observerFailures = new ConcurrentHashMap<>();

    public ProcessFSM(String processName,
                      ProcessNode node,
                      EngineConfig config,
                      LogBackend logBackend,
                      String instanceId,
                      ScheduledExecutorService scheduler,
                      ProcessRouter router) {
        this(processName, node, () -> config, logBackend, instanceId, scheduler, router, EngineObserver.NOOP);
    }

    public ProcessFSM(String processName,
                      ProcessNode node,
                      EngineConfig config,
                      LogBackend logBackend,
                      String instanceId,
                      ScheduledExecutorService scheduler,
                      ProcessRouter router,
                      EngineObserver observer) {
        this(processName, node, () -> config, logBackend, instanceId, scheduler, router, observer);
    }

    public ProcessFSM(String processName,
                      ProcessNode node,
                      Supplier<EngineConfig> configSource,
                      LogBackend logBackend,
                      String instanceId,
                      ScheduledExecutorService scheduler,
                      ProcessRouter router,
                      EngineObserver observer) {
        this.processName = Objects.requireNonNull(processName, "processName");
        this.node = Objects.requireNonNull(node, "node");
        this.configSource = Objects.requireNonNull(configSource, "configSource");
        this.logBackend = Objects.requireNonNull(logBackend, "logBackend");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.router = Objects.requireNonNull(router, "router");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.strategyOverride = node.reinitStrategy();
        // Named per process: a stuck init, load or compute in a thread dump shows whose it is.
        this.workerExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("fom-worker-" + processName + "-", 0).factory());
        this.logWriter = new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                Thread.ofVirtual().name("fom-log-" + processName).factory());
        this.dispatcher = Thread.ofVirtual()
                .name("fom-fsm-" + processName)
                .start(this::dispatcherLoop);
    }

    private EngineConfig config() {
        return configSource.get();
    }

    private ReinitStrategy strategy() {
        ReinitStrategy own = strategyOverride;
        return own != null ? own : config().reinitStrategy();
    }

    private BackoffPolicy backoff() {
        return new BackoffPolicy(config().backoffMin(), config().backoffMax());
    }

    /** Apply the re-init strategy of the node as a later graph defines it ({@code null}: the engine's). */
    public void setReinitStrategyOverride(ReinitStrategy strategy) {
        this.strategyOverride = strategy;
    }

    /** Begin a cold start: run {@code init}. */
    public void spawnInit() {
        post(Envelope.SpawnInit.INSTANCE);
    }

    /**
     * Begin a warm start: skip {@code init} and {@code load} the properties of the
     * {@code LogInitialized} at {@code clock}.
     */
    public void spawnLoad(long clock, Map<String, byte[]> properties) {
        stateVisible = true; // persisted state that consumers may have been built on
        post(new Envelope.SpawnLoad(clock, properties));
    }

    /**
     * Replace the serving version with the candidate persisted at {@code clock} (a re-init written
     * before a restart or a pause and never loaded): loaded beside it, no new init.
     */
    public void replaceFrom(long clock, Map<String, byte[]> properties) {
        post(new Envelope.ReplaceFrom(clock, properties));
    }

    /** Completes when state first reaches {@link State.Serving}, or exceptionally on terminal failure. */
    public CompletionStage<Void> servingReady() {
        return servingReady;
    }

    /** @param parentQueryId the query whose compute sends this one ({@code ctx.query}), or {@code null} */
    public CompletionStage<Object> submitQuery(Object message, long deadlineEpochMillis, UUID parentQueryId) {
        Objects.requireNonNull(message, "message");
        var reply = new CompletableFuture<Object>();
        UUID queryId = UUID.randomUUID();
        // Reported on the submitting thread, so an observer can pick up the caller's context (a trace
        // span). The reply's hook below reports exactly one outcome, the caller's timeout included.
        Instant sentAt = Instant.now();
        observe("onQuerySent", () -> observer.onQuerySent(processName, queryId, message.getClass(), parentQueryId));
        if (observer != EngineObserver.NOOP) { // nobody listens: no thread per query for nothing
            // Async: with a plain whenComplete the caller's own thread could run the observer inside get().
            reply.whenCompleteAsync((result, failure) -> {
                if (failure == null) {
                    Duration took = Duration.between(sentAt, Instant.now());
                    observe("onQueryCompleted", () -> observer.onQueryCompleted(processName, queryId, took));
                } else {
                    Throwable cause = unwrap(failure);
                    observe("onQueryFailed",
                            () -> observer.onQueryFailed(processName, queryId, failureReason(cause), cause));
                }
            }, OBSERVER_THREADS);
        }
        var query = new Envelope.Query(queryId, message, reply, deadlineEpochMillis);
        if (state instanceof State.Dead) {
            rejectOrRepark(query);
        } else {
            post(query);
        }
        return reply;
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

    /**
     * Take over a query another part of the engine already announced to observers (one that waited
     * for this FSM to exist, or one its predecessor could not serve): same id, same reply, so its
     * outcome is still reported once.
     */
    public void submitAnnouncedQuery(Envelope.Query q) {
        Objects.requireNonNull(q, "q");
        if (state instanceof State.Dead && !replaced) {
            q.reply().completeExceptionally(new QueryRejectedException(deadMessage()));
            return;
        }
        post(q);
    }

    public CompletionStage<Void> shutdown(Duration timeout) {
        return shutdown(timeout, false);
    }

    /**
     * Like {@link #shutdown(Duration)}, but first writes {@code LogDead} for the current Sid, so the
     * next start cold-inits the node's new definition. Used when a graph swap replaces the node.
     */
    public CompletionStage<Void> shutdownReplace(Duration timeout) {
        return shutdown(timeout, true);
    }

    private CompletionStage<Void> shutdown(Duration timeout, boolean retireSid) {
        Objects.requireNonNull(timeout, "timeout");
        var reply = new CompletableFuture<Void>();
        post(new Envelope.Shutdown(reply, retireSid));
        return reply.orTimeout(timeout.toMillis() + SHUTDOWN_REPLY_GRACE_MS, TimeUnit.MILLISECONDS);
    }

    public State currentState() {
        return state;
    }

    public Sid currentSid() {
        return currentSid;
    }

    /**
     * What introspection reports. State and Sid are separate writes, so they are re-read until the
     * state is stable around the Sid. No Sid is reported while a new one is being made: in
     * Initializing (the new Sid is written just before the move to Loading) and in a re-init's
     * cleanup (the old Sid is already retired in the log).
     */
    public View view() {
        State before;
        Sid sid;
        State after;
        do {
            before = state;
            sid = currentSid;
            after = state;
        } while (before != after);
        if (before instanceof State.Serving serving) return new View(serving, serving.sid());
        // An init after failed loads keeps reporting the Sid it could not load: cancelInit(Sid) takes that one.
        boolean makingNew = (before instanceof State.Initializing && (sid == null || !sid.equals(loadFallbackSid)))
                || (before instanceof State.CleaningUp c && c.mode() == State.CleanupMode.REINIT);
        return new View(before, makingNew ? null : sid);
    }

    public String processName() {
        return processName;
    }

    /** {@code "Initializing"} or {@code "Loading"} while a new version is made beside the serving one, else {@code null}. */
    public String replacementPhase() {
        return state instanceof State.Serving s && s.replacement() != null ? s.replacement().name() : null;
    }

    /** The serving version is due to be replaced: a re-init is in flight, queued, or failed. */
    public boolean stale() {
        return stale || pendingReinitCause != null || replacementPhase() != null;
    }

    /**
     * Whether stopping now would lose a requested re-init: one is queued, failed, or still
     * initialising. A candidate already persisted and loading is not lost: a resume or a restart loads it.
     */
    public boolean reinitDue() {
        return stale || pendingReinitCause != null || "Initializing".equals(replacementPhase());
    }

    /** Whether shutting this FSM down swallowed a requested re-init (a pause must then mark the node stale). */
    public boolean reinitInterrupted() {
        return reinitInterrupted;
    }

    /** A re-init request waits for the one under way (for tests). */
    public boolean reinitQueued() {
        return pendingReinitCause != null;
    }

    /** An automatic re-init retry is scheduled (for tests). */
    public boolean reinitRetryPending() {
        return reinitRetryTask != null;
    }

    public int mailboxSize() {
        return mailbox.size();
    }

    public int inFlightQueries() {
        Generation current = live;
        int total = current == null ? 0 : current.inFlight.get();
        for (Generation old : retiring.values()) {
            if (old != current) total += old.inFlight.get();
        }
        return total;
    }

    /** Failed {@code init} attempts in the latest (re)initialisation cycle. */
    public int initRetries() {
        return initFailures;
    }

    /** Failed {@code load} attempts in the latest (re)initialisation cycle. */
    public int loadRetries() {
        return loadFailures;
    }

    /** The most recent init/load failure as {@code "Class: message"}, or {@code null}. */
    public String lastFailure() {
        return lastFailure;
    }

    /** Install the listener called after every move to Serving with a new Sid (the reactive cascade). */
    public void setSidPromotionListener(SidPromotionListener listener) {
        this.sidPromotionListener = listener;
    }

    /**
     * Install the handler that takes queries this FSM cannot serve because a graph swap is
     * replacing it: they wait for the replacement instead of being rejected.
     */
    public void setQueryReparker(Consumer<Envelope.Query> reparker) {
        this.queryReparker = reparker;
    }

    /** Whether the log refused to retire this process's state (it is still live in the log). */
    public boolean retireRefused() {
        return retireRefused;
    }

    /**
     * The exception that stopped this process's state from being retired, or {@code null} if the log
     * refused the append outright. A refusal means a takeover; an exception may be a transient write
     * failure, so the graph change reports the two differently.
     */
    public RuntimeException retireFailureCause() {
        return retireFailureCause;
    }

    /**
     * Whether this process's state was retired when it was replaced or removed. The {@code LogDead}
     * is written before cleanup starts, so a shutdown that timed out on a slow {@code cleanUp} has
     * usually retired it already.
     */
    public RetireOutcome retireOutcome() {
        return retireOutcome;
    }

    /** Whether this FSM has written the LogDead that retires {@code sid}. */
    public boolean retired(Sid sid) {
        return sid != null && retiredSids.contains(sid);
    }

    /**
     * The state this FSM wrote and never retired, if any; {@code null} when there is none, when the
     * state came from the log at startup (a later attempt may still load it), or when a
     * {@code Process} served it (then a cleanup retires it). A dead FSM cannot write any more, so
     * the caller that removes it retires this for it.
     */
    public Sid unretiredOwnSid() {
        Sid sid = currentSid;
        if (sid == null || !currentSidIsOurs || retiredSids.contains(sid)) {
            Sid candidate = unretiredCandidate; // a replacement that never served
            return candidate == null || retiredSids.contains(candidate) ? null : candidate;
        }
        return sid;
    }

    /** External trigger entry: queues a {@link Envelope.ReinitRequest}. */
    public void submitReinit(ReinitCause cause) {
        Objects.requireNonNull(cause, "cause");
        if (state instanceof State.Dead) return;
        post(new Envelope.ReinitRequest(cause));
    }

    /**
     * The engine is closing: no re-init starts from here on, a scheduled retry is dropped and one
     * under way is given up (the old version serves until shutdown). The request stays in the log.
     */
    public void stopReinits() {
        post(Envelope.StopReinits.INSTANCE);
    }

    /** Cancel an in-flight init/load for {@code targetSid}; no-op for any other state. */
    public CompletionStage<Void> cancelInit(Sid targetSid) {
        Objects.requireNonNull(targetSid, "targetSid");
        var reply = new CompletableFuture<Void>();
        post(new Envelope.CancelRequest(targetSid, reply));
        return reply;
    }

    /** Cancel whatever init or load is running now, whatever its Sid (a cold init has none). */
    public CompletionStage<Void> cancelCurrentInit() {
        var reply = new CompletableFuture<Void>();
        post(new Envelope.CancelRequest(null, reply));
        return reply;
    }

    /**
     * Cancel the running init/load because the node is being paused or removed: queries waiting on
     * it are told it is shutting down, since it was stopped and did not fail.
     */
    public CompletionStage<Void> cancelCurrentInitForStop() {
        stoppedOnRequest = true;
        return cancelCurrentInit();
    }

    /**
     * The Sid this FSM is replacing while it re-initialises or falls back from a failed load,
     * otherwise {@link #currentSid()}: the last version consumers may have seen.
     */
    public Sid lastServedSid() {
        Sid previous = supersededSid;
        return previous != null ? previous : currentSid;
    }

    /**
     * The process whose dispatcher thread this is, if it belongs to engine {@code instanceId}, else
     * {@code null}. Observer callbacks about that process run here, and a call that stops it from
     * here would wait for this very thread: it can only time out.
     */
    public static String dispatchedProcess(String instanceId) {
        ProcessFSM fsm = DISPATCHING.get();
        return fsm != null && fsm.instanceId.equals(instanceId) ? fsm.processName : null;
    }

    /**
     * Deadline of the query whose compute runs on the calling thread, or {@code null}: a query sent
     * from there through the outer Engine inherits it, as one sent through the context does.
     */
    public static Long issuingDeadline() {
        Envelope.Query issuing = ISSUING.get();
        return issuing == null ? null : issuing.deadlineEpochMillis();
    }

    /** Id of the query whose compute runs on the calling thread, or {@code null}. */
    public static UUID issuingQueryId() {
        Envelope.Query issuing = ISSUING.get();
        return issuing == null ? null : issuing.queryId();
    }

    /**
     * Tie a query sent from inside a compute (through its context or the outer Engine) to the query
     * that compute answers: once nobody waits for that one, nobody waits for this one either.
     * Cancelling the compute's stage does not reach a query sent before the compute returned.
     */
    public static void linkToIssuingQuery(CompletableFuture<?> child) {
        Envelope.Query issuing = ISSUING.get();
        CompletableFuture<?> parent = issuing == null ? null : issuing.reply();
        if (parent == null || parent == child) return;
        parent.whenComplete((res, err) -> {
            if (err != null && !child.isDone()) {
                trampolined(() -> child.completeExceptionally(new CancellationException(
                        "the query that sent it is no longer waited for")));
            }
        });
    }

    /**
     * Count observer failures in {@code shared}, kept by the graph per process name, so the WARN
     * throttle survives the new FSM a pause/resume or restart creates.
     */
    void shareObserverFailures(ConcurrentHashMap<String, AtomicLong> shared) {
        observerFailures = Objects.requireNonNull(shared, "shared");
    }

    @Override
    public void close() {
        try {
            shutdown(config().cleanupTimeout())
                    .toCompletableFuture()
                    .get(config().cleanupTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception ignored) {
            // best-effort
        }
        cancelReinitRetry();
        cancelRetryTick();
        // Interrupt stuck init/load/compute workers instead of blocking forever in ExecutorService.close().
        workerExecutor.shutdownNow();
        logWriter.shutdown(); // records already queued are still written
        try {
            workerExecutor.awaitTermination(config().cleanupTimeout().toMillis(), TimeUnit.MILLISECONDS);
            logWriter.awaitTermination(config().cleanupTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void dispatcherLoop() {
        DISPATCHING.set(this);
        // Two engines in one JVM log identical lines for same-named processes: the MDC tells them apart.
        MDC.put(MDC_ENGINE, instanceId);
        MDC.put(MDC_PROCESS, processName);
        try {
            while (!(state instanceof State.Dead)) {
                Envelope env = mailbox.take();
                try {
                    handle(env);
                } catch (Throwable t) {
                    // The loop outlives a failed envelope, even an Error, and even when logging it fails too.
                    try {
                        log.error("FSM[{}] uncaught error handling {}", processName,
                                env.getClass().getSimpleName(), t);
                    } catch (Throwable ignored) {
                        // nothing left to report it with
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            quietly(() -> log.warn("FSM[{}] dispatcher interrupted", processName));
            settleAfterDispatcherStop(processName + " dispatcher interrupted");
        } catch (Throwable fatal) {
            quietly(() -> log.error("FSM[{}] dispatcher stopped: {}", processName, fatal.toString()));
            settleAfterDispatcherStop(processName + " dispatcher stopped");
        } finally {
            // Nobody reads the mailbox from here on: settle what is queued, so no caller waits for ever.
            dispatcherExited = true;
            if (!(state instanceof State.Dead)) state = State.Dead.INSTANCE;
            Envelope leftover;
            while ((leftover = mailbox.poll()) != null) {
                Envelope env = leftover;
                quietly(() -> rejectAfterDeath(env));
            }
        }
    }

    private void settleAfterDispatcherStop(String why) {
        quietly(() -> failStashedQueries(new QueryRejectedException(why)));
        quietly(() -> servingReady.completeExceptionally(new IllegalStateException(why)));
        quietly(this::completeShutdownReply);
    }

    private static void quietly(Runnable action) {
        try {
            action.run();
        } catch (Throwable ignored) {
            // best effort on the way out
        }
    }

    private void handle(Envelope env) {
        switch (env) {
            case Envelope.SpawnInit ignored -> onSpawnInit();
            case Envelope.SpawnLoad s -> onSpawnLoad(s.clock(), s.properties());
            case Envelope.ReplaceFrom r -> onReplaceFrom(r);
            case Envelope.ReinitRetry r -> onReinitRetry(r);
            case Envelope.InitResult r -> onInitResult(r);
            case Envelope.LoadResult r -> onLoadResult(r);
            case Envelope.Query q -> onQuery(q);
            case Envelope.RetryTick t -> onRetryTick(t);
            case Envelope.Shutdown s -> onShutdown(s);
            case Envelope.CleanupDone c -> onCleanupDone(c);
            case Envelope.ReinitRequest r -> onReinitRequest(r);
            case Envelope.CancelRequest c -> onCancelRequest(c);
            case Envelope.StopReinits ignored -> onStopReinits();
        }
    }

    private void onSpawnInit() {
        if (!(state instanceof State.NotPresent)) {
            log.debug("FSM[{}] ignoring SpawnInit in state {}", processName, state.name());
            return;
        }
        initStartedAt = Instant.now();
        resetFailureCounts();
        long cycle = nextCycle();
        transition(new State.Initializing(cycle, 1, null));
        runInitAttempt(cycle, 1);
    }

    private void onSpawnLoad(long clock, Map<String, byte[]> properties) {
        if (!(state instanceof State.NotPresent)) {
            log.debug("FSM[{}] ignoring SpawnLoad in state {}", processName, state.name());
            return;
        }
        Sid sid = new Sid(processName, clock);
        currentSid = sid;
        currentSidIsOurs = false; // a warm start: this state predates us, never retire it blindly
        resetFailureCounts();
        long cycle = nextCycle();
        transition(new State.Loading(cycle, 1, null, properties));
        runLoadAttempt(sid, properties, cycle, 1);
    }

    private long nextCycle() {
        return ++cycles;
    }

    private void resetFailureCounts() {
        initFailures = 0;
        loadFailures = 0;
        loadFallbacks = 0;
    }

    private Phase phase() {
        return switch (state) {
            case State.Initializing i -> new Phase(true, i.cycle(), i.attempt(), null, null);
            case State.Loading l -> new Phase(false, l.cycle(), l.attempt(), l.properties(), currentSid);
            case State.Serving s when s.replacement() instanceof State.Replacement.Initializing i ->
                    new Phase(true, i.cycle(), i.attempt(), null, null);
            case State.Serving s when s.replacement() instanceof State.Replacement.Loading l ->
                    new Phase(false, l.cycle(), l.attempt(), l.properties(), l.candidate());
            default -> null;
        };
    }

    /** The serving state while a new version is being made beside it, else {@code null}. */
    private State.Serving replacing() {
        return state instanceof State.Serving s && s.replacement() != null ? s : null;
    }

    /** A step of the replacement: the process stays Serving, so observers see no state change. */
    private void setReplacement(Sid serving, State.Replacement replacement) {
        synchronized (cycleLock) { // see writeCandidate
            replacementCycle = replacement == null ? -1 : replacement.cycle();
        }
        state = new State.Serving(serving, replacement);
        if (log.isDebugEnabled()) {
            log.debug("FSM[{}] Serving {} (replacement: {})", processName, serving,
                    replacement == null ? "none" : replacement.name());
        }
    }

    private void enterInit(long cycle, int attempt, Throwable lastError) {
        State.Serving replacing = replacing();
        if (replacing != null) {
            setReplacement(replacing.sid(), new State.Replacement.Initializing(cycle, attempt, lastError));
        } else {
            transition(new State.Initializing(cycle, attempt, lastError));
        }
    }

    private void enterLoad(long cycle, int attempt, Throwable lastError, Map<String, byte[]> properties, Sid sid) {
        State.Serving replacing = replacing();
        if (replacing != null) {
            setReplacement(replacing.sid(), new State.Replacement.Loading(cycle, attempt, lastError, properties, sid));
        } else {
            transition(new State.Loading(cycle, attempt, lastError, properties));
        }
    }

    private void onInitResult(Envelope.InitResult r) {
        Phase phase = phase();
        if (phase == null || !phase.init() || phase.cycle() != r.cycle() || phase.attempt() != r.attempt()) {
            log.debug("FSM[{}] stale InitResult cycle={} attempt={} in state {}",
                    processName, r.cycle(), r.attempt(), state.name());
            retireIfCandidateWritten(r); // the replacement was given up meanwhile: it will never be loaded
            return;
        }
        if (!r.ok()) {
            onInitAttemptFailed(phase, r.failure());
            return;
        }
        State.Serving replacing = replacing();
        Sid newSid = null;
        RuntimeException appendFailed = null;
        if (r.write() != null) {
            newSid = r.write().sid(); // a replacement: appended by the log writer
            appendFailed = r.write().failure();
        } else {
            try {
                newSid = persistInitialized(r.properties(), replacing == null ? null : replacing.sid());
            } catch (RuntimeException e) {
                appendFailed = e;
            }
        }
        if (appendFailed != null) {
            onInitAppendFailed(phase, appendFailed);
            return;
        }
        if (newSid == null) {
            terminateOnLostLeadership("init");
            return;
        }
        loadFallbackSid = null;
        if (replacing == null) {
            currentSid = newSid;
            currentSidIsOurs = true;
        }
        // A request that came while this init ran is older in the log than the state just written:
        // record it again after it, or a restart would take it as done.
        if (pendingReinitCause != null) appendTrigger();
        if (initAttemptStartedAt != null && claimEnd(attemptOpen)) {
            Duration took = Duration.between(initAttemptStartedAt, Instant.now());
            log.info("Process '{}' init completed in {} ms (sid={})", processName, took.toMillis(), newSid.clock());
            Sid sid = newSid;
            observe("onInitCompleted", () -> observer.onInitCompleted(processName, sid, took));
        }
        enterLoad(phase.cycle(), 1, null, r.properties(), newSid);
        runLoadAttempt(newSid, r.properties(), phase.cycle(), 1);
    }

    /** The init succeeded but its {@code LogInitialized} could not be written: a failed attempt, or the end. */
    private void onInitAppendFailed(Phase phase, RuntimeException e) {
        log.error("FSM[{}] could not persist LogInitialized for init attempt {}: {}",
                processName, phase.attempt(), withCauses(e));
        if (claimEnd(attemptOpen)) reportInitFailed(phase.attempt(), e);
        if (e instanceof LeadershipLostException) {
            // Thrown by a fenced backend: as permanent as a refused append, no retry can succeed.
            terminateOnLostLeadership("init", e);
            return;
        }
        if (e instanceof IllegalArgumentException) {
            // The backend refuses this event outright (e.g. over the payload limits): retrying cannot help.
            lastFailure = describe(e);
            terminate(e);
            return;
        }
        onInitAttemptFailed(phase, e);
    }

    private void onInitAttemptFailed(Phase init, Throwable failure) {
        log.warn("FSM[{}] init attempt {} failed: {}", processName, init.attempt(), unwrap(failure).toString());
        initFailures = init.attempt();
        lastFailure = describe(failure);
        UndeclaredDependencyException undeclared = undeclaredDependency(failure);
        if (undeclared != null) {
            // No retry can make the graph grow the dependency the node asked for.
            terminate(undeclared);
            return;
        }
        Duration elapsed = Duration.between(initStartedAt, Instant.now());
        Duration remaining = config().initTimeout().minus(elapsed);
        // The budget is total across retries and never overrun. When the backoff does not fit, retry
        // halfway into what is left after an attempt as long as this one, but not sooner than half of
        // backoffMin, so a fast-failing init does not hammer its upstream at the end of the budget.
        // When not even that fits, give up now with the failure it actually had.
        Duration attemptTook = initAttemptStartedAt == null ? Duration.ZERO
                : Duration.between(initAttemptStartedAt, Instant.now());
        Duration room = remaining.minus(attemptTook);
        Duration delay = backoff().delayFor(init.attempt());
        if (delay.plus(attemptTook).compareTo(remaining) > 0) delay = room.dividedBy(2);
        Duration floor = config().backoffMin().dividedBy(2);
        if (delay.compareTo(floor) < 0) delay = floor;
        if (!remaining.isPositive() || delay.plus(attemptTook).compareTo(remaining) >= 0) {
            var ex = new InitializationTimeoutException(
                    "Init for " + processName + " ran out of its " + config().initTimeout()
                            + " budget (total across retries); last failure: " + failure);
            ex.initCause(unwrap(failure));
            // A replacement that gives up keeps the old version serving: its WARN is the one line then.
            if (replacing() == null) log.error("FSM[{}] giving up: {}", processName, ex.getMessage());
            terminate(ex);
            return;
        }
        int nextAttempt = init.attempt() + 1;
        enterInit(init.cycle(), nextAttempt, failure);
        scheduleRetry(Envelope.Phase.INIT, init.cycle(), nextAttempt, delay);
    }

    private void onLoadResult(Envelope.LoadResult r) {
        Phase loading = phase();
        if (loading == null || loading.init() || loading.cycle() != r.cycle() || loading.attempt() != r.attempt()) {
            log.debug("FSM[{}] stale LoadResult cycle={} attempt={} in state {}",
                    processName, r.cycle(), r.attempt(), state.name());
            if (r.ok()) discardProcess(r.process(), r.sid());
            return;
        }
        Sid sid = r.sid();
        if (!r.ok()) {
            // A replacement's result already handed to the log writer wins over a late timeout.
            if (!closeCommit()) return;
            onLoadAttemptFailed(loading, sid, r.failure());
            return;
        }
        boolean loaded = false;
        RuntimeException appendFailed = null;
        if (r.write() != null) {
            commitGate = null; // the attempt is settled
            loaded = r.write().written(); // a replacement: appended by the log writer
            appendFailed = r.write().failure();
        } else {
            try {
                loaded = persistLoaded(sid);
            } catch (RuntimeException e) {
                appendFailed = e;
            }
        }
        if (appendFailed != null) {
            onLoadAppendFailed(loading, r, appendFailed);
            return;
        }
        if (!loaded) {
            discardProcess(r.process(), sid);
            terminateOnLostLeadership("load");
            return;
        }
        if (loadAttemptStartedAt != null && sid != null && claimEnd(attemptOpen)) {
            Duration took = Duration.between(loadAttemptStartedAt, Instant.now());
            if (replacing() != null) { // it serves once the switch is logged
                log.info("Process '{}' load completed in {} ms (sid={})", processName, took.toMillis(), sid.clock());
            } else {
                log.info("Process '{}' load completed in {} ms — now Serving (sid={})",
                        processName, took.toMillis(), sid.clock());
            }
            observe("onLoadCompleted", () -> observer.onLoadCompleted(processName, sid, took));
        }
        State.Serving replacing = replacing();
        if (replacing != null) {
            promoteReplacement(replacing.sid(), sid, r.process());
        } else {
            startServing(sid, r.process());
        }
    }

    private void onLoadAppendFailed(Phase loading, Envelope.LoadResult r, RuntimeException e) {
        Sid sid = r.sid();
        log.error("FSM[{}] could not persist LogLoaded for load attempt {}: {}",
                processName, loading.attempt(), withCauses(e));
        discardProcess(r.process(), sid);
        if (e instanceof LeadershipLostException) {
            if (claimEnd(attemptOpen)) reportLoadFailed(sid, loading.attempt(), e);
            terminateOnLostLeadership("load", e); // permanent, as in init
            return;
        }
        if (e instanceof IllegalArgumentException) {
            // The backend can never store this event: retrying burns the budget for nothing.
            if (claimEnd(attemptOpen)) reportLoadFailed(sid, loading.attempt(), e);
            lastFailure = describe(e);
            terminate(e);
            return;
        }
        onLoadAttemptFailed(loading, sid, e);
    }

    /** A cold start or a RELEASE_FIRST re-init is loaded and its {@code LogLoaded} written. */
    private void startServing(Sid sid, Process process) {
        Sid previousSid = supersededSid;
        Generation generation = new Generation(sid, process);
        live = generation;
        transition(new State.Serving(sid, null));
        drainStashedQueries(generation);
        supersededSid = null;
        loadFallbacks = 0;
        stateVisible = true;
        // Cascade before signalling readiness: a startup wave waiting on this node must not start a
        // consumer before the promotion marks it stale.
        notifySidPromotion(previousSid, sid);
        servingReady.complete(null);
        Envelope.ReplaceFrom candidate = pendingCandidate;
        pendingCandidate = null;
        if (candidate != null) onReplaceFrom(candidate);
        startPendingReinit();
    }

    /**
     * The new version is loaded and its {@code LogLoaded} written: it answers from the next envelope
     * on. The old one finishes the computes it started, then is cleaned up.
     */
    private void promoteReplacement(Sid oldSid, Sid newSid, Process process) {
        Generation old = live;
        if (old != null) retiring.put(old.sid, old);
        live = new Generation(newSid, process);
        currentSid = newSid;
        currentSidIsOurs = true;
        loadFallbackSid = null;
        setReplacement(newSid, null);
        stale = false;
        reinitInterrupted = false;
        reinitDroppedFor = null;
        replacementFailures = 0;
        replacementCause = null;
        loadFallbacks = 0;
        stateVisible = true;
        log.info("Process '{}' replaced {} with {}", processName, oldSid, newSid);
        notifySidPromotion(oldSid, newSid);
        // Hygiene only: the LogLoaded already made the new version the live one in the log.
        onWriter(() -> {
            try {
                if (!persistDead(oldSid)) {
                    log.warn("FSM[{}] could not retire the replaced {}: no longer the leader of the log",
                            processName, oldSid);
                }
            } catch (RuntimeException e) {
                log.warn("FSM[{}] could not retire the replaced {}: {}", processName, oldSid, withCauses(e));
            }
        });
        if (old != null) cleanUpWhenIdle(old);
        startPendingReinit();
    }

    private void onLoadAttemptFailed(Phase loading, Sid sid, Throwable failure) {
        log.warn("FSM[{}] load attempt {} failed: {}", processName, loading.attempt(), unwrap(failure).toString());
        loadFailures = loading.attempt();
        lastFailure = describe(failure);
        if (claimEnd(attemptOpen)) reportLoadFailed(sid, loading.attempt(), failure);
        UndeclaredDependencyException undeclared = undeclaredDependency(failure);
        if (undeclared != null) {
            // As in init: and a fallback to a fresh init would only loop on it.
            terminate(undeclared);
            return;
        }
        if (loading.attempt() < config().maxLoadRetries()) {
            int nextAttempt = loading.attempt() + 1;
            enterLoad(loading.cycle(), nextAttempt, failure, loading.properties(), sid);
            scheduleRetry(Envelope.Phase.LOAD, loading.cycle(), nextAttempt, backoff().delayFor(loading.attempt()));
            return;
        }
        fallBackToInit(loading, sid, failure);
    }

    /** {@code load} ran out of retries on {@code sid}: initialise a fresh state instead. */
    private void fallBackToInit(Phase loading, Sid sid, Throwable failure) {
        if (replacing() != null) {
            // The candidate never served: retire it and initialise another, the old version serving on.
            retireCandidate(sid);
        } else {
            // The state being replaced may have been served or read: promote the fresh one as a
            // change, so reactive consumers re-initialise against it.
            if (supersededSid == null && stateVisible) supersededSid = sid;
            // Or a flapping node leaves one live LogInitialized per attempt in the log for ever.
            retireUnservedOwnState(sid);
        }
        loadFallbacks++;
        loadFallbackSid = sid; // reported while the fallback init runs: cancelInit(Sid) takes it
        if (loadFallbacks == 1) {
            log.info("FSM[{}] load fallback to init after {} failed attempts", processName, loading.attempt());
            initStartedAt = Instant.now();
            long cycle = nextCycle();
            enterInit(cycle, 1, null);
            runInitAttempt(cycle, 1);
            return;
        }
        // Load fails even on freshly initialised state: back off, bounded by the init budget (not reset here).
        if (Duration.between(initStartedAt, Instant.now()).compareTo(config().initTimeout()) >= 0) {
            var ex = new InitializationTimeoutException("Load for " + processName + " kept failing after "
                    + loadFallbacks + " fresh inits within " + config().initTimeout()
                    + " (total init budget); last failure: " + failure);
            ex.initCause(unwrap(failure));
            terminate(ex);
            return;
        }
        Duration delay = backoff().delayFor(loadFallbacks - 1);
        log.warn("FSM[{}] load failed again after a fresh init ({} times); retrying init in {} ms",
                processName, loadFallbacks, delay.toMillis());
        long cycle = nextCycle();
        enterInit(cycle, 1, failure);
        scheduleRetry(Envelope.Phase.INIT, cycle, 1, delay);
    }

    private void onQuery(Envelope.Query q) {
        switch (state) {
            case State.Serving ignored -> runCompute(live, q);
            case State.Initializing ignored -> stash.add(q);
            case State.Loading ignored -> stash.add(q);
            // Just created: SpawnInit/SpawnLoad is already on its way.
            case State.NotPresent ignored -> stash.add(q);
            // A re-init keeps this FSM, so the query waits for the new state; a swap replaces the FSM,
            // so the graph parks the query for the new one.
            case State.CleaningUp cleaning -> {
                switch (cleaning.mode()) {
                    case REINIT -> stash.add(q);
                    case REPLACE -> repark(q);
                    case SHUTDOWN -> q.reply().completeExceptionally(
                            new QueryRejectedException(processName + " is shutting down"));
                }
            }
            case State.Dead ignored -> rejectOrRepark(q);
        }
    }

    private void onStopReinits() {
        reinitsStopped = true;
        retryToken++;
        cancelReinitRetry();
        if (pendingReinitCause != null) {
            pendingReinitCause = null;
            reinitInterrupted = true;
        }
        State.Serving replacing = replacing();
        if (replacing == null) return;
        // A load whose LogLoaded is already being written finishes: the log has switched anyway.
        if (replacing.replacement() instanceof State.Replacement.Loading && !closeCommit()) return;
        log.info("FSM[{}] engine closing: the re-init stops; {} serves until shutdown", processName, replacing.sid());
        cancelRunningAttempt();
        cancelRetryTick();
        reinitInterrupted = true;
        // A written candidate is kept for the next start, as a shutdown keeps it.
        setReplacement(replacing.sid(), null);
        loadFallbackSid = null;
    }

    private void onReinitRequest(Envelope.ReinitRequest r) {
        ReinitCause cause = r.cause();
        if (reinitsStopped) {
            reinitInterrupted = true; // recorded in the log; the next start replays it
            return;
        }
        switch (state) {
            case State.Serving s -> {
                // During a replacement the request runs after it: the new version may or may not be
                // built on what changed, and that cannot be told.
                if (s.replacement() == null) {
                    startReinit(cause);
                } else {
                    deferReinit(cause, s.replacement() instanceof State.Replacement.Loading);
                }
            }
            case State.Initializing ignored -> pendingReinitCause = mergeCause(pendingReinitCause, cause);
            case State.Loading ignored -> deferReinit(cause, true);
            case State.NotPresent ignored -> pendingReinitCause = mergeCause(pendingReinitCause, cause);
            // RELEASE_FIRST cleanup: the init that follows picks the cause up.
            case State.CleaningUp ignored -> pendingReinitCause = mergeCause(pendingReinitCause, cause);
            case State.Dead ignored -> {
                log.debug("FSM[{}] dropping reinit after Dead", processName);
                reinitInterrupted = true;
            }
        }
    }

    /**
     * Queue a request until the version being made serves. Once that version's {@code LogInitialized}
     * is {@code written}, the request's own record is older than it: record it again, or a restart
     * would take it as done.
     */
    private void deferReinit(ReinitCause cause, boolean written) {
        boolean first = pendingReinitCause == null;
        pendingReinitCause = mergeCause(pendingReinitCause, cause);
        if (first && written) appendTrigger();
    }

    private static ReinitCause mergeCause(ReinitCause existing, ReinitCause incoming) {
        return existing != null ? existing : incoming;
    }

    private void startPendingReinit() {
        ReinitCause pending = pendingReinitCause;
        if (pending == null) return;
        pendingReinitCause = null;
        startReinit(pending);
    }

    private void startReinit(ReinitCause cause) {
        if (reinitsStopped) {
            reinitInterrupted = true;
            return;
        }
        if (strategy() == ReinitStrategy.RELEASE_FIRST) {
            beginReinit(cause);
        } else {
            beginReplacement(cause);
        }
    }

    /** Start making a new version while the serving one keeps answering. Nothing is written yet. */
    private void beginReplacement(ReinitCause cause) {
        if (!(state instanceof State.Serving serving)) return;
        if (serving.replacement() != null) {
            pendingReinitCause = mergeCause(pendingReinitCause, cause);
            return;
        }
        if (leadershipLost) {
            dropReinit(cannotReinit());
            return;
        }
        log.info("FSM[{}] re-init started; {} keeps serving (cause={})", processName, serving.sid(), cause);
        startReplacementCycle(serving, cause);
        long cycle = nextCycle();
        setReplacement(serving.sid(), new State.Replacement.Initializing(cycle, 1, null));
        runInitAttempt(cycle, 1);
    }

    /** Finish a replacement whose candidate was persisted before a restart or a pause: load it, no init. */
    private void onReplaceFrom(Envelope.ReplaceFrom r) {
        if (state instanceof State.NotPresent || state instanceof State.Initializing || state instanceof State.Loading) {
            pendingCandidate = r; // the version it replaces is still starting
            return;
        }
        if (!(state instanceof State.Serving serving) || serving.replacement() != null) {
            log.debug("FSM[{}] ignoring the candidate at clock {} in state {}", processName, r.clock(), state.name());
            return;
        }
        Sid candidate = new Sid(processName, r.clock());
        if (candidate.clock() <= serving.sid().clock()) return; // not newer than what serves
        if (leadershipLost) {
            dropReinit(cannotReinit());
            return;
        }
        var cause = new ReinitCause.Triggered("finishing the re-init started before");
        if (strategy() == ReinitStrategy.RELEASE_FIRST) {
            // Loading it beside the old version would hold both in memory, which RELEASE_FIRST avoids;
            // and once the old one is retired the log no longer keeps the candidate.
            log.info("FSM[{}] retiring the replacement {} persisted earlier; RELEASE_FIRST re-initialises instead",
                    processName, candidate);
            retireCandidate(candidate);
            ReinitCause merged = mergeCause(pendingReinitCause, cause);
            pendingReinitCause = null;
            startReinit(merged);
            return;
        }
        log.info("FSM[{}] loading the replacement {} persisted earlier; {} keeps serving",
                processName, candidate, serving.sid());
        startReplacementCycle(serving, cause); // a fallback init after failed loads gets the full budget
        long cycle = nextCycle();
        setReplacement(serving.sid(), new State.Replacement.Loading(cycle, 1, null, r.properties(), candidate));
        runLoadAttempt(candidate, r.properties(), cycle, 1);
    }

    private void startReplacementCycle(State.Serving serving, ReinitCause cause) {
        retryToken++; // a scheduled automatic retry is superseded
        cancelReinitRetry();
        replacementCause = cause;
        replacementCancelled = false;
        observe("onReinitStarted", () -> observer.onReinitStarted(processName, serving.sid()));
        initStartedAt = Instant.now();
        resetFailureCounts();
        loadFallbackSid = null;
    }

    private void onReinitRetry(Envelope.ReinitRetry r) {
        if (r.token() != retryToken) return; // superseded by a newer re-init
        if (!(state instanceof State.Serving serving) || serving.replacement() != null) return;
        reinitRetryTask = null;
        log.info("FSM[{}] retrying the re-init that failed", processName);
        startReinit(r.cause());
    }

    /**
     * The replacement gave up: the old version keeps serving. Only a permanent failure is not
     * retried: lost leadership, an append the log refuses, an undeclared dependency, an operator's
     * cancel. A request that arrived meanwhile starts the next one at once.
     */
    private void abandonReplacement(State.Serving replacing, Throwable cause) {
        cancelRunningAttempt();
        Sid kept = replacing.sid();
        if (replacing.replacement() instanceof State.Replacement.Loading loading) {
            retireCandidate(loading.candidate());
        }
        setReplacement(kept, null);
        loadFallbackSid = null;
        lastFailure = describe(cause);
        stale = true;
        boolean lostLeadership = cause instanceof LeadershipLostException;
        if (lostLeadership) {
            leadershipLost = true;
            reinitDroppedFor = kept; // reported here: a later request is dropped without a second report
        }
        boolean permanent = lostLeadership || replacementCancelled
                || cause instanceof IllegalArgumentException || undeclaredDependency(cause) != null;
        log.warn("FSM[{}] re-init gave up; keeps serving {}: {}", processName, kept, describe(cause));
        observe("onReinitFailed", () -> observer.onReinitFailed(processName, kept, cause));
        ReinitCause pending = pendingReinitCause;
        pendingReinitCause = null;
        if (lostLeadership) {
            if (pending != null) reinitInterrupted = true;
            return;
        }
        if (pending != null) { // a request that came meanwhile starts now, after a cancel too
            startReinit(pending);
            return;
        }
        if (!permanent) scheduleReinitRetry();
    }

    private void scheduleReinitRetry() {
        EngineConfig config = config();
        if (!config.reinitRetryEnabled()) return;
        Duration delay = new BackoffPolicy(config.reinitRetryBackoffMin(), config.reinitRetryBackoffMax())
                .delayFor(++replacementFailures);
        ReinitCause cause = replacementCause != null ? replacementCause
                : new ReinitCause.Triggered("retrying a failed re-init");
        log.info("FSM[{}] retrying the re-init in {} ms", processName, delay.toMillis());
        cancelReinitRetry();
        reinitRetryTask = schedulePost(new Envelope.ReinitRetry(++retryToken, cause), delay);
    }

    /** Drop the scheduled automatic retry, so it neither fires nor keeps this FSM reachable. */
    private void cancelReinitRetry() {
        ScheduledFuture<?> task = reinitRetryTask;
        reinitRetryTask = null;
        if (task != null) {
            task.cancel(false);
            reinitRetryScheduled.set(false);
        }
    }

    private void onCancelRequest(Envelope.CancelRequest c) {
        State.Serving replacing = replacing();
        if (replacing != null) {
            cancelReplacement(replacing, c.targetSid());
            c.reply().complete(null);
            return;
        }
        if (c.targetSid() == null && reinitRetryTask != null && state instanceof State.Serving) {
            // An operator's cancel also stops a retry that is only scheduled; the node stays stale.
            log.info("FSM[{}] cancelling the scheduled re-init retry; {} keeps serving", processName, currentSid);
            retryToken++;
            cancelReinitRetry();
            stale = true;
        }
        // A null target cancels whatever runs; a Sid only the attempt for that Sid.
        if (c.targetSid() != null && (currentSid == null || !c.targetSid().equals(currentSid))) {
            c.reply().complete(null);
            return;
        }
        if (state instanceof State.Serving) {
            // A stop's cancel that found the start already done: if the stop does not go through
            // (refused, lost leadership), the node keeps serving and must fail like any other later.
            stoppedOnRequest = false;
        }
        if (state instanceof State.Initializing || state instanceof State.Loading) {
            log.info("FSM[{}] cancelling current init/load ({})", processName,
                    c.targetSid() != null ? c.targetSid() : state.name());
            var ex = InitInProgressException.cancelled(processName);
            lastFailure = describe(ex); // NodeReport.lastException tells why the node is Dead
            terminate(ex);
        }
        c.reply().complete(null);
    }

    /**
     * Only the version being made is cancelled; the serving one goes on. A Sid names the candidate (or
     * the candidate whose failed load the running init follows), never the incumbent.
     */
    private void cancelReplacement(State.Serving replacing, Sid target) {
        Sid candidate = replacing.replacement() instanceof State.Replacement.Loading l ? l.candidate() : null;
        boolean matches = target == null || target.equals(candidate) || target.equals(loadFallbackSid);
        // A loaded candidate already being written as the live version is past cancelling.
        if (matches && (candidate == null || closeCommit())) {
            log.info("FSM[{}] cancelling the re-init; {} keeps serving", processName, replacing.sid());
            replacementCancelled = true;
            abandonReplacement(replacing, InitInProgressException.cancelled(processName));
        }
        stoppedOnRequest = false; // it keeps serving: a later failure is reported as one
    }

    /**
     * {@link ReinitStrategy#RELEASE_FIRST}: Serving → CleaningUp(REINIT), writing {@code LogDead}
     * before cleanup, then a fresh init.
     */
    private void beginReinit(ReinitCause cause) {
        if (!(state instanceof State.Serving s) || s.replacement() != null) {
            pendingReinitCause = mergeCause(pendingReinitCause, cause); // after the version being made serves
            return;
        }
        Sid sid = currentSid;
        if (sid == null) {
            log.warn("FSM[{}] reinit requested without a currentSid; ignoring", processName);
            return;
        }
        if (reinitRetryScheduled.get()) {
            return; // the scheduled retry re-inits anyway
        }
        log.info("FSM[{}] beginning reinit cycle (cause={})", processName, cause);
        try {
            if (!writeNow(() -> persistDead(sid))) {
                // Keep serving the state we have (reads stay correct) rather than end Dead with none.
                logReinitDropped(sid);
                dropReinit(cannotReinit());
                return;
            }
            reinitDeadFailures = 0;
        } catch (RuntimeException e) {
            onReinitDeadFailed(sid, cause, e);
            return;
        }
        Generation serving = live;
        supersededSid = sid;
        retireLive();
        transition(new State.CleaningUp(sid, State.CleanupMode.REINIT));
        cleanUpRetired(serving, sid);
    }

    /** RELEASE_FIRST: the re-init's {@code LogDead} failed. Keep serving; retry unless the failure is permanent. */
    private void onReinitDeadFailed(Sid sid, ReinitCause cause, RuntimeException e) {
        if (e instanceof LeadershipLostException) {
            logReinitDropped(sid);
            dropReinit(e);
            return;
        }
        if (e instanceof IllegalArgumentException) {
            log.error("FSM[{}] cannot re-initialise {}: the log refuses its LogDead outright; "
                    + "keeping the current state and dropping the request: {}", processName, sid, e.toString());
            dropReinit(e);
            return;
        }
        int attempt = ++reinitDeadFailures;
        Duration delay = backoff().delayFor(attempt);
        lastFailure = describe(e); // a stalled re-init shows in introspect()
        if (Integer.bitCount(attempt) == 1) { // attempts 1, 2, 4, 8, …: no log flood during an outage
            log.error("FSM[{}] could not persist LogDead for {} (attempt {}); retrying reinit in {} ms: {}",
                    processName, sid, attempt, delay.toMillis(), e.toString());
        } else {
            log.debug("FSM[{}] could not persist LogDead for {} (attempt {}); retrying reinit in {} ms: {}",
                    processName, sid, attempt, delay.toMillis(), e.toString());
        }
        reinitRetryScheduled.set(true);
        try {
            reinitRetryTask = scheduler.schedule(() -> {
                reinitRetryScheduled.set(false);
                post(new Envelope.ReinitRequest(cause));
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException closing) {
            reinitRetryScheduled.set(false);
            log.debug("FSM[{}] scheduler rejected the reinit retry (engine closing)", processName);
        }
    }

    private void logReinitDropped(Sid sid) {
        log.error("FSM[{}] cannot re-initialise {}: this instance is no longer the leader of the log; "
                + "keeping the current state and dropping the request", processName, sid);
    }

    private LeadershipLostException cannotReinit() {
        return new LeadershipLostException("cannot re-initialise " + processName + ": no longer the leader of the log");
    }

    /**
     * Give up on a re-init this instance can no longer record. The node keeps serving the state it
     * has, but that state is known to be stale: the failure is reported, and a pause must remember it
     * so a resume re-initialises instead of warm-loading it.
     */
    private void dropReinit(RuntimeException cause) {
        reinitInterrupted = true;
        stale = true;
        lastFailure = describe(cause);
        reinitDeadFailures = 0; // a later transient failure starts its backoff from the bottom
        Sid sid = currentSid;
        if (sid != null && sid.equals(reinitDroppedFor)) {
            return; // already reported for this state; a trigger loop must not flood the observer
        }
        reinitDroppedFor = sid;
        reportInitFailed(1, cause);
    }

    private void onRetryTick(Envelope.RetryTick t) {
        Phase phase = phase();
        boolean init = t.phase() == Envelope.Phase.INIT;
        if (phase == null || phase.init() != init || phase.attempt() != t.forAttempt()) return;
        if (phase.cycle() != t.cycle()) {
            log.debug("FSM[{}] stale {} RetryTick cycle={} attempt={}", processName, t.phase(), t.cycle(), t.forAttempt());
            return;
        }
        if (init) {
            runInitAttempt(t.cycle(), t.forAttempt());
        } else {
            runLoadAttempt(phase.loadSid(), phase.properties(), t.cycle(), t.forAttempt());
        }
    }

    private void onShutdown(Envelope.Shutdown s) {
        if (state instanceof State.Dead) {
            // A dead FSM cannot retire anything; whoever removes it writes the LogDead (see unretiredOwnSid).
            retireOutcome = RetireOutcome.NOT_NEEDED;
            awaitLogWriter(config().cleanupTimeout());
            s.reply().complete(null);
            return;
        }
        if (pendingShutdownReply != null) {
            pendingShutdownReply.whenComplete((v, err) -> {
                if (err != null) s.reply().completeExceptionally(err);
                else s.reply().complete(null);
            });
            return;
        }
        pendingShutdownReply = s.reply();
        stoppedOnRequest = true;
        cancelReinitRetry();
        State prev = state;
        cancelRetryTick();
        State.Serving replacing = replacing();
        Sid candidate = replacing != null && replacing.replacement() instanceof State.Replacement.Loading l
                ? l.candidate() : null;
        if (prev instanceof State.Initializing || prev instanceof State.Loading || replacing != null) {
            cancelRunningAttempt(); // its result would be discarded anyway
        }
        // A load being written as the live version lands first; the retires below follow it.
        if (candidate != null && !closeCommit()) candidate = awaitCommit(candidate);
        if (replacing != null) setReplacement(replacing.sid(), null);
        // A requested re-init will not happen in this FSM. A persisted candidate is not lost: a resume
        // or a restart loads it.
        if ((prev instanceof State.CleaningUp c && c.mode() == State.CleanupMode.REINIT) || pendingReinitCause != null
                || stale || (replacing != null && candidate == null)) {
            reinitInterrupted = true;
        }
        // Already in CleaningUp(REINIT): LogDead is written and a CleanupDone is on its way. Switch the
        // mode and wait for it.
        boolean cleanupInFlight = prev instanceof State.CleaningUp;
        Sid retiringSid = prev instanceof State.CleaningUp cleaning
                ? cleaning.sidBeingRetired()
                : orPlaceholder(currentSid);
        State.CleanupMode mode = s.retireSid() ? State.CleanupMode.REPLACE : State.CleanupMode.SHUTDOWN;
        if (s.retireSid()) retireCandidatesNow(candidate);
        if (s.retireSid() && currentSid != null && !cleanupInFlight) {
            retireForReplace();
        } else {
            retireOutcome = RetireOutcome.NOT_NEEDED; // nothing of ours, or the cleanup already wrote it
        }
        transition(new State.CleaningUp(retiringSid, mode));
        if (mode == State.CleanupMode.REPLACE) {
            replaced = true;
            Envelope.Query stashed;
            while ((stashed = stash.poll()) != null) {
                repark(stashed); // the replacement serves them
            }
        } else {
            failStashedQueries(new QueryRejectedException(processName + " is shutting down"));
        }
        if (prev instanceof State.Serving) {
            Generation serving = live;
            retireLive();
            cleanUpRetired(serving, retiringSid);
        } else if (!cleanupInFlight) {
            post(new Envelope.CleanupDone(retiringSid, null, null));
        }
    }

    /**
     * The candidate's {@code LogLoaded} is being written: wait for it. Once written the log serves the
     * candidate, so that is the state this stop records and a removal retires.
     *
     * @return the candidate, or {@code null} if it is now the current Sid
     */
    private Sid awaitCommit(Sid candidate) {
        awaitLogWriter(config().cleanupTimeout());
        AtomicInteger gate = commitGate;
        if (gate == null || gate.get() != COMMIT_WRITTEN) return candidate;
        log.info("FSM[{}] {} was promoted in the log as it stopped", processName, candidate);
        currentSid = candidate;
        currentSidIsOurs = true;
        return null;
    }

    /** A replacement that never served goes with the node; retired first, it never outlives its incumbent. */
    private void retireCandidatesNow(Sid unloaded) {
        writeNow(() -> {
            if (unloaded != null) retireCandidateNow(unloaded);
            Sid orphan = unretiredCandidate;
            if (orphan != null) retireCandidateNow(orphan);
            if (openCandidate != null) retireCandidateNow(openCandidate);
            return null;
        });
    }

    /** A swap replaces the node: write {@code LogDead} before cleanup, so a crash mid-cleanup still retires it. */
    private void retireForReplace() {
        Sid retire = currentSid;
        try {
            if (writeNow(() -> persistDead(retire))) {
                retireOutcome = RetireOutcome.PERSISTED;
            } else {
                log.error("FSM[{}] could not retire {} during replace: no longer the leader of the log",
                        processName, currentSid);
                retireRefused = true; // the swap reports it: the state is still live in the log
                retireOutcome = RetireOutcome.FAILED;
            }
        } catch (RuntimeException e) {
            log.error("FSM[{}] could not persist LogDead for {} during replace; "
                    + "a restart may warm-load the retired state: {}", processName, currentSid, e.toString());
            // Still live in the log either way, so the swap reports it; the cause says which it was.
            retireFailureCause = e;
            retireRefused = true;
            retireOutcome = RetireOutcome.FAILED;
        }
    }

    private void onCleanupDone(Envelope.CleanupDone c) {
        retiring.remove(c.generation());
        boolean ok = c.failure() == null;
        if (!ok) {
            log.warn("FSM[{}] cleanup failed: {}", processName, c.failure().toString());
        }
        Sid retiredSid = c.generation();
        // Clock 0: stopped before any state existed (mid init), so no cleanUp ran and nothing is recorded.
        if (retiredSid.clock() != 0) {
            onWriter(() -> { // a stop waits for the writer, bounded, before it is reported done
                try {
                    persistCleanedUp(retiredSid, ok);
                } catch (RuntimeException e) {
                    // LogCleanedUp is informational; never let it wedge the FSM in CleaningUp.
                    log.warn("FSM[{}] could not persist LogCleanedUp for {}: {}", processName, retiredSid,
                            withCauses(e));
                }
            });
            Instant cleanupStart = c.startedAt(); // of this cleanup, not of an earlier generation's
            Duration took = cleanupStart == null ? Duration.ZERO : Duration.between(cleanupStart, Instant.now());
            observe("onCleanupCompleted", () -> observer.onCleanupCompleted(processName, retiredSid, ok, took));
        }
        // A version replaced while serving needs nothing more: its successor is already live.
        if (!(state instanceof State.CleaningUp cleaning)) {
            return;
        }
        switch (cleaning.mode()) {
            case REINIT -> {
                if (!retiredSid.equals(cleaning.sidBeingRetired())) return; // an older version finishing
                // The LogDead landed, so this really re-initialises: forget an earlier dropped request,
                // whose staleness a pause would otherwise persist for ever.
                reinitInterrupted = false;
                reinitDroppedFor = null;
                reinitDeadFailures = 0;
                currentSid = null;
                live = null;
                initStartedAt = Instant.now();
                resetFailureCounts();
                long cycle = nextCycle();
                transition(new State.Initializing(cycle, 1, null));
                runInitAttempt(cycle, 1);
            }
            case SHUTDOWN, REPLACE -> {
                // Every version still held (live and any replaced one draining) is cleaned up first.
                if (!retiring.isEmpty()) return;
                if (pendingReinitCause != null) reinitInterrupted = true;
                // Nothing queued may land after the stop is reported: a resume or a new engine scans the log next.
                awaitLogWriter(config().cleanupTimeout());
                transition(State.Dead.INSTANCE);
                completeShutdownReply();
                servingReady.completeExceptionally(
                        new IllegalStateException(processName + " transitioned to Dead before Serving"));
            }
        }
    }

    private void runInitAttempt(long cycle, int attempt) {
        // A replacement's init has no Sid of its own yet: it is not the serving version.
        State.Serving replacing = replacing();
        Sid own = replacing != null ? loadFallbackSid : currentSid;
        Sid replaces = replacing != null ? replacing.sid() : null;
        Sid ctxSid = orPlaceholder(own);
        List<String> deps = dependencyNames();
        // What is left of the total budget: a retry near the end must not run a whole budget past it.
        long budgetLeftMs = initStartedAt == null ? config().initTimeout().toMillis()
                : config().initTimeout().minus(Duration.between(initStartedAt, Instant.now())).toMillis();
        long timeoutMs = Math.max(1, Math.min(config().initTimeout().toMillis(), budgetLeftMs));
        Instant attemptStart = Instant.now();
        logAttemptStart("init", attempt);
        observe("onInitStarted", () -> observer.onInitStarted(processName, attempt));
        var open = new AtomicBoolean(true);
        attemptOpen = open;
        // Published before the worker runs: a terminate() right after this cancels this attempt.
        var cancelHandle = new CompletableFuture<Void>();
        runningAttempt = cancelHandle;
        // The budget covers the call too: an init may do its work before returning its stage.
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        runningWorker = workerExecutor.submit(() -> {
            CompletableFuture<Map<String, byte[]>> stage = null;
            try {
                if (cancelHandle.isCancelled()) {
                    throw new AttemptCancelledException(processName + " init cancelled before it started");
                }
                var ctx = QueryableContextImpl.forPhase(ctxSid, workerExecutor, deps, router);
                stage = invokeInit(ctx).toCompletableFuture();
                CompletableFuture<Map<String, byte[]>> started = stage;
                cancelHandle.whenComplete((ignored, failure) -> started.cancel(true)); // runs now if already cancelled
                // Checked first: an already-completed stage would hand back its value with no time left.
                long leftNanos = deadlineNanos - System.nanoTime();
                if (leftNanos <= 0) throw new TimeoutException("the init budget is spent");
                Map<String, byte[]> props = stage.get(leftNanos, TimeUnit.NANOSECONDS);
                Objects.requireNonNull(props, "init returned null properties");
                // Only if still ours: a stop or the budget watchdog may have ended the attempt meanwhile.
                if (cancelHandle.complete(null)) {
                    if (replaces == null) {
                        post(new Envelope.InitResult(cycle, attempt, props, null));
                    } else {
                        onWriter(() -> writeCandidate(cycle, attempt, props, replaces));
                    }
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                cancelHandle.complete(null);
                reportInitAttemptEnd(open, cancelHandle, attempt, cause);
                post(new Envelope.InitResult(cycle, attempt, null, cause));
            } catch (TimeoutException e) {
                // Init returns only properties, so a late result holds nothing to release: just stop it.
                stage.cancel(true);
                var timeout = new TimeoutException(
                        processName + " init attempt " + attempt + " exceeded " + Duration.ofMillis(timeoutMs));
                cancelHandle.complete(null);
                reportInitAttemptEnd(open, cancelHandle, attempt, timeout);
                post(new Envelope.InitResult(cycle, attempt, null, timeout));
            } catch (Throwable t) {
                cancelHandle.complete(null);
                reportInitAttemptEnd(open, cancelHandle, attempt, t);
                post(new Envelope.InitResult(cycle, attempt, null, t));
            }
        });
        watchBudget(cancelHandle, timeoutMs, () -> {
            var timeout = new TimeoutException(processName + " init attempt " + attempt + " exceeded "
                    + Duration.ofMillis(timeoutMs) + " (init had not even returned its stage)");
            // Reported before the result is posted: if this was the last attempt the dispatcher gives up
            // at once, and its stop would otherwise report a cancellation for a timeout.
            reportInitAttemptEnd(open, cancelHandle, attempt, timeout);
            post(new Envelope.InitResult(cycle, attempt, null, timeout));
        });
        initAttemptStartedAt = attemptStart;
    }

    private void logAttemptStart(String phase, int attempt) {
        if (attempt == 1) {
            log.info("Process '{}' {} started", processName, phase);
        } else {
            log.info("Process '{}' {} retry (attempt {})", processName, phase, attempt);
        }
    }

    /**
     * End the attempt at its budget from the timer side. The worker checks the budget only once init
     * or load has returned its stage, so one that blocks before returning is bounded only by this. The
     * blocked thread is not interrupted, but the attempt is over: its late result is dropped.
     */
    private void watchBudget(CompletableFuture<Void> cancelHandle, long timeoutMs, Runnable onTimeout) {
        try {
            // Completing the handle runs hooks that cancel the user's stage: not on the single timer thread.
            var watch = scheduler.schedule(() -> Thread.ofVirtual().name("fom-budget-" + processName).start(() -> {
                if (cancelHandle.completeExceptionally(new TimeoutException("attempt budget spent"))) {
                    onTimeout.run();
                }
            }), timeoutMs + BUDGET_WATCH_GRACE_MS, TimeUnit.MILLISECONDS);
            cancelHandle.whenComplete((ignored, failure) -> watch.cancel(false));
        } catch (RejectedExecutionException closing) {
            // the engine is closing: the attempt is being stopped anyway
        }
    }

    /** Whether the caller is first to report the end of the attempt {@code open} belongs to. */
    private static boolean claimEnd(AtomicBoolean open) {
        return open != null && open.compareAndSet(true, false);
    }

    /**
     * The worker's report of how its attempt ended. A stop cancels the handle and then the attempt's
     * stage, so the worker may wake with a CancellationException or an InterruptedException and report
     * first: it is still the engine's stop, and reported as one.
     */
    private void reportInitAttemptEnd(AtomicBoolean open, CompletableFuture<Void> cancelHandle,
                                      int attempt, Throwable cause) {
        if (!claimEnd(open)) return; // reported by the cancel
        reportInitFailed(attempt, cancelHandle.isCancelled()
                ? new AttemptCancelledException(processName + " init attempt " + attempt + " cancelled")
                : cause);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CompletionStage<Map<String, byte[]>> invokeInit(QueryableContext ctx) {
        Object factory = node.initFactory().get();
        if (factory == null) {
            throw new IllegalStateException("the init factory of '" + processName + "' returned null");
        }
        Serializable param = node.param();
        if (param == null) {
            return ((ProcessInitializer) factory).init(ctx);
        }
        return ((ParamProcessInitializer) factory).init(ctx, param);
    }

    private void runLoadAttempt(Sid sid, Map<String, byte[]> properties, long cycle, int attempt) {
        List<String> deps = dependencyNames();
        long timeoutMs = config().loadTimeout().toMillis();
        loadAttemptStartedAt = Instant.now();
        logAttemptStart("load", attempt);
        observe("onLoadStarted", () -> observer.onLoadStarted(processName, sid, attempt));
        attemptOpen = new AtomicBoolean(true);
        AtomicInteger gate = replacing() != null ? new AtomicInteger(COMMIT_OPEN) : null;
        commitGate = gate;
        // Taken after the observer callback, so a slow observer cannot eat the budget.
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        // Published before the worker runs. A cancel stops waiting for the attempt; the loader's own
        // work cannot be forced to stop, but a Process it produces late is released.
        var cancelHandle = new CompletableFuture<Void>();
        runningAttempt = cancelHandle;
        // Exactly one owner for the Process: the FSM, which serves and later cleans it up, or a discard
        // hook. Without the claim a Process the FSM refused would be cleaned up twice.
        var resultClaimed = new AtomicBoolean();
        runningWorker = workerExecutor.submit(() -> {
            try {
                var ctx = QueryableContextImpl.forPhase(orPlaceholder(sid), workerExecutor, deps, router);
                CompletableFuture<Process> loading = invokeLoad(ctx, properties).toCompletableFuture();
                // Release a Process that already arrived, then cancel: a loader that watches for the
                // cancellation stops there, and one that ignores it owns what it allocated.
                Runnable abandon = () -> {
                    loading.thenAccept(late -> {
                        if (late != null && resultClaimed.compareAndSet(false, true)) discardProcess(late, sid);
                    });
                    loading.cancel(true);
                };
                CompletableFuture<Process> waited = loading.copy();
                cancelHandle.whenComplete((ignored, failure) -> { // runs now if already cancelled
                    if (failure == null) return; // the attempt finished on its own
                    abandon.run();
                    waited.cancel(true);
                });
                Process process;
                try {
                    long leftNanos = deadlineNanos - System.nanoTime();
                    if (leftNanos <= 0) throw new TimeoutException("the load budget is spent");
                    process = waited.get(leftNanos, TimeUnit.NANOSECONDS);
                } catch (TimeoutException e) {
                    // An uncancelled stage would keep its work (a coroutine, a poll loop) running for good.
                    abandon.run();
                    var timedOut = new TimeoutException(
                            processName + " load attempt " + attempt + " exceeded " + Duration.ofMillis(timeoutMs));
                    timedOut.initCause(e);
                    throw timedOut;
                }
                Objects.requireNonNull(process, "load returned null process");
                cancelHandle.complete(null); // the attempt is over: a cancel has nothing left to stop
                if (!resultClaimed.compareAndSet(false, true)) {
                    // A cancel got here first and owns the Process.
                    throw new AttemptCancelledException(processName + " load attempt " + attempt + " was cancelled");
                }
                if (gate == null) {
                    post(new Envelope.LoadResult(cycle, sid, attempt, process, null));
                } else {
                    onWriter(() -> commitCandidate(cycle, sid, attempt, process, gate));
                }
            } catch (ExecutionException e) {
                // copy() wraps the failure in a CompletionException: report the real cause.
                Throwable cause = e.getCause() != null ? unwrap(e.getCause()) : e;
                cancelHandle.complete(null);
                post(new Envelope.LoadResult(cycle, sid, attempt, null, cause));
            } catch (TimeoutException e) {
                cancelHandle.complete(null);
                post(new Envelope.LoadResult(cycle, sid, attempt, null, e));
            } catch (Throwable t) {
                // Settled whatever ended it: a cancel during the next backoff must not report this one again.
                cancelHandle.complete(null);
                post(new Envelope.LoadResult(cycle, sid, attempt, null, t));
            }
        });
        // Completing the handle exceptionally also arms the discard hook for a late Process.
        watchBudget(cancelHandle, timeoutMs, () -> post(new Envelope.LoadResult(cycle, sid, attempt, null,
                new TimeoutException(processName + " load attempt " + attempt + " exceeded "
                        + Duration.ofMillis(timeoutMs) + " (load had not even returned its stage)"))));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CompletionStage<Process> invokeLoad(QueryableContext ctx, Map<String, byte[]> props) {
        Object factory = node.loadFactory().get();
        if (factory == null) {
            throw new IllegalStateException("the load factory of '" + processName + "' returned null");
        }
        Serializable param = node.param();
        if (param == null) {
            return ((ProcessLoader) factory).load(ctx, props);
        }
        return ((ParamProcessLoader) factory).load(ctx, props, param);
    }

    private void runCompute(Generation generation, Envelope.Query q) {
        // Timed out or cancelled while it waited: nobody reads the answer, so don't run user code for it.
        if (q.reply().isDone()) return;
        Sid sid = generation.sid;
        Process process = generation.process;
        List<String> deps = dependencyNames();
        generation.inFlight.incrementAndGet();
        generation.replies.add(q.reply());
        Instant computeStart = Instant.now();
        // The in-flight slot is what cleanup drains. Freed once: when the stage settles, or as soon as
        // nobody waits for the answer and the compute cannot be stopped, so an abandoned compute does
        // not hold a re-init or close for the whole cleanup budget.
        var released = new AtomicBoolean();
        Runnable release = () -> {
            if (released.compareAndSet(false, true)) {
                generation.replies.remove(q.reply());
                generation.inFlight.decrementAndGet();
            }
        };
        var stageRef = new AtomicReference<CompletionStage<?>>();
        workerExecutor.submit(() -> {
            try {
                if (q.reply().isDone()) { // failed while waiting for a worker
                    release.run();
                    return;
                }
                // Once the caller timed out or cancelled, cancel the compute so it does not hold up cleanup.
                q.reply().whenComplete((res, err) -> {
                    if (err == null) return;
                    trampolined(() -> {
                        CompletionStage<?> stage = stageRef.get();
                        if (stage != null) cancelQuietly(stage);
                        if (stage == null || !isSettled(stage)) release.run();
                    });
                });
                var ctx = QueryableContextImpl.forQuery(
                        sid, workerExecutor, deps, q.deadlineEpochMillis(), router, q.queryId());
                CompletionStage<?> stage;
                Envelope.Query outer = ISSUING.get();
                ISSUING.set(q);
                try {
                    stage = process.compute(ctx, q.message());
                } finally {
                    if (outer == null) ISSUING.remove(); else ISSUING.set(outer);
                }
                stageRef.set(stage);
                if (q.reply().isDone()) trampolined(() -> cancelQuietly(stage)); // failed while it ran
                // Completing this reply may complete the reply of a compute waiting on it, and so on up
                // a chain of queries: trampolined, so the chain does not become recursion.
                stage.whenComplete((res, err) -> trampolined(() -> {
                    Duration took = Duration.between(computeStart, Instant.now());
                    try {
                        // The outcome is reported by the reply's own hook (see submitQuery).
                        if (err != null) {
                            q.reply().completeExceptionally(unwrap(err));
                        } else {
                            q.reply().complete(res);
                        }
                    } finally {
                        if (released.get() || observer == EngineObserver.NOOP) {
                            release.run();
                        } else {
                            // Off the completing thread, so a slow observer delays neither answers nor
                            // other computes; before the slot is freed, so no callback outlives the process.
                            OBSERVER_THREADS.execute(() -> {
                                try {
                                    observe("onComputeDuration", () -> observer.onComputeDuration(processName, took));
                                } finally {
                                    release.run();
                                }
                            });
                        }
                    }
                }));
            } catch (Throwable t) {
                release.run();
                q.reply().completeExceptionally(t);
            }
        });
    }

    private static boolean isSettled(CompletionStage<?> stage) {
        try {
            return stage.toCompletableFuture().isDone();
        } catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private static void cancelQuietly(CompletionStage<?> stage) {
        try {
            stage.toCompletableFuture().cancel(true);
        } catch (UnsupportedOperationException ignored) {
            // a stage that cannot be converted cannot be cancelled either
        }
    }

    /**
     * Run {@code task}, and whatever it triggers through here, as a loop on this thread rather than
     * nested calls. Settling one query of a chain settles the next, as deep as the chain goes; nested,
     * that overflows the stack, and CompletableFuture swallows the overflow.
     */
    private static void trampolined(Runnable task) {
        ArrayDeque<Runnable> pending = CANCELLING.get();
        if (pending != null) {
            pending.add(task);
            return;
        }
        pending = new ArrayDeque<>();
        CANCELLING.set(pending);
        try {
            for (Runnable next = task; next != null; next = pending.poll()) {
                try {
                    next.run();
                } catch (RuntimeException e) {
                    log.warn("settling a chain of queries failed: {}", e.toString());
                }
            }
        } finally {
            CANCELLING.remove();
        }
    }

    /** Move the live generation to {@link #retiring}; its queries count until its cleanup is done. */
    private void retireLive() {
        Generation serving = live;
        if (serving != null) retiring.put(serving.sid, serving);
        live = null;
    }

    /** Clean up the generation just retired, or report the cleanup done at once if there was none. */
    private void cleanUpRetired(Generation generation, Sid sid) {
        if (generation != null) {
            cleanUpWhenIdle(generation);
        } else {
            post(new Envelope.CleanupDone(sid, null, null));
        }
    }

    /** Drain {@code generation}'s queries, then clean it up; ends with its {@code CleanupDone}. */
    private void cleanUpWhenIdle(Generation generation) {
        Sid sid = generation.sid;
        Process process = generation.process;
        Duration timeout = config().cleanupTimeout();
        Thread.ofVirtual().name("fom-cleanup-" + processName).start(() -> {
            Instant startedAt = null; // set once cleanUp is actually called
            try {
                // Draining and cleanUp() share one budget: the whole cleanup ends within what callers wait for.
                Instant deadline = Instant.now().plus(timeout);
                while (generation.inFlight.get() > 0 && Instant.now().isBefore(deadline)) {
                    Thread.sleep(10);
                }
                if (generation.inFlight.get() > 0) {
                    // Nobody will serve them: fail them now (which cancels their computes).
                    var abandoned = new QueryRejectedException(
                            processName + " is shutting down: the query outlived the cleanup timeout");
                    for (CompletableFuture<Object> reply : List.copyOf(generation.replies)) {
                        reply.completeExceptionally(abandoned);
                    }
                }
                var ctx = new ProcessContextImpl(orPlaceholder(sid), workerExecutor);
                startedAt = Instant.now();
                CompletableFuture<CompletableFuture<Void>> calling = callCleanUp(process, ctx);
                CompletableFuture<Void> cleaning = calling.thenCompose(stage -> stage);
                long remainingMs = Duration.between(Instant.now(), deadline).toMillis();
                try {
                    // A cleanUp that finishes at once did its job, even if draining used up the budget.
                    cleaning.get(Math.max(CLEANUP_GRACE_MS, remainingMs), TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    abandonCleanUp(calling, cleaning);
                    var timedOut = new TimeoutException(processName + " cleanUp did not finish within its share of "
                            + timeout + " (the budget also covers draining in-flight queries)");
                    timedOut.initCause(e);
                    throw timedOut;
                }
                post(new Envelope.CleanupDone(sid, null, startedAt));
            } catch (Throwable t) {
                post(new Envelope.CleanupDone(sid, unwrap(t), startedAt));
            }
        });
    }

    /**
     * Call {@code cleanUp} on a thread of its own, so the budget bounds a body that blocks before
     * returning its stage. It is not interrupted: such a body runs to its end, but the node moves on.
     */
    private CompletableFuture<CompletableFuture<Void>> callCleanUp(Process process, ProcessContextImpl ctx) {
        var calling = new CompletableFuture<CompletableFuture<Void>>();
        Thread.ofVirtual().name("fom-cleanup-call-" + processName).start(() -> {
            try {
                calling.complete(process.cleanUp(ctx).toCompletableFuture());
            } catch (Throwable t) {
                calling.completeExceptionally(t);
            }
        });
        return calling;
    }

    /**
     * The cleanup budget is over. Cancelling tells an implementation that watches for it to stop (a
     * coroutine, an open transaction); the stage may not exist yet if cleanUp is still blocked in the
     * call. A failure reported later would reach nobody, so it is logged.
     */
    private void abandonCleanUp(CompletableFuture<CompletableFuture<Void>> calling, CompletableFuture<Void> cleaning) {
        calling.thenAccept(stage -> stage.cancel(true));
        cleaning.cancel(true);
        calling.thenCompose(stage -> stage).whenComplete((ignored, late) -> {
            if (late != null && !(unwrap(late) instanceof CancellationException)) {
                log.warn("FSM[{}] cleanUp failed after its budget had run out: {}",
                        processName, withCauses(unwrap(late)));
            }
        });
    }

    /**
     * Best-effort {@code cleanUp()} for a {@link Process} that was built but will never serve (its
     * load result arrived stale, after a stop, or its {@code LogLoaded} could not be written).
     */
    private void discardProcess(Process process, Sid sid) {
        Duration timeout = config().cleanupTimeout();
        Thread.ofVirtual().name("fom-discard-" + processName).start(() -> {
            try {
                var ctx = new ProcessContextImpl(orPlaceholder(sid), workerExecutor);
                process.cleanUp(ctx).toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                log.warn("FSM[{}] cleanUp of a discarded process failed: {}", processName, unwrap(t).toString());
            }
        });
    }

    /** Run {@code task} on the log writer, after every append queued before it. */
    private void onWriter(Runnable task) {
        Runnable wrapped = () -> {
            ProcessFSM outer = WRITING.get();
            WRITING.set(this);
            try {
                task.run();
            } catch (Throwable t) {
                log.warn("FSM[{}] log write failed: {}", processName, withCauses(t));
            } finally {
                if (outer == null) WRITING.remove(); else WRITING.set(outer);
            }
        };
        if (WRITING.get() == this) {
            wrapped.run();
            return;
        }
        try {
            logWriter.execute(wrapped);
        } catch (RejectedExecutionException closed) {
            wrapped.run(); // closed: nothing else is written any more
        }
    }

    /** Run {@code append} on the log writer and wait for it: for the paths that may block (stop, RELEASE_FIRST). */
    private <T> T writeNow(Supplier<T> append) {
        if (WRITING.get() == this) return append.get();
        var result = new CompletableFuture<T>();
        onWriter(() -> {
            try {
                result.complete(append.get());
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        try {
            return result.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error error) throw error;
            throw e;
        }
    }

    /**
     * Writer thread: append the replacement's {@code LogInitialized} and hand the outcome to the
     * dispatcher. A candidate an abandoned cycle left open is retired first, so the log never holds two.
     */
    private void writeCandidate(long cycle, int attempt, Map<String, byte[]> props, Sid replaces) {
        if (replacementCycle != cycle) return; // given up meanwhile; the cancel reported the attempt
        if (openCandidate != null) retireCandidateNow(openCandidate);
        Envelope.Write write;
        try {
            Sid sid = persistInitialized(props, replaces);
            if (sid != null) openCandidate = sid;
            write = sid != null ? Envelope.Write.written(sid) : Envelope.Write.REFUSED;
        } catch (RuntimeException e) {
            write = Envelope.Write.failed(e);
        }
        // Posted only while the cycle is current, so the dispatcher sees it before any stop completes.
        // Given up meanwhile, the candidate is retired here, before anything else is written.
        boolean givenUp;
        synchronized (cycleLock) {
            givenUp = replacementCycle != cycle;
            if (!givenUp) post(new Envelope.InitResult(cycle, attempt, props, null, write));
        }
        if (givenUp && write.sid() != null) retireCandidateNow(write.sid());
    }

    /**
     * Writer thread: append the loaded candidate's {@code LogLoaded} (the switch in the log) unless the
     * dispatcher gave the attempt up first, and hand the outcome to the dispatcher.
     */
    private void commitCandidate(long cycle, Sid sid, int attempt, Process process, AtomicInteger gate) {
        if (!gate.compareAndSet(COMMIT_OPEN, COMMIT_RUNNING)) {
            discardProcess(process, sid);
            return;
        }
        Envelope.Write write;
        try {
            boolean loaded = persistLoaded(sid);
            if (loaded && sid.equals(openCandidate)) openCandidate = null;
            gate.set(loaded ? COMMIT_WRITTEN : COMMIT_FAILED);
            write = loaded ? Envelope.Write.written(sid) : Envelope.Write.REFUSED;
        } catch (RuntimeException e) {
            gate.set(COMMIT_FAILED);
            write = Envelope.Write.failed(e);
        }
        post(new Envelope.LoadResult(cycle, sid, attempt, process, null, write));
    }

    /**
     * Give up the replacement's current load attempt unless the writer is already committing it.
     *
     * @return {@code false} if the commit is under way: its result decides the attempt
     */
    private boolean closeCommit() {
        AtomicInteger gate = commitGate;
        return gate == null || gate.compareAndSet(COMMIT_OPEN, COMMIT_CLOSED) || gate.get() == COMMIT_CLOSED;
    }

    /** Retire a candidate that will never serve, on the log writer. */
    private void retireCandidate(Sid candidate) {
        onWriter(() -> retireCandidateNow(candidate));
    }

    /** A stale InitResult carries a candidate the writer appended: retire it if still open. */
    private void retireIfCandidateWritten(Envelope.InitResult r) {
        if (r.write() == null || r.write().sid() == null) return;
        Sid candidate = r.write().sid();
        onWriter(() -> {
            if (candidate.equals(openCandidate)) retireCandidateNow(candidate);
        });
    }

    /** Writer thread. Its clock is newer than the serving version's. */
    private void retireCandidateNow(Sid candidate) {
        if (candidate.equals(openCandidate)) openCandidate = null;
        if (retiredSids.contains(candidate)) return;
        try {
            if (persistDead(candidate)) {
                if (candidate.equals(unretiredCandidate)) unretiredCandidate = null;
                return;
            }
            log.error("FSM[{}] could not retire the unused {}: no longer the leader of the log", processName, candidate);
        } catch (RuntimeException e) {
            log.warn("FSM[{}] could not retire the unused {}: {}", processName, candidate, e.toString());
        }
        unretiredCandidate = candidate;
    }

    /**
     * Retire a state this FSM initialised and then failed to load. Never one from the log at startup
     * (a later attempt may still load it) and never one a {@code Process} has served.
     */
    private void retireUnservedOwnState(Sid sid) {
        if (sid == null || !currentSidIsOurs || retiredSids.contains(sid) || live != null) return;
        try {
            if (!persistDead(sid)) {
                log.error("FSM[{}] could not retire the unloadable state {}: no longer the leader", processName, sid);
            }
        } catch (RuntimeException e) {
            log.warn("FSM[{}] could not retire the unloadable state {}: {}", processName, sid, e.toString());
        }
    }

    /**
     * Wait until every record queued on the log writer so far is written, at most {@code timeout}.
     *
     * @return {@code false} if the writer did not drain in time (a hung backend)
     */
    public boolean awaitLogWriter(Duration timeout) {
        if (WRITING.get() == this) return true;
        var drained = new CompletableFuture<Void>();
        try {
            logWriter.execute(() -> drained.complete(null));
        } catch (RejectedExecutionException closed) {
            return true; // closed: it ran what it had
        }
        try {
            drained.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            // reported below
        }
        log.warn("FSM[{}] log writes still pending after {}; not waiting for them any longer", processName, timeout);
        return false;
    }

    /** Record a re-init request for this process again, after what was just written (see {@link #deferReinit}). */
    private void appendTrigger() {
        onWriter(() -> {
            try {
                appendOrFail(new LogTrigger(0, System.currentTimeMillis(), List.of(processName)));
            } catch (RuntimeException e) {
                log.warn("FSM[{}] could not record the queued re-init: {}", processName, e.toString());
            }
        });
    }

    private Sid persistInitialized(Map<String, byte[]> properties, Sid replaces) {
        var event = new LogInitialized(0, System.currentTimeMillis(), processName, properties, replaces);
        Optional<LogEvent> persisted = appendOrFail(event);
        return persisted.map(e -> ((LogInitialized) e).sid()).orElse(null);
    }

    private boolean persistLoaded(Sid sid) {
        return appendOrFail(new LogLoaded(0, System.currentTimeMillis(), sid)).isPresent();
    }

    private void persistCleanedUp(Sid sid, boolean ok) {
        appendOrFail(new LogCleanedUp(0, System.currentTimeMillis(), sid, ok));
    }

    /** @return {@code false} if the log refused the append: another instance owns it */
    private boolean persistDead(Sid sid) {
        boolean written = appendOrFail(new LogDead(0, System.currentTimeMillis(), sid)).isPresent();
        if (written) retiredSids.add(sid);
        return written;
    }

    /**
     * Every append goes through here. An Error (an OutOfMemoryError serializing a large state, an
     * AssertionError in a backend) becomes an ordinary append failure, which every caller handles;
     * escaping to the dispatcher loop it would strand the node mid-transition.
     */
    private Optional<LogEvent> appendOrFail(LogEvent event) {
        try {
            return logBackend.append(event, instanceId);
        } catch (Error error) {
            throw new IllegalStateException("appending " + event.getClass().getSimpleName() + " failed: " + error,
                    error);
        } catch (RuntimeException e) {
            // An Error may arrive wrapped, e.g. "Self-suppression not permitted" around a reused OOM.
            // That IllegalArgumentException is not the backend refusing the event for good, so it must
            // not send the node Dead without a retry.
            int depth = 0; // bounded: a cause chain can be made cyclic with initCause
            for (Throwable c = e.getCause(); c != null && depth++ < 32; c = c.getCause()) {
                if (c instanceof VirtualMachineError vm) {
                    throw new IllegalStateException("appending " + event.getClass().getSimpleName()
                            + " failed: " + vm, e);
                }
            }
            throw e;
        }
    }

    private void scheduleRetry(Envelope.Phase phase, long cycle, int forAttempt, Duration delay) {
        cancelRetryTick(); // one attempt at a time: an earlier tick is stale anyway
        retryTickTask = schedulePost(new Envelope.RetryTick(phase, cycle, forAttempt), delay);
    }

    private void cancelRetryTick() {
        ScheduledFuture<?> task = retryTickTask;
        retryTickTask = null;
        if (task != null) task.cancel(false);
    }

    private ScheduledFuture<?> schedulePost(Envelope env, Duration delay) {
        try {
            return scheduler.schedule(() -> post(env), delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            log.debug("FSM[{}] scheduler rejected {} (engine closing)", processName, env);
            return null;
        }
    }

    /**
     * Enqueue {@code env} for the dispatcher, or settle it here if the dispatcher has exited.
     * {@code remove} is atomic against the dispatcher's final drain, so each envelope is settled once.
     */
    private void post(Envelope env) {
        mailbox.offer(env);
        if (dispatcherExited && mailbox.remove(env)) {
            rejectAfterDeath(env);
        }
    }

    private void rejectAfterDeath(Envelope env) {
        switch (env) {
            case Envelope.Query q -> rejectOrRepark(q);
            case Envelope.Shutdown s -> {
                awaitLogWriter(config().cleanupTimeout());
                s.reply().complete(null);
            }
            case Envelope.CancelRequest c -> c.reply().complete(null);
            case Envelope.LoadResult r -> {
                if (r.ok()) discardProcess(r.process(), r.sid());
            }
            case Envelope.InitResult r -> retireIfCandidateWritten(r);
            default -> { }
        }
    }

    /** A query reaching a Dead FSM: its successor serves it if a swap replaced this one, else it is rejected. */
    private void rejectOrRepark(Envelope.Query q) {
        if (replaced) {
            repark(q);
        } else {
            q.reply().completeExceptionally(new QueryRejectedException(deadMessage()));
        }
    }

    private void repark(Envelope.Query q) {
        var reparker = queryReparker;
        if (reparker != null) {
            reparker.accept(q);
        } else {
            q.reply().completeExceptionally(new QueryRejectedException(processName + " is shutting down"));
        }
    }

    /**
     * What a query reaching this FSM after it stopped is told. A query looked up just before a pause or
     * removal can still arrive; "Dead" would be wrong then, and callers retry the two differently.
     */
    private String deadMessage() {
        return stoppedOnRequest ? processName + " is shutting down" : processName + " is Dead";
    }

    private void drainStashedQueries(Generation generation) {
        Envelope.Query q;
        while ((q = stash.poll()) != null) {
            runCompute(generation, q);
        }
    }

    private void failStashedQueries(Throwable cause) {
        Envelope.Query q;
        while ((q = stash.poll()) != null) {
            q.reply().completeExceptionally(cause);
        }
    }

    private void completeShutdownReply() {
        if (pendingShutdownReply != null) {
            pendingShutdownReply.complete(null);
            pendingShutdownReply = null;
        }
    }

    /**
     * This process's own undeclared dependency behind {@code failure}, however deeply wrapped, or
     * {@code null}. An init that catches and rethrows must not turn a permanent programming error into
     * a retry loop. Another process's typo can reach us too (inside a rejection, or as a compute's
     * answer); a consumer retries after that like after any failed query.
     */
    private UndeclaredDependencyException undeclaredDependency(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = failure; t != null && seen.add(t); t = t.getCause()) { // a cause cycle ends here
            if (t instanceof UndeclaredDependencyException undeclared && processName.equals(undeclared.requester())) {
                return undeclared;
            }
        }
        return null;
    }

    private void transition(State next) {
        State prev = state;
        state = next;
        if (log.isDebugEnabled()) {
            log.debug("FSM[{}] {} -> {}", processName, prev.name(), next.name());
        }
        observe("onStateTransition", () -> observer.onStateTransition(processName, prev.name(), next.name()));
    }

    private void notifySidPromotion(Sid previousSid, Sid newSid) {
        SidPromotionListener listener = sidPromotionListener;
        if (listener == null || newSid == null) return;
        try {
            listener.onSidPromotion(processName, previousSid, newSid);
        } catch (Throwable t) {
            log.warn("FSM[{}] sid promotion listener threw: {}", processName, t.toString());
        }
    }

    /** Call an observer callback; a throwing observer must not break the engine. */
    private void observe(String callback, Runnable call) {
        try {
            call.run();
        } catch (Throwable t) {
            observerThrew(callback, t);
        }
    }

    private void reportInitFailed(int attempt, Throwable cause) {
        observe("onInitFailed", () -> observer.onInitFailed(processName, attempt, cause));
    }

    private void reportLoadFailed(Sid sid, int attempt, Throwable cause) {
        observe("onLoadFailed", () -> observer.onLoadFailed(processName, sid, attempt, cause));
    }

    /** WARN on the 1st, 2nd, 4th, 8th… failure of each callback, DEBUG otherwise: no log flood. */
    private void observerThrew(String callback, Throwable t) {
        long n = observerFailures.computeIfAbsent(callback, k -> new AtomicLong()).incrementAndGet();
        if (Long.bitCount(n) == 1) {
            log.warn("FSM[{}] observer.{} threw (failure #{} of this callback; repeats are logged at doubling intervals): {}",
                    processName, callback, n, t.toString());
        } else {
            log.debug("FSM[{}] observer.{} threw (failure #{}): {}", processName, callback, n, t.toString());
        }
    }

    private void cancelRunningAttempt() {
        CompletableFuture<?> attempt = runningAttempt;
        // Whoever settles the handle first reports the failure: a cancel from here, now, on the dispatcher.
        if (attempt != null && attempt.cancel(true)) {
            // An explicit stop also interrupts the worker, so an init or load doing its work before
            // returning its stage stops too. A spent budget does not: the handle is settled by then.
            Future<?> worker = runningWorker;
            if (worker != null) worker.cancel(true);
        }
        // Reported whether or not the cancel won: an attempt that completed just before the stop has its
        // result dropped as stale, and would otherwise never get its end callback.
        if (!claimEnd(attemptOpen)) return;
        Phase phase = phase();
        if (phase == null) return;
        if (phase.init()) {
            reportInitFailed(phase.attempt(), new AttemptCancelledException(
                    processName + " init attempt " + phase.attempt() + " cancelled"));
        } else {
            reportLoadFailed(phase.loadSid(), phase.attempt(), new AttemptCancelledException(
                    processName + " load attempt " + phase.attempt() + " cancelled"));
        }
    }

    private void terminate(Throwable cause) {
        State.Serving replacing = replacing();
        if (replacing != null) {
            abandonReplacement(replacing, cause); // a failed re-init: the old version serves on
            return;
        }
        cancelRunningAttempt();
        cancelReinitRetry();
        cancelRetryTick();
        // Waiting queries belong to other processes: give them our rejection, not our failure, or a
        // consumer inherits e.g. a permanent UndeclaredDependencyException for a typo it does not have.
        failStashedQueries(new QueryRejectedException(deadMessage(), cause));
        transition(State.Dead.INSTANCE);
        servingReady.completeExceptionally(cause);
        completeShutdownReply();
    }

    private void terminateOnLostLeadership(String phase) {
        var ex = new LeadershipLostException("Lost leadership for " + processName + " during " + phase);
        // Reported like any other failure of that phase, so metrics and traces see it.
        Phase running = phase();
        if (claimEnd(attemptOpen)) {
            int attempt = running != null ? running.attempt() : 1;
            if ("load".equals(phase)) {
                reportLoadFailed(running != null ? running.loadSid() : currentSid, attempt, ex);
            } else {
                reportInitFailed(attempt, ex);
            }
        }
        terminateOnLostLeadership(phase, ex);
    }

    /** The same end, for a failure the caller has already reported to the observer. */
    private void terminateOnLostLeadership(String phase, RuntimeException cause) {
        log.error("FSM[{}] lost leadership during {}", processName, phase);
        lastFailure = describe(cause); // NodeReport.lastException says why the node is Dead
        terminate(cause);
    }

    private List<String> dependencyNames() {
        var names = new ArrayList<String>(node.dependencies().size());
        node.dependencies().forEach(d -> names.add(d.name()));
        return names;
    }

    /** {@code sid}, or a placeholder at clock 0 (the log's LogLeader, never a process state) when there is none. */
    private Sid orPlaceholder(Sid sid) {
        return sid != null ? sid : new Sid(processName, 0);
    }

    private static String describe(Throwable failure) {
        Throwable cause = unwrap(failure);
        return cause.getClass().getName() + ": " + cause.getMessage();
    }

    /** {@code t} and its causes, so a storage error behind a wrapped append failure shows in the log. */
    private static String withCauses(Throwable t) {
        StringBuilder sb = new StringBuilder(t.toString());
        for (Throwable c = t.getCause(); c != null && c != t; c = c.getCause()) {
            sb.append(" <- ").append(c);
            if (c.getCause() == c) break;
        }
        return sb.toString();
    }

    private static Throwable unwrap(Throwable t) {
        Throwable c = t;
        while ((c instanceof ExecutionException || c instanceof CompletionException) && c.getCause() != null) {
            c = c.getCause();
        }
        return c;
    }
}
