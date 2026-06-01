package io.fom.fsm;

import io.fom.EngineConfig;
import io.fom.ProcessNode;
import io.fom.Sid;
import io.fom.api.EngineObserver;
import io.fom.api.InitInProgressException;
import io.fom.api.InitializationTimeoutException;
import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.api.QueryRejectedException;
import io.fom.log.LogBackend;
import io.fom.log.LogCleanedUp;
import io.fom.log.LogDead;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLoaded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-process actor-like FSM driven by a single virtual-thread dispatcher
 * that reads {@link Envelope}s from a mailbox and pattern-matches against
 * the current {@link State}. {@code compute()} for inbound queries runs on
 * a separate virtual thread, so the dispatcher is never blocked by user code.
 *
 * <p>Lifecycle: {@code NotPresent → Initializing → Loading → Serving → CleaningUp → Dead}.
 * On {@code init} failure: exponential backoff with jitter, retrying until
 * {@code init.timeout} elapses. On {@code load} failure: retries up to
 * {@code maxLoadRetries}, then falls back to {@code Initializing}.</p>
 */
public final class ProcessFSM implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ProcessFSM.class);

    private final String processName;
    private final ProcessNode node;
    private final java.util.function.Supplier<EngineConfig> configSource;
    private final LogBackend logBackend;
    private final String instanceId;
    private final ScheduledExecutorService scheduler;
    private final ProcessRouter router;
    private final EngineObserver observer;

    private final ExecutorService workerExecutor;
    private final LinkedBlockingQueue<Envelope> mailbox = new LinkedBlockingQueue<>();
    private final ArrayDeque<Envelope.Query> stash = new ArrayDeque<>();
    private final AtomicInteger inFlightQueries = new AtomicInteger();
    private final CompletableFuture<Void> servingReady = new CompletableFuture<>();
    private final CompletableFuture<Void> dead = new CompletableFuture<>();
    private final Thread dispatcher;

    private volatile State state = State.NotPresent.INSTANCE;
    private volatile Sid currentSid;

    /** Set once the process has been loaded; cleared on exit from Serving. Owned by the dispatcher. */
    private volatile Process liveProcess;

    /** Pending re-init cause stashed while we are still in Initializing/Loading. */
    private ReinitCause pendingReinitCause;

    /** Sid of the previously live state — passed to the promotion listener after Serving. */
    private Sid liveProcessPreviousSid;

    /** Listener for Sid promotions — used by {@code GraphMachine} to fire reactive cascade. */
    private volatile SidPromotionListener sidPromotionListener;

    /** Set when entering Initializing for the first time; reset only on fresh entry. */
    private Instant initStartedAt;

    /** Per-attempt timestamps for observer durations. */
    private Instant initAttemptStartedAt;
    private Instant loadAttemptStartedAt;

    /** Pending Shutdown reply — completed when state reaches Dead. */
    private CompletableFuture<Void> pendingShutdownReply;

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
                      java.util.function.Supplier<EngineConfig> configSource,
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
        this.workerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.dispatcher = Thread.ofVirtual()
                .name("fom-fsm-" + processName)
                .start(this::dispatcherLoop);
    }

    private EngineConfig config() {
        return configSource.get();
    }

    private BackoffPolicy initBackoff() {
        return new BackoffPolicy(config().backoffMin(), config().backoffMax());
    }

    private BackoffPolicy loadBackoff() {
        return new BackoffPolicy(config().backoffMin(), config().backoffMax());
    }

    /** Begin a cold start — go through {@code init} for the first time. */
    public void spawnInit() {
        mailbox.offer(Envelope.SpawnInit.INSTANCE);
    }

    /**
     * Begin a warm start from an existing log record — skip {@code init} and
     * try to {@code load} the given properties. {@code clock} is the clock
     * of the source {@code LogInitialized} event.
     */
    public void spawnLoad(int clock, Map<String, byte[]> properties) {
        currentSid = new Sid(processName, clock);
        mailbox.offer(new Envelope.SpawnLoad(properties));
    }

    /** Completes when state first reaches {@link State.Serving}, or exceptionally on terminal failure. */
    public CompletionStage<Void> servingReady() {
        return servingReady;
    }

    public CompletionStage<Object> submitQuery(Object message, long deadlineEpochMillis) {
        Objects.requireNonNull(message, "message");
        var reply = new CompletableFuture<Object>();
        if (state instanceof State.Dead) {
            reply.completeExceptionally(new QueryRejectedException(processName + " is Dead"));
            return reply;
        }
        var env = new Envelope.Query(UUID.randomUUID(), message, reply, deadlineEpochMillis);
        if (!mailbox.offer(env)) {
            reply.completeExceptionally(new QueryRejectedException("Mailbox full"));
        }
        return reply;
    }

    public CompletionStage<Void> shutdown(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        var reply = new CompletableFuture<Void>();
        mailbox.offer(new Envelope.Shutdown(reply, false));
        return reply.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Like {@link #shutdown(Duration)} but writes {@code LogDead} for the
     * current Sid before cleanup. Used by {@code GraphMachine} when a node
     * is being replaced via {@code Engine.newGraph}: the retired Sid is
     * permanently removed so the next JVM restart cold-inits the new node
     * definition rather than warm-loading stale state.
     */
    public CompletionStage<Void> shutdownReplace(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        var reply = new CompletableFuture<Void>();
        mailbox.offer(new Envelope.Shutdown(reply, true));
        return reply.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public State currentState() {
        return state;
    }

    public Sid currentSid() {
        return currentSid;
    }

    public String processName() {
        return processName;
    }

    /** Mailbox size — exposed for {@code introspect()} / metrics. */
    public int mailboxSize() {
        return mailbox.size();
    }

    public int inFlightQueries() {
        return inFlightQueries.get();
    }

    /** Install (or replace) the listener that fires after every Serving transition with a new Sid. */
    public void setSidPromotionListener(SidPromotionListener listener) {
        this.sidPromotionListener = listener;
    }

    /** External trigger entry — places a {@link Envelope.ReinitRequest} on the mailbox. */
    public void submitReinit(ReinitCause cause) {
        Objects.requireNonNull(cause, "cause");
        if (state instanceof State.Dead) return;
        mailbox.offer(new Envelope.ReinitRequest(cause));
    }

    /** Cancel an in-flight init/load for {@code targetSid}; no-op for any other state. */
    public CompletionStage<Void> cancelInit(Sid targetSid) {
        Objects.requireNonNull(targetSid, "targetSid");
        var reply = new CompletableFuture<Void>();
        mailbox.offer(new Envelope.CancelRequest(targetSid, reply));
        return reply;
    }

    @Override
    public void close() {
        try {
            shutdown(config().defaultCleanupTimeout())
                    .toCompletableFuture()
                    .get(config().defaultCleanupTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception ignored) {
            // best-effort
        }
        // Bounded shutdown: interrupt stuck init/load/compute workers instead of
        // blocking forever inside ExecutorService.close().
        workerExecutor.shutdownNow();
        try {
            workerExecutor.awaitTermination(
                    config().defaultCleanupTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ───────────────── dispatcher loop ─────────────────

    private void dispatcherLoop() {
        try {
            while (!(state instanceof State.Dead)) {
                Envelope env = mailbox.take();
                try {
                    handle(env);
                } catch (Throwable t) {
                    log.error("FSM[{}] uncaught error handling {}", processName, env, t);
                }
            }
            dead.complete(null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("FSM[{}] dispatcher interrupted", processName);
            // Don't leave callers blocked on our futures after an interrupt.
            failStashedQueries(new QueryRejectedException(processName + " dispatcher interrupted"));
            if (!servingReady.isDone()) {
                servingReady.completeExceptionally(
                        new IllegalStateException(processName + " interrupted before Serving"));
            }
            if (pendingShutdownReply != null) {
                pendingShutdownReply.complete(null);
                pendingShutdownReply = null;
            }
            dead.complete(null);
        }
    }

    private void handle(Envelope env) {
        switch (env) {
            case Envelope.SpawnInit ignored -> onSpawnInit();
            case Envelope.SpawnLoad s -> onSpawnLoad(s.properties());
            case Envelope.InitResult r -> onInitResult(r);
            case Envelope.LoadResult r -> onLoadResult(r);
            case Envelope.Query q -> onQuery(q);
            case Envelope.RetryTick t -> onRetryTick(t);
            case Envelope.Shutdown s -> onShutdown(s);
            case Envelope.CleanupDone c -> onCleanupDone(c);
            case Envelope.ReinitRequest r -> onReinitRequest(r);
            case Envelope.CancelRequest c -> onCancelRequest(c);
        }
    }

    // ───────────────── handlers ─────────────────

    private void onSpawnInit() {
        if (!(state instanceof State.NotPresent)) {
            log.debug("FSM[{}] ignoring SpawnInit in state {}", processName, state.name());
            return;
        }
        initStartedAt = Instant.now();
        transition(new State.Initializing(1, null));
        dispatchInitWorker(1);
    }

    private void onSpawnLoad(Map<String, byte[]> properties) {
        if (!(state instanceof State.NotPresent)) {
            log.debug("FSM[{}] ignoring SpawnLoad in state {}", processName, state.name());
            return;
        }
        transition(new State.Loading(1, null, properties));
        dispatchLoadWorker(properties, 1);
    }

    private void onInitResult(Envelope.InitResult r) {
        if (!(state instanceof State.Initializing init) || init.attempt() != r.attempt()) {
            log.debug("FSM[{}] stale InitResult attempt={} in state {}", processName, r.attempt(), state.name());
            return;
        }
        if (r.ok()) {
            Sid newSid = persistInitialized(r.properties());
            if (newSid == null) {
                terminateOnLostLeadership("init");
                return;
            }
            currentSid = newSid;
            if (initAttemptStartedAt != null) {
                Duration initDuration = Duration.between(initAttemptStartedAt, Instant.now());
                log.info("Process '{}' init completed in {} ms (sid={})",
                        processName, initDuration.toMillis(), newSid.clock());
                try {
                    observer.onInitCompleted(processName, newSid, initDuration);
                } catch (Throwable t) {
                    log.warn("FSM[{}] observer.onInitCompleted threw: {}", processName, t.toString());
                }
            }
            transition(new State.Loading(1, null, r.properties()));
            dispatchLoadWorker(r.properties(), 1);
        } else {
            log.warn("FSM[{}] init attempt {} failed: {}", processName, init.attempt(), unwrap(r.failure()).toString());
            Duration elapsed = Duration.between(initStartedAt, Instant.now());
            if (elapsed.compareTo(config().defaultInitTimeout()) >= 0) {
                var ex = new InitializationTimeoutException(
                        "Init for " + processName + " exceeded " + config().defaultInitTimeout()
                                + " (total budget); last failure: " + r.failure());
                ex.initCause(unwrap(r.failure()));
                terminate(ex);
                return;
            }
            int nextAttempt = init.attempt() + 1;
            transition(new State.Initializing(nextAttempt, r.failure()));
            scheduleRetry(Envelope.Phase.INIT, nextAttempt, initBackoff().delayFor(init.attempt()));
        }
    }

    private void onLoadResult(Envelope.LoadResult r) {
        if (!(state instanceof State.Loading loading) || loading.attempt() != r.attempt()) {
            log.debug("FSM[{}] stale LoadResult attempt={} in state {}", processName, r.attempt(), state.name());
            return;
        }
        if (r.ok()) {
            if (!persistLoaded()) {
                terminateOnLostLeadership("load");
                return;
            }
            if (loadAttemptStartedAt != null && currentSid != null) {
                Duration loadDuration = Duration.between(loadAttemptStartedAt, Instant.now());
                log.info("Process '{}' load completed in {} ms — now Serving (sid={})",
                        processName, loadDuration.toMillis(), currentSid.clock());
                try {
                    observer.onLoadCompleted(processName, currentSid, loadDuration);
                } catch (Throwable t) {
                    log.warn("FSM[{}] observer.onLoadCompleted threw: {}", processName, t.toString());
                }
            }
            Sid previousSid = liveProcessPreviousSid;
            liveProcess = r.process();
            transition(State.Serving.INSTANCE);
            drainStashedQueries(r.process());
            if (!servingReady.isDone()) servingReady.complete(null);
            notifySidPromotion(previousSid, currentSid);
            // If a re-init was requested while we were Initializing/Loading, apply it now.
            ReinitCause pending = pendingReinitCause;
            if (pending != null) {
                pendingReinitCause = null;
                beginReinit(pending);
            }
        } else {
            log.warn("FSM[{}] load attempt {} failed: {}", processName, loading.attempt(), unwrap(r.failure()).toString());
            try {
                observer.onLoadFailed(processName, currentSid, loading.attempt(), r.failure());
            } catch (Throwable t) {
                log.warn("FSM[{}] observer.onLoadFailed threw: {}", processName, t.toString());
            }
            if (loading.attempt() >= config().maxLoadRetries()) {
                log.info("FSM[{}] load fallback to init after {} failed attempts", processName, loading.attempt());
                initStartedAt = Instant.now();
                transition(new State.Initializing(1, null));
                dispatchInitWorker(1);
                return;
            }
            int nextAttempt = loading.attempt() + 1;
            transition(new State.Loading(nextAttempt, r.failure(), loading.properties()));
            scheduleRetry(Envelope.Phase.LOAD, nextAttempt, loadBackoff().delayFor(loading.attempt()));
        }
    }

    private void onQuery(Envelope.Query q) {
        switch (state) {
            case State.Serving ignored0 -> dispatchComputeWorker(liveProcess, q);
            case State.Initializing ignored1 -> stash.add(q);
            case State.Loading ignored2 -> stash.add(q);
            case State.NotPresent ignored3 ->
                    q.reply().completeExceptionally(new InitInProgressException(processName));
            case State.CleaningUp ignored4 ->
                    q.reply().completeExceptionally(new QueryRejectedException(processName + " is shutting down"));
            case State.Dead ignored5 ->
                    q.reply().completeExceptionally(new QueryRejectedException(processName + " is Dead"));
        }
    }

    private void onReinitRequest(Envelope.ReinitRequest r) {
        ReinitCause cause = r.cause();
        switch (state) {
            case State.Serving ignored -> beginReinit(cause);
            case State.Initializing ignored2 -> pendingReinitCause = mergeCause(pendingReinitCause, cause);
            case State.Loading ignored3 -> pendingReinitCause = mergeCause(pendingReinitCause, cause);
            case State.NotPresent ignored4 -> pendingReinitCause = mergeCause(pendingReinitCause, cause);
            case State.CleaningUp ignored5 -> {
                // Already retiring; the post-cleanup branch will route back into Initializing.
                // Remember the cause so the next pass picks it up.
                pendingReinitCause = mergeCause(pendingReinitCause, cause);
            }
            case State.Dead ignored6 -> log.debug("FSM[{}] dropping reinit after Dead", processName);
        }
    }

    private void onCancelRequest(Envelope.CancelRequest c) {
        if (currentSid == null || !c.targetSid().equals(currentSid)) {
            c.reply().complete(null);
            return;
        }
        if (state instanceof State.Initializing || state instanceof State.Loading) {
            log.info("FSM[{}] cancelling current init/load for {}", processName, c.targetSid());
            var ex = new InitInProgressException("Cancelled by admin: " + processName);
            terminate(ex);
        }
        c.reply().complete(null);
    }

    /** Transition Serving → CleaningUp(forReinit=true) and write {@code LogDead} before cleanup. */
    private void beginReinit(ReinitCause cause) {
        Sid sid = currentSid;
        if (sid == null) {
            log.warn("FSM[{}] reinit requested without a currentSid; ignoring", processName);
            return;
        }
        log.info("FSM[{}] beginning reinit cycle (cause={})", processName, cause);
        persistDead(sid);
        Process process = liveProcess;
        liveProcessPreviousSid = sid;
        liveProcess = null;
        transition(new State.CleaningUp(sid, State.CleanupMode.REINIT));
        if (process != null) {
            triggerCleanupWhenIdle(process);
        } else {
            mailbox.offer(new Envelope.CleanupDone(null));
        }
    }

    private static ReinitCause mergeCause(ReinitCause existing, ReinitCause incoming) {
        return existing != null ? existing : incoming;
    }

    private void onRetryTick(Envelope.RetryTick t) {
        switch (t.phase()) {
            case INIT -> {
                if (state instanceof State.Initializing init && init.attempt() == t.forAttempt()) {
                    dispatchInitWorker(t.forAttempt());
                }
            }
            case LOAD -> {
                if (state instanceof State.Loading load && load.attempt() == t.forAttempt()) {
                    dispatchLoadWorker(load.properties(), t.forAttempt());
                }
            }
        }
    }

    private void onShutdown(Envelope.Shutdown s) {
        if (state instanceof State.Dead) {
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
        State prev = state;
        Sid retiringSid = currentSid != null ? currentSid : new Sid(processName, 0);
        State.CleanupMode mode = s.retireSid() ? State.CleanupMode.REPLACE : State.CleanupMode.SHUTDOWN;
        if (s.retireSid() && currentSid != null) {
            // Write LogDead before cleanup so a crash mid-cleanup still records
            // the Sid as retired on restart.
            persistDead(currentSid);
        }
        transition(new State.CleaningUp(retiringSid, mode));
        failStashedQueries(new QueryRejectedException(processName + " is shutting down"));

        if (prev instanceof State.Serving) {
            Process p = liveProcess;
            liveProcess = null;
            if (p != null) {
                triggerCleanupWhenIdle(p);
            } else {
                mailbox.offer(new Envelope.CleanupDone(null));
            }
        } else {
            mailbox.offer(new Envelope.CleanupDone(null));
        }
    }

    private void onCleanupDone(Envelope.CleanupDone c) {
        if (!(state instanceof State.CleaningUp cleaning)) {
            return;
        }
        boolean ok = c.failure() == null;
        if (!ok) {
            log.warn("FSM[{}] cleanup failed: {}", processName, c.failure().toString());
        }
        Sid retiredSid = cleaning.sidBeingRetired();
        persistCleanedUp(retiredSid, ok);
        try {
            observer.onCleanupCompleted(processName, retiredSid, ok, Duration.ZERO);
        } catch (Throwable t) {
            log.warn("FSM[{}] observer.onCleanupCompleted threw: {}", processName, t.toString());
        }

        switch (cleaning.mode()) {
            case REINIT -> {
                // Recycle: start a new init cycle for the same process node.
                currentSid = null;
                liveProcess = null;
                initStartedAt = Instant.now();
                transition(new State.Initializing(1, null));
                dispatchInitWorker(1);
            }
            case SHUTDOWN, REPLACE -> {
                // SHUTDOWN: no LogDead (idempotent restart preserved).
                // REPLACE:  LogDead already written in onShutdown before cleanup.
                // Both terminate at Dead.
                transition(State.Dead.INSTANCE);
                if (pendingShutdownReply != null) {
                    pendingShutdownReply.complete(null);
                    pendingShutdownReply = null;
                }
                if (!servingReady.isDone()) {
                    servingReady.completeExceptionally(
                            new IllegalStateException(processName + " transitioned to Dead before Serving"));
                }
            }
        }
    }

    // ───────────────── workers ─────────────────

    private void dispatchInitWorker(int attempt) {
        Sid ctxSid = currentSid != null ? currentSid : new Sid(processName, 0);
        List<String> deps = dependencyNames();
        long timeoutMs = config().defaultInitTimeout().toMillis();
        Instant attemptStart = Instant.now();
        if (attempt == 1) {
            log.info("Process '{}' init started", processName);
        } else {
            log.info("Process '{}' init retry (attempt {})", processName, attempt);
        }
        try {
            observer.onInitStarted(processName, attempt);
        } catch (Throwable t) {
            log.warn("FSM[{}] observer.onInitStarted threw: {}", processName, t.toString());
        }
        workerExecutor.submit(() -> {
            try {
                var ctx = QueryableContextImpl.forPhase(ctxSid, workerExecutor, deps, router);
                Map<String, byte[]> props = invokeInit(ctx)
                        .toCompletableFuture()
                        .get(timeoutMs, TimeUnit.MILLISECONDS);
                Objects.requireNonNull(props, "init returned null properties");
                mailbox.offer(new Envelope.InitResult(attempt, props, null));
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                safeOnInitFailed(attempt, cause);
                mailbox.offer(new Envelope.InitResult(attempt, null, cause));
            } catch (TimeoutException e) {
                safeOnInitFailed(attempt, e);
                mailbox.offer(new Envelope.InitResult(attempt, null, e));
            } catch (Throwable t) {
                safeOnInitFailed(attempt, t);
                mailbox.offer(new Envelope.InitResult(attempt, null, t));
            }
        });
        initAttemptStartedAt = attemptStart;
    }

    private void safeOnInitFailed(int attempt, Throwable cause) {
        try {
            observer.onInitFailed(processName, attempt, cause);
        } catch (Throwable t) {
            log.warn("FSM[{}] observer.onInitFailed threw: {}", processName, t.toString());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CompletionStage<Map<String, byte[]>> invokeInit(QueryableContext ctx) {
        Object factory = node.initFactory().get();
        Serializable param = node.param();
        if (param == null) {
            return ((ProcessInitializer) factory).init(ctx);
        }
        ParamProcessInitializer paramInit = (ParamProcessInitializer) factory;
        return paramInit.init(ctx, param);
    }

    private void dispatchLoadWorker(Map<String, byte[]> properties, int attempt) {
        Sid sid = currentSid;
        List<String> deps = dependencyNames();
        long timeoutMs = config().defaultLoadTimeout().toMillis();
        loadAttemptStartedAt = Instant.now();
        if (attempt == 1) {
            log.info("Process '{}' load started", processName);
        } else {
            log.info("Process '{}' load retry (attempt {})", processName, attempt);
        }
        try {
            observer.onLoadStarted(processName, sid, attempt);
        } catch (Throwable t) {
            log.warn("FSM[{}] observer.onLoadStarted threw: {}", processName, t.toString());
        }
        workerExecutor.submit(() -> {
            try {
                var ctx = QueryableContextImpl.forPhase(
                        sid != null ? sid : new Sid(processName, 0), workerExecutor, deps, router);
                Process process = invokeLoad(ctx, properties)
                        .toCompletableFuture()
                        .get(timeoutMs, TimeUnit.MILLISECONDS);
                Objects.requireNonNull(process, "load returned null process");
                mailbox.offer(new Envelope.LoadResult(attempt, process, null));
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                mailbox.offer(new Envelope.LoadResult(attempt, null, cause));
            } catch (TimeoutException e) {
                mailbox.offer(new Envelope.LoadResult(attempt, null, e));
            } catch (Throwable t) {
                mailbox.offer(new Envelope.LoadResult(attempt, null, t));
            }
        });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CompletionStage<Process> invokeLoad(QueryableContext ctx, Map<String, byte[]> props) {
        Object factory = node.loadFactory().get();
        Serializable param = node.param();
        if (param == null) {
            return ((ProcessLoader) factory).load(ctx, props);
        }
        ParamProcessLoader paramLoad = (ParamProcessLoader) factory;
        return paramLoad.load(ctx, props, param);
    }

    private void dispatchComputeWorker(Process process, Envelope.Query q) {
        Sid sid = currentSid;
        List<String> deps = dependencyNames();
        inFlightQueries.incrementAndGet();
        try {
            observer.onQuerySent(processName, q.queryId(), q.message().getClass());
        } catch (Throwable t) {
            log.warn("FSM[{}] observer.onQuerySent threw: {}", processName, t.toString());
        }
        Instant computeStart = Instant.now();
        workerExecutor.submit(() -> {
            try {
                var ctx = QueryableContextImpl.forQuery(sid, workerExecutor, deps, q.deadlineEpochMillis(), router);
                CompletionStage<?> stage = process.compute(ctx, q.message());
                stage.whenComplete((res, err) -> {
                    inFlightQueries.decrementAndGet();
                    Duration elapsed = Duration.between(computeStart, Instant.now());
                    if (err != null) {
                        Throwable cause = unwrap(err);
                        safeOnQueryFailed(q.queryId(), "exception", cause);
                        q.reply().completeExceptionally(cause);
                    } else {
                        safeOnQueryCompleted(q.queryId(), elapsed);
                        q.reply().complete(res);
                    }
                    safeOnComputeDuration(elapsed);
                });
            } catch (Throwable t) {
                inFlightQueries.decrementAndGet();
                safeOnQueryFailed(q.queryId(), "exception", t);
                q.reply().completeExceptionally(t);
            }
        });
    }

    private void safeOnQueryCompleted(UUID queryId, Duration duration) {
        try {
            observer.onQueryCompleted(processName, queryId, duration);
        } catch (Throwable t) {
            log.warn("FSM[{}] observer.onQueryCompleted threw: {}", processName, t.toString());
        }
    }

    private void safeOnQueryFailed(UUID queryId, String reason, Throwable cause) {
        try {
            observer.onQueryFailed(processName, queryId, reason, cause);
        } catch (Throwable t) {
            log.warn("FSM[{}] observer.onQueryFailed threw: {}", processName, t.toString());
        }
    }

    private void safeOnComputeDuration(Duration duration) {
        try {
            observer.onComputeDuration(processName, duration);
        } catch (Throwable t) {
            log.warn("FSM[{}] observer.onComputeDuration threw: {}", processName, t.toString());
        }
    }

    private void triggerCleanupWhenIdle(Process process) {
        Sid sid = currentSid;
        Duration timeout = config().defaultCleanupTimeout();
        Thread.ofVirtual().name("fom-cleanup-" + processName).start(() -> {
            try {
                Instant drainDeadline = Instant.now().plus(timeout);
                while (inFlightQueries.get() > 0 && Instant.now().isBefore(drainDeadline)) {
                    Thread.sleep(10);
                }
                var ctx = new ProcessContextImpl(sid != null ? sid : new Sid(processName, 0), workerExecutor);
                process.cleanUp(ctx).toCompletableFuture()
                        .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                mailbox.offer(new Envelope.CleanupDone(null));
            } catch (Throwable t) {
                mailbox.offer(new Envelope.CleanupDone(unwrap(t)));
            }
        });
    }

    // ───────────────── log writes ─────────────────

    private Sid persistInitialized(Map<String, byte[]> properties) {
        long now = System.currentTimeMillis();
        LogInitialized event = new LogInitialized(0, now, processName, properties);
        Optional<LogEvent> persisted = logBackend.append(event, instanceId);
        if (persisted.isEmpty()) return null;
        LogInitialized actual = (LogInitialized) persisted.get();
        return actual.sid();
    }

    private boolean persistLoaded() {
        long now = System.currentTimeMillis();
        LogLoaded event = new LogLoaded(0, now, currentSid);
        return logBackend.append(event, instanceId).isPresent();
    }

    private void persistCleanedUp(Sid sid, boolean ok) {
        long now = System.currentTimeMillis();
        logBackend.append(new LogCleanedUp(0, now, sid, ok), instanceId);
    }

    private void persistDead(Sid sid) {
        long now = System.currentTimeMillis();
        logBackend.append(new LogDead(0, now, sid), instanceId);
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

    // ───────────────── helpers ─────────────────

    private void scheduleRetry(Envelope.Phase phase, int forAttempt, Duration delay) {
        scheduler.schedule(
                () -> mailbox.offer(new Envelope.RetryTick(phase, forAttempt)),
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void drainStashedQueries(Process process) {
        Envelope.Query q;
        while ((q = stash.poll()) != null) {
            dispatchComputeWorker(process, q);
        }
    }

    private void failStashedQueries(Throwable cause) {
        Envelope.Query q;
        while ((q = stash.poll()) != null) {
            q.reply().completeExceptionally(cause);
        }
    }

    private void transition(State next) {
        State prev = state;
        state = next;
        if (log.isDebugEnabled()) {
            log.debug("FSM[{}] {} -> {}", processName, prev.name(), next.name());
        }
        try {
            observer.onStateTransition(processName, prev.name(), next.name());
        } catch (Throwable t) {
            log.warn("FSM[{}] observer.onStateTransition threw: {}", processName, t.toString());
        }
    }

    private void terminate(Throwable cause) {
        failStashedQueries(cause);
        transition(State.Dead.INSTANCE);
        if (!servingReady.isDone()) {
            servingReady.completeExceptionally(cause);
        }
        if (pendingShutdownReply != null) {
            pendingShutdownReply.complete(null);
            pendingShutdownReply = null;
        }
    }

    private void terminateOnLostLeadership(String phase) {
        var ex = new IllegalStateException("Lost leadership for " + processName + " during " + phase);
        log.error("FSM[{}] lost leadership during {}", processName, phase);
        terminate(ex);
    }

    private List<String> dependencyNames() {
        var names = new ArrayList<String>(node.dependencies().size());
        node.dependencies().forEach(d -> names.add(d.name()));
        return names;
    }

    private static Throwable unwrap(Throwable t) {
        Throwable c = t;
        while (c instanceof ExecutionException && c.getCause() != null) {
            c = c.getCause();
        }
        return c;
    }
}
