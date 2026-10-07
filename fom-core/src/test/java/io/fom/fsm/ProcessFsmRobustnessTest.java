package io.fom.fsm;

import io.fom.EngineConfig;
import io.fom.ProcessNode;
import io.fom.Sid;
import io.fom.SnapshotPolicy;
import io.fom.SnapshotResult;
import io.fom.api.Process;
import io.fom.api.ProcessContext;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogCleanedUp;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import java.util.function.Supplier;

/**
 * Regression tests for FSM lifecycle bugs: throwing log appends, replies that
 * never completed after {@code Dead}, shutdown racing a reinit cleanup, loaded
 * processes dropped without {@code cleanUp}, and a cleanup budget that could
 * double.
 */
class ProcessFsmRobustnessTest {

    static final AtomicInteger CLEANUPS = new AtomicInteger();
    /** Set when the engine cancels the stage a load returned — what a cooperative loader watches for. */
    static final java.util.concurrent.atomic.AtomicBoolean LOAD_CANCELLED =
            new java.util.concurrent.atomic.AtomicBoolean();
    static final AtomicLong CLEANUP_END = new AtomicLong();
    static volatile long cleanupSleepMs;
    static volatile long computeSleepMs;
    static volatile CountDownLatch cleanupStarted;
    static volatile CountDownLatch loadGate;
    static volatile CountDownLatch loadEntered;

    @BeforeEach
    void reset() {
        CLEANUPS.set(0);
        LOAD_CANCELLED.set(false);
        CLEANUP_END.set(0);
        cleanupSleepMs = 0;
        computeSleepMs = 0;
        cleanupStarted = new CountDownLatch(1);
        loadGate = null;
        loadEntered = new CountDownLatch(1);
    }

    private static EngineConfig cfg(Duration cleanup) {
        return cfg(cleanup, Duration.ofSeconds(5));
    }

    private static EngineConfig cfg(Duration cleanup, Duration loadTimeout) {
        return new EngineConfig(Duration.ofSeconds(5), loadTimeout, cleanup, Duration.ofSeconds(5), Duration.ofMillis(100), Duration.ofMillis(10),
                Duration.ofMillis(100), 1, SnapshotPolicy.Disabled.INSTANCE);
    }

    /** InMemory backend whose appends can be made to throw, like a disk that fills up. */
    static final class FlakyBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        volatile Predicate<LogEvent> failIf = e -> false;

        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int clock) { return delegate.get(clock); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String id) { return delegate.compact(events, id); }
        @Override public void close() { delegate.close(); }

        @Override
        public Optional<LogEvent> append(LogEvent event, String id) {
            if (failIf.test(event)) throw new RuntimeException("Append failed (injected)");
            return delegate.append(event, id);
        }
    }

    private static ProcessFSM fsm(LogBackend backend, Duration cleanup) {
        return fsm(backend, cfg(cleanup));
    }

    private static ProcessFSM fsm(LogBackend backend, EngineConfig config) {
        backend.append(new LogLeader(0, System.currentTimeMillis(), "i1"), "i1");
        Supplier<Init> factory = Init::new;
        var node = new ProcessNode("P", List.of(), null, factory, factory);
        ProcessRouter router = (n, q, d, parent) ->CompletableFuture.failedFuture(new IllegalStateException());
        return new ProcessFSM("P", node, config, backend, "i1",
                Executors.newSingleThreadScheduledExecutor(), router);
    }

    /**
     * A load that merely outruns its budget is cancelled too: an uncancelled stage would
     * keep its work (a coroutine, a poll loop) running for the rest of the JVM's life.
     */
    @Test
    @Timeout(30)
    void a_load_that_outruns_its_budget_is_cancelled() throws Exception {
        loadGate = new CountDownLatch(1); // the load only ends when its budget does
        ProcessFSM f = fsm(new InMemoryLogBackend(), cfg(Duration.ofSeconds(5), Duration.ofMillis(300)));
        try {
            f.spawnInit();
            assertThat(loadEntered.await(5, TimeUnit.SECONDS)).isTrue();

            // The fix cancels at ~300 ms; without it nothing cancels until the FSM gives up
            // on its whole init budget seconds later.
            await().atMost(Duration.ofSeconds(2)).until(LOAD_CANCELLED::get);
        } finally {
            loadGate.countDown(); // do not leave a common-pool worker blocked for the JVM's life
            f.shutdown(Duration.ofMillis(200));
        }
    }

    private static Object query(ProcessFSM f) throws Exception {
        return f.submitQuery("q", System.currentTimeMillis() + 5_000, null).toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(30)
    void failing_LogInitialized_append_is_retried_and_recovers() throws Exception {
        var backend = new FlakyBackend();
        backend.failIf = e -> e instanceof LogInitialized;
        ProcessFSM f = fsm(backend, Duration.ofSeconds(5));
        f.spawnInit();
        await().atMost(Duration.ofSeconds(3)).until(() ->
                f.currentState() instanceof State.Initializing init && init.attempt() > 1);

        backend.failIf = e -> false; // the disk recovers
        f.servingReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertThat(f.currentState()).isInstanceOf(State.Serving.class);
        assertThat(query(f)).isEqualTo("ok");
    }

    @Test
    @Timeout(30)
    void failing_LogCleanedUp_append_does_not_wedge_a_reinit() throws Exception {
        var backend = new FlakyBackend();
        ProcessFSM f = fsm(backend, Duration.ofSeconds(5));
        f.spawnInit();
        f.servingReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
        Sid first = f.currentSid();

        backend.failIf = e -> e instanceof LogCleanedUp;
        f.submitReinit(new ReinitCause.Triggered("x"));
        await().atMost(Duration.ofSeconds(5)).until(() ->
                f.currentState() instanceof State.Serving && f.currentSid() != null && !f.currentSid().equals(first));
        assertThat(query(f)).isEqualTo("ok");
    }

    @Test
    @Timeout(30)
    void cancelInit_on_a_dead_fsm_completes() throws Exception {
        loadGate = new CountDownLatch(1); // load never finishes on its own
        ProcessFSM f = fsm(new InMemoryLogBackend(), Duration.ofSeconds(5));
        f.spawnInit();
        await().atMost(Duration.ofSeconds(5)).until(() -> f.currentState() instanceof State.Loading);
        Sid sid = f.currentSid();
        f.cancelInit(sid).toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertThat(f.currentState()).isInstanceOf(State.Dead.class);

        f.cancelInit(sid).toCompletableFuture().get(2, TimeUnit.SECONDS);
        f.shutdown(Duration.ofSeconds(1)).toCompletableFuture().get(2, TimeUnit.SECONDS);
        loadGate.countDown();
    }

    @Test
    @Timeout(30)
    void shutdown_during_reinit_cleanup_waits_for_the_real_cleanup() throws Exception {
        cleanupSleepMs = 1500;
        var backend = new InMemoryLogBackend();
        // The RELEASE_FIRST shape: CleaningUp(REINIT) when the shutdown lands. KEEP_OLD: see below.
        ProcessFSM f = fsm(backend, cfg(Duration.ofSeconds(10)).withReinitStrategy(io.fom.ReinitStrategy.RELEASE_FIRST));
        f.spawnInit();
        f.servingReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
        f.submitReinit(new ReinitCause.Triggered("x"));
        assertThat(cleanupStarted.await(5, TimeUnit.SECONDS)).isTrue();

        f.shutdown(Duration.ofSeconds(10)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(CLEANUP_END.get()).as("cleanUp finished before the shutdown reply").isNotZero();
        assertThat(f.currentState()).isInstanceOf(State.Dead.class);
        long cleanedUpEvents = Arrays.stream(backend.getBetween(0, backend.length()))
                .filter(e -> e instanceof LogCleanedUp).count();
        assertThat(cleanedUpEvents).isEqualTo(1);
    }

    /** KEEP_OLD: the replaced version is still cleaning up when the shutdown lands; the reply waits for it. */
    @Test
    @Timeout(30)
    void shutdown_while_a_replaced_version_cleans_up_waits_for_it_and_the_live_one() throws Exception {
        cleanupSleepMs = 1500;
        var backend = new InMemoryLogBackend();
        ProcessFSM f = fsm(backend, Duration.ofSeconds(10));
        f.spawnInit();
        f.servingReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
        Sid first = f.currentSid();
        f.submitReinit(new ReinitCause.Triggered("x"));
        assertThat(cleanupStarted.await(5, TimeUnit.SECONDS)).as("the old version is being cleaned up").isTrue();
        assertThat(f.currentSid()).as("the new version already serves").isNotEqualTo(first);

        f.shutdown(Duration.ofSeconds(10)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(f.currentState()).isInstanceOf(State.Dead.class);
        assertThat(CLEANUPS).as("both versions cleaned up before the reply").hasValue(2);
        var cleaned = Arrays.stream(backend.getBetween(0, backend.length()))
                .filter(e -> e instanceof LogCleanedUp).map(e -> ((LogCleanedUp) e).sid()).toList();
        assertThat(cleaned).containsExactlyInAnyOrder(first, f.currentSid());
    }

    /** A shutdown cancels a load in progress, exactly like a pause, a removal or cancelInit. */
    @Test
    @Timeout(30)
    void shutting_down_during_a_load_cancels_it() throws Exception {
        loadGate = new CountDownLatch(1);
        // A generous load budget on purpose: with a short one the attempt can time out before the
        // shutdown lands, and then the engine legitimately releases the late Process — which would
        // make the assertion below flaky instead of meaningful.
        ProcessFSM f = fsm(new InMemoryLogBackend(), cfg(Duration.ofSeconds(5), Duration.ofSeconds(60)));
        f.spawnInit();
        assertThat(loadEntered.await(5, TimeUnit.SECONDS)).isTrue();
        f.shutdown(Duration.ofSeconds(5)).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertThat(f.currentState()).isInstanceOf(State.Dead.class);
        assertThat(LOAD_CANCELLED).as("the loader is told to stop").isTrue();

        loadGate.countDown();
        Thread.sleep(300);
        assertThat(CLEANUPS).as("the late Process of an uncooperative loader is its own to release")
                .hasValue(0);
    }

    /**
     * Cancelling tells the load to stop (see {@link io.fom.api.ProcessLoader}): a loader that
     * watches for it releases what it holds. This fixture ignores the cancellation and finishes
     * anyway, so its late {@code Process} is dropped — by contract, that loader owns it.
     */
    @Test
    @Timeout(30)
    void cancelling_a_load_cancels_it_and_a_loader_that_ignores_that_owns_its_late_result()
            throws Exception {
        loadGate = new CountDownLatch(1);
        ProcessFSM f = fsm(new InMemoryLogBackend(), cfg(Duration.ofSeconds(5), Duration.ofSeconds(60)));
        f.spawnInit();
        assertThat(loadEntered.await(5, TimeUnit.SECONDS)).isTrue();
        f.cancelInit(f.currentSid()).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertThat(f.currentState()).isInstanceOf(State.Dead.class);
        assertThat(LOAD_CANCELLED).isTrue();

        loadGate.countDown();
        Thread.sleep(500);
        assertThat(CLEANUPS).hasValue(0);
    }

    @Test
    @Timeout(30)
    void drain_and_cleanup_share_one_budget_so_the_reply_does_not_time_out() throws Exception {
        computeSleepMs = 1800; // drain eats most of the 2s budget
        cleanupSleepMs = 1000; // cleanUp would overrun it
        var backend = new InMemoryLogBackend();
        ProcessFSM f = fsm(backend, Duration.ofSeconds(2));
        f.spawnInit();
        f.servingReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
        f.submitQuery("slow", System.currentTimeMillis() + 10_000, null);
        await().atMost(Duration.ofSeconds(2)).until(() -> f.inFlightQueries() == 1);

        // Completes normally (no TimeoutException): the overrunning cleanUp is
        // cut off inside the budget and recorded as not ok.
        f.shutdown(Duration.ofSeconds(2)).toCompletableFuture().get(3, TimeUnit.SECONDS);

        assertThat(f.currentState()).isInstanceOf(State.Dead.class);
        assertThat(Arrays.stream(backend.getBetween(0, backend.length()))
                .filter(e -> e instanceof LogCleanedUp c && !c.ok())).hasSize(1);
    }

    static final class Init implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of("k", new byte[]{1}));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            CountDownLatch gate = loadGate;
            if (gate == null) {
                return CompletableFuture.completedFuture(new Proc());
            }
            CompletableFuture<Process> loading = CompletableFuture.supplyAsync(() -> {
                loadEntered.countDown();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return new Proc();
            });
            loading.whenComplete((p, e) -> {
                if (e instanceof java.util.concurrent.CancellationException) LOAD_CANCELLED.set(true);
            });
            return loading;
        }
    }

    static final class Proc implements Process {
        @Override
        public CompletionStage<?> compute(QueryableContext ctx, Object query) {
            return CompletableFuture.supplyAsync(() -> {
                sleep(computeSleepMs);
                return "ok";
            });
        }

        @Override
        public CompletionStage<Void> cleanUp(ProcessContext ctx) {
            CLEANUPS.incrementAndGet();
            cleanupStarted.countDown();
            return CompletableFuture.runAsync(() -> {
                sleep(cleanupSleepMs);
                CLEANUP_END.set(System.currentTimeMillis());
            });
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
