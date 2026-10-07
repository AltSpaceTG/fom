package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogEvent;
import io.fom.log.LogPaused;
import io.fom.log.LogTrigger;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Edge cases of the control plane and the query path found by the round-22 field test: records
 * about a paused node landing out of order, a trigger outliving its process's removal, computes
 * run for queries nobody waits for, a slow observer on the reply path, retries squeezed out of the
 * init budget, and a trigger racing {@code close()}.
 */
class ControlPlaneEdgeCaseTest {

    static final Map<String, AtomicInteger> INITS = new ConcurrentHashMap<>();
    static final Map<String, AtomicInteger> COMPUTES = new ConcurrentHashMap<>();
    static final Map<String, Long> INIT_DELAY = new ConcurrentHashMap<>();
    static final Map<String, Long> LOAD_DELAY = new ConcurrentHashMap<>();
    static final Map<String, Long> CLEANUP_DELAY = new ConcurrentHashMap<>();
    static final java.util.Set<String> CLEANUPS_BEGUN = ConcurrentHashMap.newKeySet();
    static final java.util.Set<String> FAIL_INIT = ConcurrentHashMap.newKeySet();
    /** Blocks the calling thread inside init/load before any stage is returned (a JDBC call without a timeout). */
    static final Map<String, Long> SYNC_BLOCK_INIT = new ConcurrentHashMap<>();
    static final Map<String, Long> SYNC_BLOCK_LOAD = new ConcurrentHashMap<>();
    /** Replaces a node's compute; the default answers "<name>-ok" at once. */
    static final Map<String, java.util.function.Function<Object, CompletionStage<?>>> COMPUTE = new ConcurrentHashMap<>();

    @BeforeEach
    void reset() {
        INITS.clear();
        COMPUTES.clear();
        INIT_DELAY.clear();
        LOAD_DELAY.clear();
        CLEANUP_DELAY.clear();
        CLEANUPS_BEGUN.clear();
        FAIL_INIT.clear();
        SYNC_BLOCK_INIT.clear();
        SYNC_BLOCK_LOAD.clear();
        COMPUTE.clear();
    }

    private static void blockUninterruptibly(long millis) {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < until) {
            try {
                Thread.sleep(Math.max(1, TimeUnit.NANOSECONDS.toMillis(until - System.nanoTime())));
            } catch (InterruptedException ignored) {
                // like a blocking driver call that does not react to interrupts
            }
        }
    }

    private static CompletableFuture<Void> after(long millis) {
        var done = new CompletableFuture<Void>();
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(millis);
                done.complete(null);
            } catch (InterruptedException e) {
                done.completeExceptionally(e);
            }
        });
        return done;
    }

    static final class Node implements ProcessInitializer, ProcessLoader {
        private final String name;

        Node(String name) {
            this.name = name;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INITS.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            blockUninterruptibly(SYNC_BLOCK_INIT.getOrDefault(name, 0L));
            if (FAIL_INIT.contains(name)) {
                return CompletableFuture.failedFuture(new IllegalStateException(name + " init fails (injected)"));
            }
            long delay = INIT_DELAY.getOrDefault(name, 0L);
            if (delay == 0) return CompletableFuture.completedFuture(Map.of());
            var result = new CompletableFuture<Map<String, byte[]>>();
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(delay);
                    result.complete(Map.of());
                } catch (InterruptedException e) {
                    result.completeExceptionally(e);
                }
            });
            return result;
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            Process process = new Process() {
                @Override
                public CompletionStage<?> compute(QueryableContext c, Object q) {
                    COMPUTES.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
                    var custom = COMPUTE.get(name);
                    return custom != null ? custom.apply(q) : CompletableFuture.completedFuture(name + "-ok");
                }

                @Override
                public CompletionStage<Void> cleanUp(io.fom.api.ProcessContext c) {
                    CLEANUPS_BEGUN.add(name);
                    long delay = CLEANUP_DELAY.getOrDefault(name, 0L);
                    return delay == 0 ? CompletableFuture.completedFuture(null) : after(delay);
                }
            };
            blockUninterruptibly(SYNC_BLOCK_LOAD.getOrDefault(name, 0L));
            long delay = LOAD_DELAY.getOrDefault(name, 0L);
            return delay == 0 ? CompletableFuture.completedFuture(process) : after(delay).thenApply(v -> process);
        }
    }

    /** An in-memory log whose appends can be held back, to force an interleaving. */
    static final class HoldingBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        volatile Predicate<LogEvent> hold = e -> false;
        volatile CountDownLatch release = new CountDownLatch(0);
        final CountDownLatch held = new CountDownLatch(1);

        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return delegate.compact(events, leader); }
        @Override public void close() { }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
            if (hold.test(event)) {
                held.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return delegate.append(event, leaderInstanceId);
        }
    }

    private static EngineConfig cfg() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(1),
                Duration.ofSeconds(10), Duration.ofMillis(10),
                Duration.ofMillis(20), Duration.ofMillis(200), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    private static GraphBuilder add(GraphBuilder b, String name, String... deps) {
        return b.add(name, (Supplier<ProcessInitializer>) () -> new Node(name),
                (Supplier<ProcessLoader>) () -> new Node(name), deps);
    }

    private static Graph graph(String... names) {
        var b = new GraphBuilder();
        for (String name : names) add(b, name);
        return b.build();
    }

    private static String state(Engine e, String name) throws Exception {
        return e.introspect().toCompletableFuture().get().graph().nodes().stream()
                .filter(n -> n.name().equals(name))
                .map(EngineReport.NodeReport::state)
                .findFirst().orElse("absent");
    }

    /** The phase of the new version being made beside the serving one, or {@code "none"}. */
    private static String replacement(Engine e, String name) throws Exception {
        return e.introspect().toCompletableFuture().get().graph().nodes().stream()
                .filter(n -> n.name().equals(name))
                .map(n -> n.replacement() == null ? "none" : n.replacement())
                .findFirst().orElse("absent");
    }

    private static int count(Map<String, AtomicInteger> counts, String name) {
        return counts.getOrDefault(name, new AtomicInteger()).get();
    }

    /**
     * A trigger marks a paused node stale while it is being resumed. Its {@code LogPaused} must not
     * land after the resume's {@code LogResumed}: replayed in that order, a restart brings the
     * node back paused although the operator resumed it.
     */
    @Test
    @Timeout(30)
    void a_trigger_racing_a_resume_does_not_bring_the_node_back_paused_after_a_restart() throws Exception {
        var backend = new HoldingBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("A"));
            e.pause(List.of("A"));
            backend.release = new CountDownLatch(1);
            backend.hold = ev -> ev instanceof LogPaused p && p.stale();
            var trigger = Thread.ofVirtual().start(() -> e.trigger("A", "go"));
            assertThat(backend.held.await(5, TimeUnit.SECONDS)).as("the stale mark is being written").isTrue();
            var resume = Thread.ofVirtual().start(() -> e.resume(List.of("A")));
            Thread.sleep(200); // let the resume reach its own log write
            backend.hold = ev -> false;
            backend.release.countDown();
            trigger.join(5_000);
            resume.join(10_000);
            await().atMost(Duration.ofSeconds(5)).until(() -> "Serving".equals(state(e, "A")));
        } finally {
            e.close();
        }
        var restarted = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            restarted.newGraph(graph("A"));
            assertThat(state(restarted, "A")).as("the operator resumed it; the log must say so").isEqualTo("Serving");
        } finally {
            restarted.close();
        }
    }

    /** The same race against a removal: a name added back must not come back paused. */
    @Test
    @Timeout(30)
    void a_trigger_racing_a_removal_does_not_leave_the_readded_process_paused() throws Exception {
        var backend = new HoldingBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Keep", "A"));
            e.pause(List.of("A"));
            backend.release = new CountDownLatch(1);
            backend.hold = ev -> ev instanceof LogPaused p && p.stale();
            var trigger = Thread.ofVirtual().start(() -> e.trigger("A", "go"));
            assertThat(backend.held.await(5, TimeUnit.SECONDS)).isTrue();
            var remove = Thread.ofVirtual().start(() -> e.remove(List.of("A")));
            Thread.sleep(200);
            backend.hold = ev -> false;
            backend.release.countDown();
            trigger.join(5_000);
            remove.join(10_000);
            e.newGraph(graph("Keep", "A"));
            assertThat(state(e, "A")).isEqualTo("Serving");
        } finally {
            e.close();
        }
        var restarted = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            restarted.newGraph(graph("Keep", "A"));
            assertThat(state(restarted, "A")).isEqualTo("Serving");
        } finally {
            restarted.close();
        }
    }

    /** A trigger still inside its dedup window when its process is removed dies with that process. */
    @Test
    @Timeout(30)
    void a_trigger_pending_when_its_process_is_removed_does_not_reinit_it_once_added_back() throws Exception {
        var config = cfg().withDedupWindow(Duration.ofMillis(600));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Keep", "P"));
            e.trigger("P", "go"); // waits out a 600 ms window
            e.remove(List.of("P"));
            e.newGraph(graph("Keep", "P")); // cold-inits P: its state was retired
            int afterReadd = count(INITS, "P");
            Thread.sleep(1_000); // well past the window
            assertThat(count(INITS, "P")).as("the removed process's trigger must not re-init the new one")
                    .isEqualTo(afterReadd);
        } finally {
            e.close();
        }
    }

    /**
     * A query that timed out while its node re-initialised is not computed once the node serves.
     * Only a RELEASE_FIRST re-init stashes queries; a KEEP_OLD one answers them from the old version.
     */
    @Test
    @Timeout(30)
    void a_query_that_timed_out_while_stashed_is_never_computed() throws Exception {
        var config = cfg().withQueryTimeout(Duration.ofMillis(200)).withReinitStrategy(ReinitStrategy.RELEASE_FIRST);
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("A"));
            INIT_DELAY.put("A", 800L);
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(2)).until(() -> "Initializing".equals(state(e, "A")));
            var stashed = e.queryProcess("A", "q").toCompletableFuture();
            assertThatThrownBy(() -> stashed.get(5, TimeUnit.SECONDS)).hasRootCauseInstanceOf(
                    java.util.concurrent.TimeoutException.class);
            await().atMost(Duration.ofSeconds(5)).until(() -> "Serving".equals(state(e, "A")));
            Thread.sleep(200);
            assertThat(count(COMPUTES, "A")).as("nobody waits for that answer any more").isZero();
        } finally {
            e.close();
        }
    }

    /** A slow {@code onComputeDuration} must not hold back the answer, let alone time it out. */
    @Test
    @Timeout(30)
    void a_slow_compute_duration_observer_does_not_delay_the_reply() throws Exception {
        var config = cfg().withQueryTimeout(Duration.ofMillis(300));
        var observer = new EngineObserver() {
            @Override
            public void onComputeDuration(String processName, Duration duration) {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(graph("A"));
            long started = System.nanoTime();
            assertThat(e.queryProcess("A", "q").toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("A-ok");
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(300));
        } finally {
            e.close();
        }
    }

    /** Fails its first attempt at once; the second needs a little real work, then succeeds. */
    static final class SecondTimeLucky implements ProcessInitializer, ProcessLoader {
        static final AtomicInteger ATTEMPTS = new AtomicInteger();

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            if (ATTEMPTS.incrementAndGet() == 1) {
                return CompletableFuture.failedFuture(new IllegalStateException("transient (injected)"));
            }
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return Map.<String, byte[]>of();
            }, r -> Thread.ofVirtual().start(r));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    /**
     * When the backoff is longer than the budget left, the retry must still get real time to run.
     * It used to be scheduled at the very end of the budget with a 1 ms attempt timeout, so a
     * transient failure killed the node although its next attempt would have succeeded.
     */
    @Test
    @Timeout(30)
    void a_retry_whose_backoff_does_not_fit_still_gets_time_to_run() throws Exception {
        SecondTimeLucky.ATTEMPTS.set(0);
        // Floor = backoffMin / 2 = 5 s, so the retry runs with ~1 s left for its 200 ms of work.
        var config = cfg().withInitTimeout(Duration.ofSeconds(6))
                .withBackoff(Duration.ofSeconds(10), Duration.ofSeconds(20));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var b = new GraphBuilder();
            b.add("T", (Supplier<ProcessInitializer>) SecondTimeLucky::new, (Supplier<ProcessLoader>) SecondTimeLucky::new);
            e.newGraph(b.build());
            assertThat(state(e, "T")).isEqualTo("Serving");
            assertThat(SecondTimeLucky.ATTEMPTS.get()).isEqualTo(2);
        } finally {
            e.close();
        }
    }

    /** Works for a second, then fails, every time. */
    static final class SlowFailure implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("boom-S (injected)");
            }, r -> Thread.ofVirtual().start(r));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("never"));
        }
    }

    /** Giving up happens within the budget and reports the failure the init really had. */
    @Test
    @Timeout(30)
    void an_init_that_keeps_failing_gives_up_within_its_budget_with_its_own_failure() throws Exception {
        var config = cfg().withInitTimeout(Duration.ofSeconds(3))
                .withBackoff(Duration.ofSeconds(5), Duration.ofSeconds(10));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var b = new GraphBuilder();
            b.add("S", (Supplier<ProcessInitializer>) SlowFailure::new, (Supplier<ProcessLoader>) SlowFailure::new);
            long started = System.nanoTime();
            var failure = new AtomicReference<Throwable>();
            try {
                e.newGraph(b.build());
            } catch (RuntimeException ex) {
                failure.set(ex);
            }
            await().atMost(Duration.ofSeconds(10)).until(() -> "Dead".equals(state(e, "S")));
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(3_300));
            assertThat(failure.get()).isNotNull();
            assertThat(failure.get().getMessage()).contains("ran out of its PT3S budget").contains("boom-S");
        } finally {
            e.close();
        }
    }

    /** A trigger that reaches the machine after close() is recorded; the caller gets no raw rejection. */
    @Test
    @Timeout(30)
    void a_trigger_racing_close_does_not_throw_a_raw_rejection() throws Exception {
        var backend = new HoldingBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        e.newGraph(graph("A", "B"));
        backend.release = new CountDownLatch(1);
        backend.hold = ev -> ev instanceof LogTrigger;
        var outcome = new AtomicReference<Throwable>();
        var trigger = Thread.ofVirtual().start(() -> {
            try {
                e.trigger(Map.of("A", "x", "B", "y"));
            } catch (Throwable t) {
                outcome.set(t);
            }
        });
        assertThat(backend.held.await(5, TimeUnit.SECONDS)).isTrue();
        e.close();
        backend.hold = ev -> false;
        backend.release.countDown();
        trigger.join(5_000);
        assertThat(outcome.get()).as("the trigger is in the log; the next start applies it").isNull();
    }

    @Test
    void blank_names_are_rejected_and_a_duplicate_dependency_is_named() {
        assertThatThrownBy(() -> add(new GraphBuilder(), " ")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
        assertThatThrownBy(() -> ProcessRef.of("\t")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> add(new GraphBuilder(), "C", "A", "A"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("'A'");
        assertThatCode(() -> add(new GraphBuilder(), "C", "A", "B")).doesNotThrowAnyException();
    }

    // ---- round 23 --------------------------------------------------------------------------

    static final AtomicReference<Engine> OUTER = new AtomicReference<>();

    /**
     * A compute that queries its own process through the outer Engine builds a chain of queries.
     * Once the root times out, cancelling it must reach the whole chain: the cancellation used to
     * recurse one stack frame per level, overflow a few thousand levels down (silently), and leave
     * the rest of the chain growing until the JVM ran out of memory.
     */
    @Test
    @Timeout(60)
    void a_chain_of_self_queries_stops_once_its_root_times_out() throws Exception {
        var config = cfg().withQueryTimeout(Duration.ofMillis(300));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        OUTER.set(e);
        COMPUTE.put("R", q -> count(COMPUTES, "R") > 300_000 // a cap, so a regression fails instead of OOMing
                ? new CompletableFuture<>()
                : OUTER.get().queryProcess("R", q));
        try {
            e.newGraph(graph("R"));
            assertThatThrownBy(() -> e.queryProcess("R", "go").toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class);
            Thread.sleep(1_000);
            int settled = count(COMPUTES, "R");
            Thread.sleep(1_000);
            assertThat(count(COMPUTES, "R")).as("nobody waits for the chain any more").isEqualTo(settled);
        } finally {
            e.close();
            OUTER.set(null);
        }
    }

    /** A timed-out compute whose stage ignores cancel() must not hold the next re-init for the cleanup budget. */
    @Test
    @Timeout(30)
    void an_uncancellable_abandoned_compute_does_not_hold_a_reinit() throws Exception {
        var config = cfg().withQueryTimeout(Duration.ofMillis(200)).withCleanupTimeout(Duration.ofSeconds(4));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        COMPUTE.put("A", q -> new CompletableFuture<>().minimalCompletionStage());
        try {
            e.newGraph(graph("A"));
            assertThatThrownBy(() -> e.queryProcess("A", "q").toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class);
            COMPUTE.remove("A");
            int before = count(INITS, "A");
            long started = System.nanoTime();
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(10)).until(() -> count(INITS, "A") > before
                    && "Serving".equals(state(e, "A")));
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(2_000));
        } finally {
            e.close();
        }
    }

    /** Nor may a compute that blocks inside compute() after its query timed out. */
    @Test
    @Timeout(30)
    void a_compute_blocked_after_its_query_timed_out_does_not_hold_a_reinit() throws Exception {
        var config = cfg().withQueryTimeout(Duration.ofMillis(200)).withCleanupTimeout(Duration.ofSeconds(4));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        COMPUTE.put("A", q -> {
            try {
                Thread.sleep(6_000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return CompletableFuture.completedFuture("late");
        });
        try {
            e.newGraph(graph("A"));
            assertThatThrownBy(() -> e.queryProcess("A", "q").toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class);
            COMPUTE.remove("A");
            int before = count(INITS, "A");
            long started = System.nanoTime();
            e.trigger("A", "go");
            await().atMost(Duration.ofSeconds(10)).until(() -> count(INITS, "A") > before
                    && "Serving".equals(state(e, "A")));
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(2_000));
        } finally {
            e.close();
        }
    }

    static final java.util.List<Long> ATTEMPT_TIMES = new java.util.concurrent.CopyOnWriteArrayList<>();

    static final class AlwaysFails implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            ATTEMPT_TIMES.add(System.nanoTime());
            return CompletableFuture.failedFuture(new IllegalStateException("upstream down (injected)"));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("never"));
        }
    }

    /** Retries squeezed into the end of the budget still keep the backoff floor (half of backoffMin). */
    @Test
    @Timeout(30)
    void retries_at_the_end_of_the_budget_keep_the_backoff_floor() throws Exception {
        ATTEMPT_TIMES.clear();
        var config = cfg().withInitTimeout(Duration.ofSeconds(2))
                .withBackoff(Duration.ofMillis(400), Duration.ofSeconds(60));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var b = new GraphBuilder();
            b.add("F", (Supplier<ProcessInitializer>) AlwaysFails::new, (Supplier<ProcessLoader>) AlwaysFails::new);
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);
            await().atMost(Duration.ofSeconds(5)).until(() -> "Dead".equals(state(e, "F")));
            for (int i = 1; i < ATTEMPT_TIMES.size(); i++) {
                assertThat(Duration.ofNanos(ATTEMPT_TIMES.get(i) - ATTEMPT_TIMES.get(i - 1)))
                        .as("gap before attempt %d of %d", i + 1, ATTEMPT_TIMES.size())
                        .isGreaterThanOrEqualTo(Duration.ofMillis(190));
            }
        } finally {
            e.close();
        }
    }

    /** A trigger that arrives while the removal is still stopping the process is dropped with it. */
    @Test
    @Timeout(30)
    void a_trigger_arriving_during_a_removal_does_not_reinit_the_readded_process() throws Exception {
        var config = cfg().withDedupWindow(Duration.ofMillis(800)).withCleanupTimeout(Duration.ofSeconds(3));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Keep", "P"));
            CLEANUP_DELAY.put("P", 600L);
            var removal = Thread.ofVirtual().start(() -> e.remove(List.of("P")));
            await().atMost(Duration.ofSeconds(2)).until(() -> CLEANUPS_BEGUN.contains("P"));
            try {
                e.trigger("P", "go"); // lands while P is being stopped
            } catch (IllegalArgumentException refused) {
                // also fine: the name is already gone
            }
            removal.join(5_000);
            CLEANUP_DELAY.clear();
            e.newGraph(graph("Keep", "P"));
            int afterReadd = count(INITS, "P");
            Thread.sleep(1_300);
            assertThat(count(INITS, "P")).isEqualTo(afterReadd);
        } finally {
            e.close();
        }
    }

    /** Nor does a pending trigger re-init a changed node's replacement, which cold-inits anyway. */
    @Test
    @Timeout(30)
    void a_pending_trigger_does_not_reinit_a_changed_nodes_replacement() throws Exception {
        var config = cfg().withDedupWindow(Duration.ofMillis(500));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Keep", "X"));
            e.trigger("X", "go");
            var changed = new GraphBuilder();
            add(changed, "Keep");
            add(changed, "X", "Keep"); // a new dependency: X is changed
            e.newGraph(changed.build());
            int afterSwap = count(INITS, "X");
            Thread.sleep(1_000);
            assertThat(count(INITS, "X")).isEqualTo(afterSwap);
        } finally {
            e.close();
        }
    }

    /**
     * Paused while its re-init was loading, then removed: the removal must retire the new state
     * (already persisted), not the old one (already retired), or a restart warm-loads the removed node.
     */
    @Test
    @Timeout(30)
    void removing_a_node_paused_while_loading_retires_its_new_state() throws Exception {
        var backend = new InMemoryLogBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Keep", "X"));
            LOAD_DELAY.put("X", 1_500L);
            e.trigger("X", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Loading".equals(replacement(e, "X")));
            e.pause(List.of("X"));
            e.remove(List.of("X"));
        } finally {
            e.close();
        }
        LOAD_DELAY.clear();
        int before = count(INITS, "X");
        var restarted = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            restarted.newGraph(graph("Keep", "X"));
            assertThat(count(INITS, "X")).as("a removed process comes back with a fresh init").isEqualTo(before + 1);
        } finally {
            restarted.close();
        }
    }

    /** A pause whose node finishes its re-init before it stops warm-loads that state on resume: one init. */
    @Test
    @Timeout(30)
    void a_pause_landing_as_a_reinit_completes_does_not_reinit_again_on_resume() throws Exception {
        var backend = new HoldingBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("X"));
            INIT_DELAY.put("X", 300L);
            e.trigger("X", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "X")));
            backend.release = new CountDownLatch(1);
            backend.hold = ev -> ev instanceof LogPaused p && p.stale();
            var pause = Thread.ofVirtual().start(() -> e.pause(List.of("X")));
            assertThat(backend.held.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(700); // the re-init finishes while the pause is being recorded
            backend.hold = ev -> false;
            backend.release.countDown();
            pause.join(5_000);
            INIT_DELAY.clear();
            int beforeResume = count(INITS, "X");
            e.resume(List.of("X"));
            Thread.sleep(500);
            assertThat(count(INITS, "X")).as("its re-init already happened").isEqualTo(beforeResume);
        } finally {
            e.close();
        }
    }

    /** Closing during a re-init's init records no LogCleanedUp for a Sid that never existed. */
    @Test
    @Timeout(30)
    void closing_mid_init_records_no_phantom_cleanup() throws Exception {
        var backend = new InMemoryLogBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        e.newGraph(graph("X"));
        INIT_DELAY.put("X", 2_000L);
        e.trigger("X", "go");
        await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "X")));
        e.close();
        for (LogEvent event : backend.getBetween(0, backend.length())) {
            if (event instanceof io.fom.log.LogCleanedUp cleaned) {
                assertThat(cleaned.sid().clock()).as("LogCleanedUp names a real state").isNotZero();
            }
        }
    }

    /** A paused process left out of the graph at restart, then added back, is not paused after the next restart. */
    @Test
    @Timeout(30)
    void a_paused_process_omitted_at_restart_and_added_back_is_not_paused_later() throws Exception {
        var backend = new InMemoryLogBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        e.newGraph(graph("Keep", "T"));
        e.pause(List.of("T"));
        e.close();
        var second = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        second.newGraph(graph("Keep"));
        second.newGraph(graph("Keep", "T"));
        assertThat(state(second, "T")).isEqualTo("Serving");
        second.close();
        var third = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            third.newGraph(graph("Keep", "T"));
            assertThat(state(third, "T")).isEqualTo("Serving");
        } finally {
            third.close();
        }
    }

    /** An interrupted resume has stopped the FSM it started before it returns, so a retry cannot overlap it. */
    @Test
    @Timeout(30)
    void an_interrupted_resume_has_stopped_what_it_started_before_it_throws() throws Exception {
        var loadFailures = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
        var observer = new EngineObserver() {
            @Override
            public void onLoadFailed(String processName, io.fom.Sid sid, int attempt, Throwable cause) {
                loadFailures.add(cause);
            }
        };
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(graph("X"));
            e.pause(List.of("X"));
            LOAD_DELAY.put("X", 1_000L);
            var failure = new AtomicReference<Throwable>();
            var seenAtThrow = new AtomicInteger(-1);
            var resume = Thread.ofVirtual().start(() -> {
                try {
                    e.resume(List.of("X"));
                } catch (RuntimeException ex) {
                    seenAtThrow.set(loadFailures.size());
                    failure.set(ex);
                }
            });
            Thread.sleep(200);
            resume.interrupt();
            resume.join(10_000);
            assertThat(failure.get()).hasMessageContaining("Interrupted while waiting for 'X'");
            assertThat(seenAtThrow.get()).as("the cancelled load was over before the call returned").isEqualTo(1);
            LOAD_DELAY.clear();
            e.resume(List.of("X"));
            assertThat(state(e, "X")).isEqualTo("Serving");
        } finally {
            e.close();
        }
    }

    // ---- round 24 --------------------------------------------------------------------------

    /** A reactive consumer removed while the cascade runs must not leave the consumers after it stale. */
    @Test
    @Timeout(30)
    void a_consumer_removed_during_a_cascade_does_not_stop_it_for_the_others() throws Exception {
        var backend = new HoldingBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var b = new GraphBuilder();
            add(b, "Src");
            add(b, "Aaa", "Src");
            add(b, "Bbb", "Src");
            e.newGraph(b.build());
            int bbbBefore = count(INITS, "Bbb");
            backend.release = new CountDownLatch(1);
            backend.hold = ev -> ev instanceof io.fom.log.LogDependencyChanged d
                    && d.sid().processName().equals("Aaa");
            e.trigger("Src", "go");
            assertThat(backend.held.await(5, TimeUnit.SECONDS)).as("the cascade reached Aaa").isTrue();
            backend.hold = ev -> false;
            var removal = Thread.ofVirtual().start(() -> e.remove(List.of("Aaa")));
            removal.join(10_000);
            backend.release.countDown();
            await().atMost(Duration.ofSeconds(5)).until(() -> count(INITS, "Bbb") > bbbBefore);
        } finally {
            e.close();
        }
    }

    /** Appends and range reads on the in-memory log stay cheap as it grows (no copy of the whole log). */
    @Test
    @Timeout(60)
    void the_in_memory_log_appends_and_scans_in_linear_time() {
        var backend = new InMemoryLogBackend();
        backend.append(new io.fom.log.LogLeader(0, System.currentTimeMillis(), "me"), "me");
        long started = System.nanoTime();
        for (int i = 0; i < 150_000; i++) {
            backend.append(new LogTrigger(0, System.currentTimeMillis(), List.of("P")), "me");
        }
        for (int from = 0; from < backend.length(); from += 1_000) {
            backend.getBetween(from, Math.min(backend.length(), from + 1_000));
        }
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5));
    }

    /**
     * RELEASE_FIRST: a node paused mid re-init has no live state: it reports no Sid, as it will after
     * a restart. KEEP_OLD: the old state was never retired, so the pause keeps it and is stale.
     */
    @Test
    @Timeout(30)
    void a_node_paused_mid_reinit_reports_no_retired_sid() throws Exception {
        var e = new Engine(cfg().withReinitStrategy(ReinitStrategy.RELEASE_FIRST), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("X"));
            INIT_DELAY.put("X", 1_000L);
            e.trigger("X", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(state(e, "X")));
            e.pause(List.of("X"));
            var node = e.introspect().toCompletableFuture().get().graph().nodes().stream()
                    .filter(n -> n.name().equals("X")).findFirst().orElseThrow();
            assertThat(node.state()).isEqualTo("Paused");
            assertThat(node.sid()).as("its old state is retired").isNull();
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_node_paused_mid_replacement_keeps_its_serving_sid_and_is_stale() throws Exception {
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("X"));
            Sid serving = e.introspect().toCompletableFuture().get().graph().nodes().get(0).sid();
            INIT_DELAY.put("X", 1_000L);
            e.trigger("X", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "X")));
            e.pause(List.of("X"));
            var node = e.introspect().toCompletableFuture().get().graph().nodes().stream()
                    .filter(n -> n.name().equals("X")).findFirst().orElseThrow();
            assertThat(node.state()).isEqualTo("Paused");
            assertThat(node.sid()).as("the old state is still live").isEqualTo(serving);
            assertThat(node.stale()).as("its re-init did not happen").isTrue();
            INIT_DELAY.clear();
            int before = count(INITS, "X");
            e.resume(List.of("X"));
            await().atMost(Duration.ofSeconds(5)).until(() -> count(INITS, "X") == before + 1);
        } finally {
            e.close();
        }
    }

    /** Every onInitStarted/onLoadStarted gets exactly one terminal callback, even when a pause races it. */
    @Test
    @Timeout(120)
    void every_started_attempt_gets_one_terminal_callback_when_a_pause_races_it() throws Exception {
        var initStarted = new AtomicInteger();
        var initEnded = new AtomicInteger();
        var loadStarted = new AtomicInteger();
        var loadEnded = new AtomicInteger();
        var observer = new EngineObserver() {
            @Override public void onInitStarted(String p, int a) { initStarted.incrementAndGet(); }
            @Override public void onInitCompleted(String p, io.fom.Sid s, Duration d) { initEnded.incrementAndGet(); }
            @Override public void onInitFailed(String p, int a, Throwable c) { initEnded.incrementAndGet(); }
            @Override public void onLoadStarted(String p, io.fom.Sid s, int a) { loadStarted.incrementAndGet(); }
            @Override public void onLoadCompleted(String p, io.fom.Sid s, Duration d) { loadEnded.incrementAndGet(); }
            @Override public void onLoadFailed(String p, io.fom.Sid s, int a, Throwable c) { loadEnded.incrementAndGet(); }
        };
        var e = new Engine(cfg().withDedupWindow(Duration.ofMillis(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(graph("X"));
            var random = new java.util.Random(42);
            for (int i = 0; i < 200; i++) {
                e.trigger("X", "go");
                Thread.sleep(random.nextInt(4));
                e.pause(List.of("X"));
                e.resume(List.of("X"));
            }
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(initEnded.get()).as("init terminals").isEqualTo(initStarted.get());
                assertThat(loadEnded.get()).as("load terminals").isEqualTo(loadStarted.get());
            });
        } finally {
            e.close();
        }
    }

    // ---- round 25 --------------------------------------------------------------------------

    /**
     * A re-init writes its LogDead while the node is still Serving. A pause that looks at the node
     * right then saw "Serving" and kept the retired Sid as live state; it must not.
     */
    @Test
    @Timeout(30)
    void a_pause_that_saw_serving_as_the_reinit_retired_its_state_reports_no_sid() throws Exception {
        var backend = new HoldingBackend();
        // Only RELEASE_FIRST retires the state before the re-init; KEEP_OLD writes no LogDead until the switch.
        var e = new Engine(cfg().withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("X"));
            backend.release = new CountDownLatch(1);
            backend.hold = ev -> ev instanceof io.fom.log.LogDead;
            e.trigger("X", "go");
            assertThat(backend.held.await(5, TimeUnit.SECONDS)).as("the re-init is retiring X").isTrue();
            var pause = Thread.ofVirtual().start(() -> e.pause(List.of("X")));
            Thread.sleep(200); // the pause reads the state (still Serving) and waits for the stop
            backend.hold = ev -> false;
            backend.release.countDown();
            pause.join(10_000);
            var node = e.introspect().toCompletableFuture().get().graph().nodes().stream()
                    .filter(n -> n.name().equals("X")).findFirst().orElseThrow();
            assertThat(node.state()).isEqualTo("Paused");
            assertThat(node.sid()).as("its state was retired by the re-init").isNull();
            int before = count(INITS, "X");
            e.resume(List.of("X"));
            assertThat(count(INITS, "X")).as("a retired state is rebuilt, not warm-loaded").isEqualTo(before + 1);
        } finally {
            e.close();
        }
    }

    /** A query that looked the node up just before a pause stopped it is not told the node is Dead. */
    @Test
    @Timeout(60)
    void a_query_racing_a_pause_is_never_told_the_node_is_dead() throws Exception {
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        var messages = new java.util.concurrent.ConcurrentLinkedQueue<String>();
        var stop = new java.util.concurrent.atomic.AtomicBoolean();
        var threads = new java.util.ArrayList<Thread>();
        try {
            e.newGraph(graph("A"));
            for (int t = 0; t < 8; t++) {
                threads.add(Thread.ofVirtual().start(() -> {
                    while (!stop.get()) {
                        try {
                            e.queryProcess("A", "q").toCompletableFuture().get(5, TimeUnit.SECONDS);
                        } catch (Exception ex) {
                            messages.add(String.valueOf(ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage()));
                        }
                    }
                }));
            }
            for (int i = 0; i < 100; i++) {
                e.pause(List.of("A"));
                e.resume(List.of("A"));
            }
        } finally {
            stop.set(true);
            for (Thread t : threads) t.join(5_000);
            e.close();
        }
        assertThat(messages).noneMatch(m -> m.contains("is Dead"));
    }

    /** An in-memory log whose LogDependencyChanged appends are slow. */
    static final class SlowCascadeBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        final AtomicInteger cascadeRecords = new AtomicInteger();
        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return delegate.compact(events, leader); }
        @Override public void close() { }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
            if (event instanceof io.fom.log.LogDependencyChanged) {
                cascadeRecords.incrementAndGet();
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            return delegate.append(event, leaderInstanceId);
        }
    }

    /** A wide cascade's durable records are not written on the producer's dispatcher: it keeps answering. */
    @Test
    @Timeout(60)
    void a_producer_keeps_answering_while_its_wide_cascade_is_recorded() throws Exception {
        var e = new Engine(cfg(), new SlowCascadeBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var b = new GraphBuilder();
            add(b, "M");
            for (int i = 0; i < 50; i++) add(b, "L" + i, "M");
            e.newGraph(b.build());
            e.trigger("M", "go"); // 50 records of 20 ms each: a second of appends
            await().atMost(Duration.ofSeconds(5)).until(() -> count(INITS, "M") >= 2
                    && "Serving".equals(state(e, "M")));
            long worst = 0;
            for (int i = 0; i < 20; i++) {
                long started = System.nanoTime();
                e.queryProcess("M", "q").toCompletableFuture().get(5, TimeUnit.SECONDS);
                worst = Math.max(worst, System.nanoTime() - started);
                Thread.sleep(25);
            }
            assertThat(Duration.ofNanos(worst)).isLessThan(Duration.ofMillis(300));
            await().atMost(Duration.ofSeconds(10)).until(() -> count(INITS, "L49") >= 2);
        } finally {
            e.close();
        }
    }

    /** Counts param decodes: resuming one node must not decode every node's recorded param. */
    static final class CountingSerDe implements io.fom.serde.SerDe {
        final JavaSerializableSerDe delegate = new JavaSerializableSerDe();
        final AtomicInteger loads = new AtomicInteger();
        @Override public byte[] serializeParam(String p, java.io.Serializable param) { return delegate.serializeParam(p, param); }
        @Override public Object loadParam(String p, byte[] bytes) {
            loads.incrementAndGet();
            return delegate.loadParam(p, bytes);
        }
    }

    static final class ParamNode implements io.fom.api.ParamProcessInitializer<String>,
            io.fom.api.ParamProcessLoader<String> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, String param) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, String param) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(param));
        }
    }

    @Test
    @Timeout(30)
    void resuming_one_node_decodes_only_its_own_param() throws Exception {
        var serDe = new CountingSerDe();
        var e = new Engine(cfg(), new InMemoryLogBackend(), serDe, new EngineObserver() { });
        try {
            var b = new GraphBuilder();
            for (int i = 0; i < 20; i++) {
                b.addWithParam("P" + i, (Supplier<io.fom.api.ParamProcessInitializer<String>>) ParamNode::new,
                        (Supplier<io.fom.api.ParamProcessLoader<String>>) ParamNode::new, "v" + i);
            }
            e.newGraph(b.build());
            e.pause(List.of("P7"));
            int before = serDe.loads.get();
            e.resume(List.of("P7"));
            assertThat(serDe.loads.get() - before).isLessThanOrEqualTo(2);
        } finally {
            e.close();
        }
    }

    interface Command { }

    @Test
    void routes_for_types_no_query_can_have_are_refused() {
        var b = new GraphBuilder();
        add(b, "A");
        assertThatThrownBy(() -> b.handles(Command.class)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can never be routed");
        assertThatThrownBy(() -> b.route(Number.class, n -> "A")).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> b.handles(String.class)).doesNotThrowAnyException();
    }

    @Test
    @Timeout(30)
    void a_factory_returning_null_is_named_in_the_failure() {
        var config = cfg().withInitTimeout(Duration.ofMillis(500));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var b = new GraphBuilder();
            b.add("N", (Supplier<ProcessInitializer>) () -> null, (Supplier<ProcessLoader>) () -> new Node("N"));
            assertThatThrownBy(() -> e.newGraph(b.build())).hasStackTraceContaining("the init factory of 'N' returned null");
        } finally {
            e.close();
        }
    }


    /** A cascade recorded off the producer's thread does not outlive close(). */
    @Test
    @Timeout(60)
    void a_cascade_under_way_does_not_write_after_close_returns() throws Exception {
        var backend = new SlowCascadeBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        var b = new GraphBuilder();
        add(b, "M");
        for (int i = 0; i < 50; i++) add(b, "L" + i, "M");
        e.newGraph(b.build());
        e.trigger("M", "go");
        await().atMost(Duration.ofSeconds(5)).until(() -> backend.cascadeRecords.get() > 0);
        e.close();
        int atClose = backend.cascadeRecords.get();
        Thread.sleep(600);
        assertThat(backend.cascadeRecords.get()).as("records written after close() returned").isEqualTo(atClose);
    }

    // ---- round 26 --------------------------------------------------------------------------

    /** Holds two kinds of append independently. */
    static final class TwoGateBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        final CountDownLatch deadGate = new CountDownLatch(1);
        final CountDownLatch pausedGate = new CountDownLatch(1);
        final CountDownLatch deadHeld = new CountDownLatch(1);
        final CountDownLatch pausedHeld = new CountDownLatch(1);
        volatile boolean armed;
        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return delegate.compact(events, leader); }
        @Override public void close() { }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
            try {
                if (armed && event instanceof io.fom.log.LogDead) {
                    deadHeld.countDown();
                    deadGate.await(10, TimeUnit.SECONDS);
                } else if (armed && event instanceof LogPaused) {
                    pausedHeld.countDown();
                    pausedGate.await(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return delegate.append(event, leaderInstanceId);
        }
    }

    /**
     * The pause saw Serving just before the re-init's LogDead, and the node had already moved on to
     * Initializing when the stop landed: its recorded state is retired all the same.
     */
    @Test
    @Timeout(30)
    void a_pause_landing_after_the_reinit_moved_on_still_knows_its_state_was_retired() throws Exception {
        var backend = new TwoGateBackend();
        // The RELEASE_FIRST order (LogDead, cleanup, then init); KEEP_OLD never retires before the switch.
        var e = new Engine(cfg().withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("X"));
            INIT_DELAY.put("X", 1_500L);
            backend.armed = true;
            e.trigger("X", "go");
            assertThat(backend.deadHeld.await(5, TimeUnit.SECONDS)).isTrue();
            var pause = Thread.ofVirtual().start(() -> e.pause(List.of("X")));
            assertThat(backend.pausedHeld.await(5, TimeUnit.SECONDS)).as("the pause saw Serving").isTrue();
            backend.deadGate.countDown(); // the re-init goes on: LogDead, cleanup, then Initializing
            await().atMost(Duration.ofSeconds(5)).until(() -> INITS.getOrDefault("X", new AtomicInteger()).get() >= 2);
            backend.pausedGate.countDown(); // only now does the stop reach the node, mid init
            pause.join(10_000);
            var node = e.introspect().toCompletableFuture().get().graph().nodes().stream()
                    .filter(n -> n.name().equals("X")).findFirst().orElseThrow();
            assertThat(node.state()).isEqualTo("Paused");
            assertThat(node.sid()).isNull();
        } finally {
            e.close();
        }
    }

    /** An older graph kept by a snapshot no longer carries the definitions of processes removed since. */
    @Test
    @Timeout(30)
    void a_snapshot_does_not_keep_the_definition_of_a_removed_process() throws Exception {
        var backend = new InMemoryLogBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var first = new GraphBuilder();
            add(first, "A");
            first.addWithParam("Secret", (Supplier<io.fom.api.ParamProcessInitializer<String>>) ParamNode::new,
                    (Supplier<io.fom.api.ParamProcessLoader<String>>) ParamNode::new, "SECRET-API-KEY");
            e.newGraph(first.build());
            e.remove(List.of("Secret")); // A's state stays under the first graph
            e.snapshot().toCompletableFuture().get(10, TimeUnit.SECONDS);
            for (LogEvent event : backend.getBetween(0, backend.length())) {
                if (event instanceof io.fom.log.LogChangeGraph g) {
                    assertThat(g.nodes()).extracting(io.fom.log.LogChangeGraph.Node::name).doesNotContain("Secret");
                }
            }
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void sub_millisecond_intervals_are_accepted() throws Exception {
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("A"));
            var ticks = new AtomicInteger();
            AutoCloseable watch = e.watch(new ScheduledWatcher<>("A", Integer.class, 0, Duration.ZERO,
                    Duration.ofNanos(500_000), v -> {
                        ticks.incrementAndGet();
                        return Optional.empty();
                    }, null));
            await().atMost(Duration.ofSeconds(5)).until(() -> ticks.get() > 3);
            watch.close();
            assertThatCode(() -> e.updateConfig(cfg().withSnapshotPolicy(
                    new SnapshotPolicy.FixedInterval(Duration.ofNanos(500_000), 1)))).doesNotThrowAnyException();
        } finally {
            e.close();
        }
    }

    enum Cmd {
        PING { @Override public String toString() { return "ping"; } },
        PONG
    }

    enum Op {
        ADD { @Override int apply() { return 1; } };
        abstract int apply();
    }

    @Test
    @Timeout(30)
    void an_enum_constant_with_a_body_routes_by_its_enum_type() throws Exception {
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var b = new GraphBuilder();
            add(b, "A").handles(Cmd.class);
            e.newGraph(b.build());
            assertThat(e.query(Cmd.PING).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("A-ok");
            assertThat(e.query(Cmd.PONG).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("A-ok");
            var withAbstract = new GraphBuilder();
            add(withAbstract, "A").handles(Op.class); // an abstract enum is still routable
            assertThat(withAbstract.build().typeRouting()).containsKey(Op.class);
            assertThatThrownBy(() -> new GraphBuilder().add("B", (Supplier<ProcessInitializer>) () -> new Node("B"),
                    (Supplier<ProcessLoader>) () -> new Node("B")).handles(Object.class))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            e.close();
        }
    }

    /** A cleanUp that returns at once is not blamed when draining queries used up the budget. */
    @Test
    @Timeout(30)
    void a_prompt_cleanup_is_ok_even_when_draining_used_the_budget() throws Exception {
        var results = new java.util.concurrent.CopyOnWriteArrayList<Boolean>();
        var observer = new EngineObserver() {
            @Override
            public void onCleanupCompleted(String processName, io.fom.Sid sid, boolean ok, Duration duration) {
                results.add(ok);
            }
        };
        var config = cfg().withQueryTimeout(Duration.ofSeconds(20)).withCleanupTimeout(Duration.ofMillis(300));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(graph("P"));
            COMPUTE.put("P", q -> new CompletableFuture<>()); // outlives the cleanup budget
            e.queryProcess("P", "q");
            await().atMost(Duration.ofSeconds(5)).until(() -> count(COMPUTES, "P") > 0);
            e.pause(List.of("P"));
            await().atMost(Duration.ofSeconds(5)).until(() -> !results.isEmpty());
            assertThat(results).containsExactly(true);
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void newgraph_with_the_same_graph_records_nothing() throws Exception {
        var backend = new InMemoryLogBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("A", "B"));
            int before = backend.length();
            assertThat(e.newGraph(graph("A", "B"))).isFalse();
            assertThat(backend.length()).isEqualTo(before);
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void an_engine_stop_is_reported_as_an_attempt_cancellation_and_a_pause_as_a_transition() throws Exception {
        var causes = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
        var transitions = new java.util.concurrent.CopyOnWriteArrayList<String>();
        var observer = new EngineObserver() {
            @Override public void onInitFailed(String p, int a, Throwable cause) { causes.add(cause); }
            @Override public void onStateTransition(String p, String from, String to) { transitions.add(from + "->" + to); }
        };
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(graph("X"));
            INIT_DELAY.put("X", 3_000L);
            e.trigger("X", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "X")));
            e.pause(List.of("X"));
            assertThat(causes).hasSize(1);
            assertThat(causes.get(0)).isInstanceOf(io.fom.api.AttemptCancelledException.class);
            assertThat(transitions).last().asString().endsWith("->Paused");
        } finally {
            e.close();
        }
    }

    @Test
    void properties_builder_is_linear_and_equals_the_chained_form() {
        var key = new TypedKey<>("k", Codecs.stringCodec());
        var chained = Properties.empty().put(key, "v").putRaw("raw", new byte[] {1, 2});
        var built = Properties.builder().put(key, "v").putRaw("raw", new byte[] {1, 2}).build();
        assertThat(built.get(key)).isEqualTo(chained.get(key));
        assertThat(built.getRaw("raw")).isEqualTo(chained.getRaw("raw"));
        long started = System.nanoTime();
        var big = Properties.builder();
        for (int i = 0; i < 100_000; i++) big.putRaw("c" + i, new byte[] {(byte) i});
        var props = big.build();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(2));
        assertThat(props.getRaw("c99999")).containsExactly((byte) 99999);
        assertThat(props.toBuilder().putRaw("x", new byte[] {7}).build().getRaw("x")).containsExactly((byte) 7);
    }

    // ---- round 27 --------------------------------------------------------------------------

    /** An engine stop racing the attempt's own wake-up is still reported as the engine's stop. */
    @Test
    @Timeout(120)
    void engine_stops_of_an_init_are_always_reported_as_attempt_cancellations() throws Exception {
        var wrong = new java.util.concurrent.CopyOnWriteArrayList<String>();
        var observer = new EngineObserver() {
            @Override
            public void onInitFailed(String p, int attempt, Throwable cause) {
                if (!(cause instanceof io.fom.api.AttemptCancelledException)) wrong.add(cause.toString());
            }
        };
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(graph("X"));
            for (int i = 0; i < 60; i++) {
                INIT_DELAY.put("X", 5_000L);
                e.trigger("X", "go");
                await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(replacement(e, "X")));
                Thread.sleep(20);
                e.pause(List.of("X"));
                INIT_DELAY.remove("X");
                e.resume(List.of("X"));
            }
            assertThat(wrong).as("stops reported as something else").isEmpty();
        } finally {
            e.close();
        }
    }

    /** An in-memory log whose length() can be made to block, like a file log during a compaction. */
    static final class BlockingLengthBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        volatile CountDownLatch lengthGate = new CountDownLatch(0);
        @Override public String logId() { return delegate.logId(); }
        @Override public int length() {
            try {
                lengthGate.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return delegate.length();
        }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public Optional<LogEvent> append(LogEvent event, String leader) { return delegate.append(event, leader); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return delegate.compact(events, leader); }
        @Override public void close() { }
    }

    /** The size policy's poll must not park the engine's timer thread on a slow backend. */
    @Test
    @Timeout(30)
    void a_size_policy_poll_does_not_stall_the_engine_timers() throws Exception {
        var backend = new BlockingLengthBackend();
        var config = cfg().withSnapshotPolicy(new SizeBasedSnapshotPolicy(1_000_000, Duration.ofMillis(5), 1));
        var e = new Engine(config, backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("A"));
            var ticks = new AtomicInteger();
            AutoCloseable watch = e.watch(new ScheduledWatcher<>("A", Integer.class, 0, Duration.ZERO,
                    Duration.ofMillis(5), v -> {
                        ticks.incrementAndGet();
                        return Optional.empty();
                    }, null));
            backend.lengthGate = new CountDownLatch(1); // the next length() calls hang
            Thread.sleep(50);
            int before = ticks.get();
            Thread.sleep(500);
            int during = ticks.get() - before;
            backend.lengthGate.countDown();
            watch.close();
            assertThat(during).as("watcher ticks while the backend's length() hangs").isGreaterThan(20);
        } finally {
            backend.lengthGate.countDown();
            e.close();
        }
    }

    /** A slow compaction behind a short FixedInterval. */
    static final class SlowCompactBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        final AtomicInteger compacts = new AtomicInteger();
        volatile boolean closedEngine;
        final AtomicInteger compactsAfterClose = new AtomicInteger();
        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public Optional<LogEvent> append(LogEvent event, String leader) { return delegate.append(event, leader); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public void close() { }

        @Override
        public SnapshotResult compact(List<LogEvent> events, String leader) {
            compacts.incrementAndGet();
            if (closedEngine) compactsAfterClose.incrementAndGet();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return delegate.compact(events, leader);
        }
    }

    /** Scheduled snapshots slower than their interval do not pile up (and do not outlive close()). */
    @Test
    @Timeout(60)
    void scheduled_snapshots_slower_than_their_interval_do_not_pile_up() throws Exception {
        var backend = new SlowCompactBackend();
        var config = cfg().withSnapshotPolicy(new SnapshotPolicy.FixedInterval(Duration.ofMillis(1), 1));
        var e = new Engine(config, backend, new JavaSerializableSerDe(), new EngineObserver() { });
        e.newGraph(graph("A"));
        Thread.sleep(1_000);
        e.close();
        backend.closedEngine = true;
        Thread.sleep(1_000);
        assertThat(backend.compactsAfterClose.get()).as("compactions started after close() returned").isZero();
    }

    /** Fails LogTrigger appends while armed. */
    static final class FailingTriggerBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        volatile boolean failTriggers;
        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return delegate.compact(events, leader); }
        @Override public void close() { }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leader) {
            if (failTriggers && event instanceof LogTrigger) throw new IllegalStateException("log outage (injected)");
            return delegate.append(event, leader);
        }
    }

    /** A change a watcher saw during a log outage is not lost: it is triggered once the log is back. */
    @Test
    @Timeout(30)
    void a_watcher_change_seen_during_a_log_outage_is_triggered_after_it() throws Exception {
        var backend = new FailingTriggerBackend();
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        var version = new AtomicInteger();
        try {
            e.newGraph(graph("A"));
            AutoCloseable watch = e.watch(new ScheduledWatcher<>("A", Integer.class, 0, Duration.ZERO,
                    Duration.ofMillis(20), prev -> version.get() != prev ? Optional.of(version.get()) : Optional.empty(),
                    null));
            int before = count(INITS, "A");
            backend.failTriggers = true;
            version.set(1);
            Thread.sleep(300);
            backend.failTriggers = false;
            await().atMost(Duration.ofSeconds(5)).until(() -> count(INITS, "A") > before);
            watch.close();
        } finally {
            e.close();
        }
    }

    @Test
    void a_route_for_an_enum_constant_body_is_refused() {
        assertThatThrownBy(() -> add(new GraphBuilder(), "A").handles(Cmd.PING.getClass()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("register");
    }

    /**
     * During a RELEASE_FIRST re-init's cleanup the old Sid is already retired: introspection reports
     * none. (KEEP_OLD has no such phase: see a_reinit_is_visible_in_introspection.)
     */
    @Test
    @Timeout(30)
    void introspection_reports_no_sid_while_a_reinit_cleans_up() throws Exception {
        var e = new Engine(cfg().withCleanupTimeout(Duration.ofSeconds(3))
                .withReinitStrategy(ReinitStrategy.RELEASE_FIRST), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("X"));
            CLEANUP_DELAY.put("X", 1_000L);
            e.trigger("X", "go");
            await().atMost(Duration.ofSeconds(5)).until(() -> "CleaningUp".equals(state(e, "X")));
            var node = e.introspect().toCompletableFuture().get().graph().nodes().stream()
                    .filter(n -> n.name().equals("X")).findFirst().orElseThrow();
            assertThat(node.sid()).isNull();
        } finally {
            e.close();
        }
    }

    /** updateGraph's read-modify-write cannot undo a concurrent removal. */
    @Test
    @Timeout(120)
    void update_graph_does_not_resurrect_a_concurrently_removed_process() throws Exception {
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Keep"));
            for (int i = 0; i < 30; i++) {
                String doomed = "Doomed" + i;
                String added = "Added" + i;
                e.updateGraph(g -> {
                    var b = new GraphBuilder();
                    for (var name : g.nodes().keySet()) add(b, name);
                    add(b, doomed);
                    return b.build();
                });
                var remover = Thread.ofVirtual().start(() -> e.remove(List.of(doomed)));
                e.updateGraph(g -> {
                    var b = new GraphBuilder();
                    for (var name : g.nodes().keySet()) add(b, name);
                    add(b, added);
                    return b.build();
                });
                remover.join(10_000);
                assertThat(e.currentGraph().nodes()).containsKey(added).doesNotContainKey(doomed);
            }
        } finally {
            e.close();
        }
    }

    // ---- round 28 --------------------------------------------------------------------------

    /** A first newGraph that fails and is retried still ends up with its snapshot policy running. */
    @Test
    @Timeout(30)
    void the_snapshot_policy_runs_after_a_failed_first_start_is_retried() throws Exception {
        var backend = new CountingLogBackend();
        var config = cfg().withInitTimeout(Duration.ofMillis(300))
                .withSnapshotPolicy(new SnapshotPolicy.FixedInterval(Duration.ofMillis(50), 1));
        var e = new Engine(config, backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            FAIL_INIT.add("X");
            assertThatThrownBy(() -> e.newGraph(graph("X"))).isInstanceOf(RuntimeException.class);
            FAIL_INIT.clear();
            e.newGraph(graph("X")); // the documented retry
            await().atMost(Duration.ofSeconds(5)).until(() -> backend.compacts.get() > 0);
        } finally {
            e.close();
        }
    }

    /** A second close() returns only once the first one has stopped the processes. */
    @Test
    @Timeout(30)
    void a_concurrent_second_close_waits_for_the_first() throws Exception {
        var cleaned = new java.util.concurrent.atomic.AtomicBoolean();
        var observer = new EngineObserver() {
            @Override
            public void onCleanupCompleted(String p, io.fom.Sid sid, boolean ok, Duration d) {
                cleaned.set(true);
            }
        };
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        e.newGraph(graph("X"));
        CLEANUP_DELAY.put("X", 800L);
        var first = Thread.ofVirtual().start(e::close);
        await().atMost(Duration.ofSeconds(5)).until(() -> CLEANUPS_BEGUN.contains("X"));
        e.close(); // the second caller
        assertThat(cleaned).as("the processes had stopped when the second close() returned").isTrue();
        first.join(5_000);
    }

    /** A control-plane call from inside an updateGraph change is refused, not silently undone. */
    @Test
    @Timeout(30)
    void a_control_plane_call_inside_an_update_graph_change_is_refused() throws Exception {
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Keep", "T1"));
            assertThatThrownBy(() -> e.updateGraph(g -> {
                e.remove(List.of("T1"));
                return g;
            })).isInstanceOf(IllegalStateException.class).hasMessageContaining("inside an updateGraph change");
            assertThat(e.currentGraph().nodes()).containsKeys("Keep", "T1");
            assertThat(state(e, "T1")).isEqualTo("Serving");
        } finally {
            e.close();
        }
    }

    // ---- round 29 --------------------------------------------------------------------------

    /** An init that blocks before returning its stage is still bounded by its budget. */
    @Test
    @Timeout(30)
    void an_init_blocked_before_returning_its_stage_is_bounded_by_its_budget() throws Exception {
        var e = new Engine(cfg().withInitTimeout(Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            SYNC_BLOCK_INIT.put("Stuck", 20_000L);
            long started = System.nanoTime();
            assertThatThrownBy(() -> e.newGraph(graph("Stuck"))).isInstanceOf(RuntimeException.class);
            await().atMost(Duration.ofSeconds(5)).until(() -> "Dead".equals(state(e, "Stuck")));
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(4));
        } finally {
            SYNC_BLOCK_INIT.clear();
            e.close();
        }
    }

    /** So is a load that blocks before returning its stage. */
    @Test
    @Timeout(30)
    void a_load_blocked_before_returning_its_stage_is_bounded_by_its_budget() throws Exception {
        var loadFailures = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
        var observer = new EngineObserver() {
            @Override
            public void onLoadFailed(String p, io.fom.Sid sid, int attempt, Throwable cause) {
                loadFailures.add(cause);
            }
        };
        var e = new Engine(cfg().withLoadTimeout(Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), observer);
        try {
            SYNC_BLOCK_LOAD.put("Slow", 20_000L);
            var install = Thread.ofVirtual().start(() -> {
                try {
                    e.newGraph(graph("Slow"));
                } catch (RuntimeException expected) {
                    // the node does not start
                }
            });
            await().atMost(Duration.ofSeconds(4)).until(() -> !loadFailures.isEmpty());
            assertThat(loadFailures.get(0)).isInstanceOf(java.util.concurrent.TimeoutException.class);
            SYNC_BLOCK_LOAD.clear();
            install.join(30_000);
        } finally {
            SYNC_BLOCK_LOAD.clear();
            e.close();
        }
    }

    /** Throws an Error from the first LogInitialized append. */
    static final class ErrorOnceBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        final java.util.concurrent.atomic.AtomicBoolean thrown = new java.util.concurrent.atomic.AtomicBoolean();
        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return delegate.compact(events, leader); }
        @Override public void close() { }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leader) {
            if (event instanceof io.fom.log.LogInitialized && thrown.compareAndSet(false, true)) {
                throw new AssertionError("injected while recording the state");
            }
            return delegate.append(event, leader);
        }
    }

    /** An Error (not a JVM error) while recording an init is a failed attempt: retried, not a node stuck for good. */
    @Test
    @Timeout(30)
    void an_error_while_recording_an_init_is_retried() throws Exception {
        var e = new Engine(cfg(), new ErrorOnceBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("X"));
            assertThat(state(e, "X")).isEqualTo("Serving");
        } finally {
            e.close();
        }
    }

    /** Throws an Error from the first LogDead append. */
    static final class DeadErrorOnceBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        final java.util.concurrent.atomic.AtomicBoolean thrown = new java.util.concurrent.atomic.AtomicBoolean();
        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return delegate.compact(events, leader); }
        @Override public void close() { }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leader) {
            if (event instanceof io.fom.log.LogDead && thrown.compareAndSet(false, true)) {
                throw new AssertionError("injected in the backend");
            }
            return delegate.append(event, leader);
        }
    }

    /** An Error while retiring the state for a re-init is retried, not a node stuck serving for good. */
    @Test
    @Timeout(30)
    void an_error_while_retiring_state_for_a_reinit_is_retried() throws Exception {
        var e = new Engine(cfg(), new DeadErrorOnceBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("X"));
            int before = count(INITS, "X");
            e.trigger("X", "go");
            await().atMost(Duration.ofSeconds(10)).until(() -> count(INITS, "X") > before
                    && "Serving".equals(state(e, "X")));
        } finally {
            e.close();
        }
    }

    // ---- round 30 --------------------------------------------------------------------------

    /** The budget watchdog's timeout is reported as a timeout even when it ends the last attempt. */
    @Test
    @Timeout(60)
    void a_watchdog_timeout_on_the_last_attempt_is_reported_as_a_timeout() throws Exception {
        for (int round = 0; round < 8; round++) {
            var causes = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
            var observer = new EngineObserver() {
                @Override public void onInitFailed(String p, int a, Throwable c) { causes.add(c); }
            };
            var e = new Engine(cfg().withInitTimeout(Duration.ofMillis(500)), new InMemoryLogBackend(),
                    new JavaSerializableSerDe(), observer);
            try {
                SYNC_BLOCK_INIT.put("Stuck", 5_000L);
                assertThatThrownBy(() -> e.newGraph(graph("Stuck"))).isInstanceOf(RuntimeException.class);
                await().atMost(Duration.ofSeconds(5)).until(() -> !causes.isEmpty());
                assertThat(causes).as("round %d", round).allMatch(c -> c instanceof java.util.concurrent.TimeoutException);
            } finally {
                SYNC_BLOCK_INIT.clear();
                e.close();
            }
        }
    }

    /** An OutOfMemoryError that reaches the engine wrapped in an IllegalArgumentException is retried. */
    static final class WrappedOomOnceBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        final java.util.concurrent.atomic.AtomicBoolean thrown = new java.util.concurrent.atomic.AtomicBoolean();
        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return delegate.compact(events, leader); }
        @Override public void close() { }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leader) {
            if (event instanceof io.fom.log.LogInitialized && thrown.compareAndSet(false, true)) {
                // what try-with-resources makes of an OOM thrown twice by the same shared instance
                throw new IllegalArgumentException("Self-suppression not permitted",
                        new OutOfMemoryError("Java heap space"));
            }
            return delegate.append(event, leader);
        }
    }

    @Test
    @Timeout(30)
    void an_out_of_memory_error_wrapped_by_the_backend_is_retried_not_final() throws Exception {
        var e = new Engine(cfg(), new WrappedOomOnceBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("X"));
            assertThat(state(e, "X")).isEqualTo("Serving");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void lifecycle_calls_take_plain_names() throws Exception {
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Keep", "A"));
            e.pause("A");
            assertThat(state(e, "A")).isEqualTo("Paused");
            e.resume("A");
            assertThat(state(e, "A")).isEqualTo("Serving");
            assertThat(e.remove("A")).isTrue();
        } finally {
            e.close();
        }
    }

    // ---- round 31 --------------------------------------------------------------------------

    /** A query waiting on a node whose start a pause cancels is told the node is shutting down. */
    @Test
    @Timeout(30)
    void a_query_waiting_on_a_start_cancelled_by_a_pause_is_not_told_dead() throws Exception {
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Keep"));
            INIT_DELAY.put("Q", 5_000L);
            var install = Thread.ofVirtual().start(() -> {
                try {
                    e.newGraph(graph("Keep", "Q"));
                } catch (RuntimeException expected) {
                    // the start is cancelled
                }
            });
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(state(e, "Q")));
            var waiting = e.queryProcess("Q", "q").toCompletableFuture();
            e.pause("Q");
            assertThatThrownBy(() -> waiting.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(io.fom.api.QueryRejectedException.class)
                    .hasMessageContaining("shutting down");
            install.join(10_000);
        } finally {
            INIT_DELAY.clear();
            e.close();
        }
    }

    /** A log that the startup scan refuses gets no LogChangeGraph appended to it. */
    static final class RefusingScanBackend implements LogBackend {
        final InMemoryLogBackend delegate = new InMemoryLogBackend();
        volatile boolean refuseReads;
        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) {
            if (refuseReads) throw new IllegalStateException("the log is spliced (injected)");
            return delegate.getBetween(from, to);
        }
        @Override public Optional<LogEvent> append(LogEvent event, String leader) { return delegate.append(event, leader); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return delegate.compact(events, leader); }
        @Override public void close() { }
    }

    @Test
    @Timeout(30)
    void a_log_the_startup_scan_refuses_gets_no_graph_recorded() {
        var backend = new RefusingScanBackend();
        backend.refuseReads = true;
        var e = new Engine(cfg(), backend, new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            assertThatThrownBy(() -> e.newGraph(graph("A"))).hasStackTraceContaining("spliced");
            for (int i = 0; i < backend.delegate.length(); i++) {
                assertThat(backend.delegate.get(i)).isNotInstanceOf(io.fom.log.LogChangeGraph.class);
            }
        } finally {
            e.close();
        }
    }

    @Test
    void report_counts_are_listed_in_a_stable_order() {
        var counts = new java.util.HashMap<String, Integer>();
        for (String t : List.of("LogTrigger", "LogDead", "LogLeader", "LogInitialized", "LogLoaded")) counts.put(t, 1);
        var report = new io.fom.log.LogBackendReport("log", 5, "me", counts, 0L);
        assertThat(report.eventCounts().keySet())
                .containsExactly("LogDead", "LogInitialized", "LogLeader", "LogLoaded", "LogTrigger");
    }

    // ---- round 32 --------------------------------------------------------------------------

    /** A snapshot over a file damaged while the engine runs fails and leaves the file (and archives) as they were. */
    @Test
    @Timeout(60)
    void a_snapshot_over_a_log_damaged_at_runtime_fails_and_changes_nothing(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        var file = dir.resolve("live.log");
        var e = new Engine(cfg(), new io.fom.log.FileLogBackend(file), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph("Alpha", "Bravo"));
            // Rename "Bravo" into "Brava" in its last record: still a well-formed event, so only the CRC tells.
            byte[] bytes = java.nio.file.Files.readAllBytes(file);
            int at = -1;
            for (int i = bytes.length - 2; i >= 1; i--) {
                if (bytes[i] == 'v' && bytes[i + 1] == 'o' && bytes[i - 1] == 'a') { at = i + 1; break; }
            }
            assertThat(at).isPositive();
            try (var raf = new java.io.RandomAccessFile(file.toFile(), "rw")) {
                raf.seek(at);
                raf.write('a');
            }
            byte[] before = java.nio.file.Files.readAllBytes(file);
            List<String> filesBefore;
            try (var list = java.nio.file.Files.list(dir)) { filesBefore = list.map(Object::toString).sorted().toList(); }
            assertThatThrownBy(() -> e.snapshot().toCompletableFuture().get(20, TimeUnit.SECONDS))
                    .hasStackTraceContaining("damaged");
            assertThat(java.nio.file.Files.readAllBytes(file)).isEqualTo(before);
            try (var list = java.nio.file.Files.list(dir)) {
                assertThat(list.map(Object::toString).sorted().toList()).isEqualTo(filesBefore);
            }
        } finally {
            e.close();
        }
    }

    // ---- round 33 --------------------------------------------------------------------------

    /** The startup scan streams the log through forEachBetween instead of materialising ranges. */
    @Test
    void the_log_scan_streams_events_instead_of_reading_ranges(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        try (var file = new io.fom.log.FileLogBackend(dir.resolve("scan.log"))) {
            file.append(new io.fom.log.LogLeader(0, 1L, "me"), "me");
            for (int i = 0; i < 5; i++) {
                file.append(new io.fom.log.LogInitialized(0, 1L, "P", Map.of("v", new byte[] {(byte) i})), "me");
            }
            var noRanges = new LogBackend() {
                @Override public String logId() { return file.logId(); }
                @Override public int length() { return file.length(); }
                @Override public LogEvent get(int position) { return file.get(position); }
                @Override public LogEvent[] getBetween(int from, int to) {
                    throw new AssertionError("the scan must not materialise a range");
                }
                @Override public void forEachBetween(int from, int to, java.util.function.Consumer<? super LogEvent> action) {
                    file.forEachBetween(from, to, action);
                }
                @Override public Optional<LogEvent> append(LogEvent event, String leader) { return file.append(event, leader); }
                @Override public LogBackendReport introspect() { return file.introspect(); }
                @Override public SnapshotResult compact(List<LogEvent> events, String leader) { return file.compact(events, leader); }
                @Override public void close() { }
            };
            var scan = LogCompaction.scan(noRanges);
            assertThat(scan.liveInits().get("P").init().properties().get("v")).isEqualTo(new byte[] {4});
        }
    }

    @Test
    void the_default_forEachBetween_hands_every_event_in_order() {
        var backend = new InMemoryLogBackend();
        backend.append(new io.fom.log.LogLeader(0, 1L, "me"), "me");
        for (int i = 0; i < 2_500; i++) backend.append(new io.fom.log.LogTrigger(0, 1L, List.of("P" + i)), "me");
        var seen = new java.util.ArrayList<LogEvent>();
        backend.forEachBetween(1, backend.length(), seen::add);
        assertThat(seen).hasSize(2_500);
        assertThat(((io.fom.log.LogTrigger) seen.get(2_499)).processNames()).containsExactly("P2499");
    }

    // ---- after round 33: decisions ---------------------------------------------------------

    /** Stopping a process from an observer callback about it (its own dispatcher) is refused at once. */
    @Test
    @Timeout(30)
    void stopping_a_process_from_a_callback_about_it_is_refused_at_once() throws Exception {
        var outcome = new java.util.concurrent.ConcurrentHashMap<String, Throwable>();
        var engineRef = new java.util.concurrent.atomic.AtomicReference<Engine>();
        var observer = new EngineObserver() {
            @Override
            public void onLoadCompleted(String processName, io.fom.Sid sid, Duration duration) {
                Engine e = engineRef.get();
                if (e == null || !processName.equals("Self")) return;
                long start = System.nanoTime();
                for (var call : List.<Runnable>of(() -> e.pause("Self"), () -> e.remove("Self"),
                        () -> e.newGraph(graph("Keep")), e::close)) {
                    try {
                        call.run();
                        outcome.put("call" + outcome.size(), new AssertionError("accepted"));
                    } catch (Throwable t) {
                        outcome.put("call" + outcome.size(), t);
                    }
                }
                outcome.put("millis", new RuntimeException(Long.toString((System.nanoTime() - start) / 1_000_000)));
            }
        };
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        engineRef.set(e);
        try {
            e.newGraph(graph("Keep", "Self"));
            await().atMost(Duration.ofSeconds(10)).until(() -> outcome.containsKey("millis"));
            for (String k : List.of("call0", "call1", "call2", "call3")) {
                assertThat(outcome.get(k)).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("dispatcher");
            }
            assertThat(Long.parseLong(outcome.get("millis").getMessage())).isLessThan(1_000);
            assertThat(state(e, "Self")).isEqualTo("Serving");
        } finally {
            engineRef.set(null);
            e.close();
        }
    }

    /** A control call from a callback while the first newGraph holds the lock fails at once; newGraph completes. */
    @Test
    @Timeout(30)
    void a_control_call_from_a_callback_during_startup_fails_fast_instead_of_stalling_it() throws Exception {
        var outcome = new java.util.concurrent.CompletableFuture<Throwable>();
        var engineRef = new java.util.concurrent.atomic.AtomicReference<Engine>();
        var observer = new EngineObserver() {
            @Override
            public void onStateTransition(String processName, String from, String to) {
                Engine e = engineRef.get();
                if (e == null || !"Dashboard".equals(processName) || !"Serving".equals(to) || outcome.isDone()) return;
                try {
                    e.pause("Other");
                    outcome.complete(null);
                } catch (Throwable t) {
                    outcome.complete(t);
                }
            }
        };
        var e = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        engineRef.set(e);
        INIT_DELAY.put("Other", 1_000L); // still starting when Dashboard serves: a refused pause must not cancel it
        try {
            long start = System.nanoTime();
            e.newGraph(graph("Other", "Dashboard"));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));
            assertThat(outcome.get(5, TimeUnit.SECONDS)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("another control-plane call is in progress");
            assertThat(state(e, "Other")).isEqualTo("Serving");
        } finally {
            INIT_DELAY.clear();
            engineRef.set(null);
            e.close();
        }
    }
}
