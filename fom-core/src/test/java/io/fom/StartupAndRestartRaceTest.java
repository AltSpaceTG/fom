package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryRejectedException;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Nodes without a running FSM (starting, Dead, being replaced) and the operations
 * that race them: queries, removals, refused pauses, restarts, close and cancel.
 */
class StartupAndRestartRaceTest {

    static final Map<String, Long> INIT_DELAY = new ConcurrentHashMap<>();
    static final Set<String> FAILING = ConcurrentHashMap.newKeySet();
    static final Set<String> HANGING_COMPUTE = ConcurrentHashMap.newKeySet();
    static final Map<String, Long> LOAD_DELAY = new ConcurrentHashMap<>();
    /** Sleep inside init before returning its stage — the budget must cover that too. */
    static final Map<String, Long> INIT_SYNC_SLEEP = new ConcurrentHashMap<>();
    static final java.util.concurrent.atomic.AtomicBoolean INIT_INTERRUPTED =
            new java.util.concurrent.atomic.AtomicBoolean();
    static final java.util.concurrent.atomic.AtomicReference<String> PARAM =
            new java.util.concurrent.atomic.AtomicReference<>("v0");
    static final Map<String, AtomicInteger> INITS = new ConcurrentHashMap<>();

    static final class Node implements ProcessInitializer, ProcessLoader {
        private final String name;

        Node(String name) {
            this.name = name;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INITS.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            long inline = INIT_SYNC_SLEEP.getOrDefault(name, 0L);
            if (inline > 0) { // all the work before the stage exists
                try {
                    Thread.sleep(inline);
                } catch (InterruptedException e) {
                    INIT_INTERRUPTED.set(true);
                    Thread.currentThread().interrupt();
                }
                return CompletableFuture.completedFuture(Map.of());
            }
            var result = new CompletableFuture<Map<String, byte[]>>();
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(INIT_DELAY.getOrDefault(name, 0L));
                } catch (InterruptedException e) {
                    result.completeExceptionally(e);
                    return;
                }
                if (FAILING.contains(name)) {
                    result.completeExceptionally(new IllegalStateException(name + " init fails"));
                } else {
                    result.complete(Map.of());
                }
            });
            return result;
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            Process process = (c, q) -> {
                var reply = new CompletableFuture<Object>();
                if (!HANGING_COMPUTE.contains(name)) reply.complete(name + "-ok");
                return reply;
            };
            long delay = LOAD_DELAY.getOrDefault(name, 0L);
            if (delay == 0) return CompletableFuture.completedFuture(process);
            var loaded = new CompletableFuture<Process>();
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(delay);
                    loaded.complete(process);
                } catch (InterruptedException e) {
                    loaded.completeExceptionally(e);
                }
            });
            return loaded;
        }
    }

    @BeforeEach
    void reset() {
        INIT_DELAY.clear();
        FAILING.clear();
        HANGING_COMPUTE.clear();
        LOAD_DELAY.clear();
        INIT_SYNC_SLEEP.clear();
        CLEANUP_MILLIS.set(0);
        INIT_INTERRUPTED.set(false);
        PARAM.set("v0");
        INITS.clear();
    }

    private static EngineConfig cfg(Duration initTimeout, Duration cleanupTimeout) {
        return new EngineConfig(
                initTimeout, Duration.ofSeconds(5), cleanupTimeout,
                Duration.ofSeconds(10), Duration.ofMillis(10),
                Duration.ofMillis(20), Duration.ofMillis(200), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    private static Engine engine(EngineConfig cfg, EngineObserver observer) {
        return new Engine(cfg, new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
    }

    private static GraphBuilder add(GraphBuilder b, String name, String... deps) {
        return b.add(name, (Supplier<ProcessInitializer>) () -> new Node(name),
                (Supplier<ProcessLoader>) () -> new Node(name), deps);
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

    private static int inits(String name) {
        return INITS.getOrDefault(name, new AtomicInteger()).get();
    }

    @Test
    @Timeout(20)
    void a_query_to_a_node_still_waiting_for_its_dependency_waits_for_it() throws Exception {
        INIT_DELAY.put("Slow", 800L);
        var b = new GraphBuilder();
        add(b, "Slow");
        add(b, "Beta", "Slow");
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), new EngineObserver() { });
        try {
            var install = CompletableFuture.runAsync(() -> e.newGraph(b.build()));
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions()
                    .until(() -> "Starting".equals(state(e, "Beta")));

            var reply = e.queryProcess("Beta", "q");
            assertThat(reply.toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("Beta-ok");
            install.get(5, TimeUnit.SECONDS);
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(20)
    void removing_an_unrelated_process_leaves_a_dead_one_alone() throws Exception {
        FAILING.add("X");
        var b = new GraphBuilder();
        add(b, "G");
        add(b, "X");
        add(b, "Z");
        var e = engine(cfg(Duration.ofMillis(300), Duration.ofSeconds(1)), new EngineObserver() { });
        try {
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "Dead".equals(state(e, "X")));
            int initsBefore = inits("X");

            long start = System.nanoTime();
            assertThat(e.remove(List.of("Z"))).isTrue();

            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(250));
            assertThat(inits("X")).as("X is not re-initialised").isEqualTo(initsBefore);
            assertThat(state(e, "X")).isEqualTo("Dead");
            assertThat(e.currentGraph().nodes()).doesNotContainKey("Z");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(20)
    void a_refused_pause_or_removal_does_not_cancel_the_init_it_names() throws Exception {
        INIT_DELAY.put("Hang", 800L);
        var first = new GraphBuilder();
        add(first, "A");
        var next = new GraphBuilder();
        add(next, "A");
        add(next, "Hang");
        add(next, "C", "Hang");
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), new EngineObserver() { });
        try {
            e.newGraph(first.build());
            var install = CompletableFuture.runAsync(() -> e.newGraph(next.build()));
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions()
                    .until(() -> "Initializing".equals(state(e, "Hang")));

            // Both are refused: C depends on Hang and would keep running.
            var pause = CompletableFuture.runAsync(() -> e.pause(List.of("Hang")));
            var remove = CompletableFuture.runAsync(() -> e.remove(List.of("Hang")));

            install.get(5, TimeUnit.SECONDS); // not failed by a cancelled init
            assertThatThrownBy(pause::join).hasCauseInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(remove::join).hasCauseInstanceOf(IllegalArgumentException.class);
            assertThat(e.queryProcess("C", "q").toCompletableFuture().get(3, TimeUnit.SECONDS)).isEqualTo("C-ok");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void resume_and_restart_from_dead_report_each_new_sid_once() throws Exception {
        List<String> promotions = new CopyOnWriteArrayList<>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onSidPromotion(String processName, Sid previousSid, Sid newSid) {
                if (processName.equals("A")) promotions.add((previousSid == null ? "-" : previousSid.clock()) + "->" + newSid.clock());
            }
        };
        var b = new GraphBuilder();
        add(b, "G");
        add(b, "A");
        // A failed re-init goes Dead only under RELEASE_FIRST; KEEP_OLD keeps serving the old version.
        var e = engine(cfg(Duration.ofMillis(400), Duration.ofSeconds(1)).withReinitStrategy(ReinitStrategy.RELEASE_FIRST), observer);
        try {
            e.newGraph(b.build());
            assertThat(promotions).hasSize(1);

            e.pause(List.of("A"));
            e.resume(List.of("A"));
            assertThat(e.queryProcess("A", "q").toCompletableFuture().get(3, TimeUnit.SECONDS)).isEqualTo("A-ok");
            assertThat(promotions).as("a warm resume of the same state is no promotion").hasSize(1);

            FAILING.add("A");
            e.trigger("A", "fail");
            await().atMost(Duration.ofSeconds(10)).ignoreExceptions().until(() -> "Dead".equals(state(e, "A")));
            FAILING.remove("A");
            e.trigger("A", "restart");
            await().atMost(Duration.ofSeconds(10)).ignoreExceptions().until(() -> "Serving".equals(state(e, "A")));
            Thread.sleep(200);

            assertThat(promotions).hasSize(2);
            assertThat(promotions.get(1)).doesNotStartWith("-");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(20)
    void close_fails_a_query_that_is_still_computing() throws Exception {
        HANGING_COMPUTE.add("H");
        var b = new GraphBuilder();
        add(b, "H");
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofMillis(300)), new EngineObserver() { });
        e.newGraph(b.build());
        var reply = e.queryProcess("H", "q", Duration.ofSeconds(30));
        Thread.sleep(100);

        e.close();

        assertThatThrownBy(() -> reply.toCompletableFuture().get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(QueryRejectedException.class);
        var afterClose = e.queryProcess("H", "q"); // fails the stage, does not throw
        assertThatThrownBy(() -> afterClose.toCompletableFuture().get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    @Timeout(20)
    void close_stops_independent_hung_processes_together() throws Exception {
        var b = new GraphBuilder();
        for (String name : List.of("H1", "H2", "H3", "H4")) {
            HANGING_COMPUTE.add(name);
            add(b, name);
        }
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofMillis(400)), new EngineObserver() { });
        e.newGraph(b.build());
        for (String name : List.of("H1", "H2", "H3", "H4")) e.queryProcess(name, "q", Duration.ofSeconds(30));
        Thread.sleep(100);

        long start = System.nanoTime();
        e.close();

        assertThat(Duration.ofNanos(System.nanoTime() - start))
                .as("one cleanup budget per dependency level, not per process").isLessThan(Duration.ofMillis(1_500));
    }

    public record ForBeta(String target) implements io.fom.api.Routable {
        @Override
        public String targetProcess() {
            return target;
        }
    }

    @Test
    @Timeout(20)
    void a_routable_query_to_a_starting_node_waits_and_current_graph_shows_the_graph_being_installed()
            throws Exception {
        INIT_DELAY.put("Slow", 800L);
        var first = new GraphBuilder();
        add(first, "A");
        var next = new GraphBuilder();
        add(next, "A");
        add(next, "Slow");
        add(next, "Beta", "Slow");
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), new EngineObserver() { });
        try {
            e.newGraph(first.build());
            var install = CompletableFuture.runAsync(() -> e.newGraph(next.build()));
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions()
                    .until(() -> "Starting".equals(state(e, "Beta")));

            assertThat(e.currentGraph().nodes()).containsKeys("Slow", "Beta");
            var reply = e.query(new ForBeta("Beta"));
            assertThat(reply.toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("Beta-ok");
            install.get(5, TimeUnit.SECONDS);
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_refused_removal_does_not_cancel_an_init_when_a_queued_swap_adds_a_dependent() throws Exception {
        INIT_DELAY.put("H", 1_000L);
        var first = new GraphBuilder();
        add(first, "A");
        var withH = new GraphBuilder();
        add(withH, "A");
        add(withH, "H");
        var withK = new GraphBuilder();
        add(withK, "A");
        add(withK, "H");
        add(withK, "K", "H");
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), new EngineObserver() { });
        try {
            e.newGraph(first.build());
            var installH = CompletableFuture.runAsync(() -> e.newGraph(withH.build()));
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions()
                    .until(() -> "Initializing".equals(state(e, "H")));
            var installK = CompletableFuture.runAsync(() -> e.newGraph(withK.build())); // queued behind the lock
            Thread.sleep(150);

            var remove = CompletableFuture.runAsync(() -> e.remove(List.of("H")));

            installH.get(10, TimeUnit.SECONDS); // H's init was not cancelled
            installK.get(10, TimeUnit.SECONDS);
            assertThatThrownBy(remove::join).hasCauseInstanceOf(IllegalArgumentException.class);
            assertThat(inits("H")).isEqualTo(1);
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void pausing_while_the_new_state_loads_does_not_init_again_on_resume() throws Exception {
        var b = new GraphBuilder();
        add(b, "G");
        add(b, "P");
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            LOAD_DELAY.put("P", 800L);
            e.trigger("P", "refresh");
            // KEEP_OLD: the new state loads beside the serving one; the pause keeps it as a candidate.
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "Loading".equals(replacement(e, "P")));

            e.pause(List.of("P"));
            LOAD_DELAY.remove("P");
            e.resume(List.of("P"));
            Thread.sleep(300);

            assertThat(inits("P")).as("the trigger's init, and no second one").isEqualTo(2);
            assertThat(e.queryProcess("P", "q").toCompletableFuture().get(3, TimeUnit.SECONDS)).isEqualTo("P-ok");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(20)
    void a_cancelled_init_is_reported_once_before_cancel_init_returns() throws Exception {
        INIT_DELAY.put("Slow", 5_000L);
        Map<String, AtomicInteger> failures = new ConcurrentHashMap<>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                failures.computeIfAbsent(processName, k -> new AtomicInteger()).incrementAndGet();
            }
        };
        var b = new GraphBuilder();
        add(b, "Slow");
        var e = engine(cfg(Duration.ofSeconds(10), Duration.ofSeconds(1)), observer);
        try {
            var install = CompletableFuture.runAsync(() -> e.newGraph(b.build()));
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions()
                    .until(() -> "Initializing".equals(state(e, "Slow")));

            e.cancelInit("Slow").toCompletableFuture().get(2, TimeUnit.SECONDS);

            assertThat(failures.get("Slow")).hasValue(1);
            var report = e.introspect().toCompletableFuture().get().graph().nodes().stream()
                    .filter(n -> n.name().equals("Slow")).findFirst().orElseThrow();
            assertThat(report.lastException()).contains("was cancelled");
            assertThatThrownBy(() -> install.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            Thread.sleep(200);
            assertThat(failures.get("Slow")).as("not reported again by the worker").hasValue(1);
        } finally {
            e.close();
        }
    }

    // ───────────────── round-7: queries racing swaps, re-inits and startup ─────────────────

    record Param(String value) implements java.io.Serializable { }

    static final class ParamNode implements io.fom.api.ParamProcessInitializer<Param>,
            io.fom.api.ParamProcessLoader<Param> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Param param) {
            INITS.computeIfAbsent(param.value(), k -> new AtomicInteger()).incrementAndGet();
            return CompletableFuture.completedFuture(
                    Map.of("v", param.value().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, Param param) {
            String value = new String(props.get("v"), java.nio.charset.StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(value));
        }
    }

    private static Graph paramGraph(String param) {
        return new GraphBuilder().addWithParam("T",
                (Supplier<io.fom.api.ParamProcessInitializer<Param>>) ParamNode::new,
                (Supplier<io.fom.api.ParamProcessLoader<Param>>) ParamNode::new,
                new Param(param)).build();
    }

    @Test
    @Timeout(60)
    void queries_to_a_changed_node_during_a_swap_wait_for_the_replacement() throws Exception {
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), new EngineObserver() { });
        try {
            e.newGraph(paramGraph("v0"));
            var stop = new java.util.concurrent.atomic.AtomicBoolean();
            var failures = new CopyOnWriteArrayList<String>();
            var answers = new AtomicInteger();
            List<Thread> clients = new java.util.ArrayList<>();
            for (int i = 0; i < 6; i++) {
                clients.add(Thread.ofVirtual().start(() -> {
                    while (!stop.get()) {
                        try {
                            e.queryProcess("T", "q", Duration.ofSeconds(10)).toCompletableFuture()
                                    .get(10, TimeUnit.SECONDS);
                            answers.incrementAndGet();
                        } catch (ExecutionException ex) {
                            failures.add(String.valueOf(ex.getCause()));
                        } catch (Exception ex) {
                            failures.add(String.valueOf(ex));
                        }
                    }
                }));
            }
            for (int v = 1; v <= 12; v++) {
                e.newGraph(paramGraph("v" + v));
            }
            stop.set(true);
            for (Thread t : clients) t.join(java.time.Duration.ofSeconds(15));

            assertThat(answers.get()).isGreaterThan(100);
            assertThat(failures).as("a query to a changed node waits for its replacement").isEmpty();
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void queries_during_a_re_init_wait_instead_of_being_rejected() throws Exception {
        var b = new GraphBuilder();
        add(b, "P");
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            var failures = new CopyOnWriteArrayList<String>();
            var answers = new AtomicInteger();
            var stop = new java.util.concurrent.atomic.AtomicBoolean();
            var client = Thread.ofVirtual().start(() -> {
                while (!stop.get()) {
                    try {
                        e.queryProcess("P", "q", Duration.ofSeconds(10)).toCompletableFuture()
                                .get(10, TimeUnit.SECONDS);
                        answers.incrementAndGet();
                    } catch (ExecutionException ex) {
                        failures.add(String.valueOf(ex.getCause()));
                    } catch (Exception ex) {
                        failures.add(String.valueOf(ex));
                    }
                }
            });
            for (int i = 0; i < 40; i++) {
                e.trigger("P", "refresh" + i);
                Thread.sleep(25);
            }
            stop.set(true);
            client.join(java.time.Duration.ofSeconds(15));

            assertThat(answers.get()).isGreaterThan(50);
            assertThat(failures).as("a query racing a re-init waits for the new state").isEmpty();
        } finally {
            e.close();
        }
    }

    /** Counts of query callbacks per process, to check a waiting query is announced exactly once. */
    static final class QueryCounts implements EngineObserver {
        final Map<String, AtomicInteger> sent = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> completed = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> failed = new ConcurrentHashMap<>();
        final List<String> reasons = new CopyOnWriteArrayList<>();

        @Override
        public void onQuerySent(String processName, java.util.UUID queryId, Class<?> messageType,
                                java.util.UUID parentQueryId) {
            sent.computeIfAbsent(processName, k -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public void onQueryCompleted(String processName, java.util.UUID queryId, Duration duration) {
            completed.computeIfAbsent(processName, k -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public void onQueryFailed(String processName, java.util.UUID queryId, String reason, Throwable cause) {
            failed.computeIfAbsent(processName, k -> new AtomicInteger()).incrementAndGet();
            reasons.add(processName + ":" + reason);
        }

        int count(Map<String, AtomicInteger> m, String name) {
            return m.getOrDefault(name, new AtomicInteger()).get();
        }
    }

    @Test
    @Timeout(30)
    void a_query_waiting_for_a_node_to_start_is_reported_to_observers_exactly_once() throws Exception {
        INIT_DELAY.put("Slow", 700L);
        var b = new GraphBuilder();
        add(b, "Slow");
        add(b, "Beta", "Slow");
        var counts = new QueryCounts();
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), counts);
        try {
            var install = CompletableFuture.runAsync(() -> e.newGraph(b.build()));
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions()
                    .until(() -> "Starting".equals(state(e, "Beta")));

            assertThat(e.queryProcess("Beta", "q", Duration.ofSeconds(5)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)).isEqualTo("Beta-ok");
            install.get(5, TimeUnit.SECONDS);

            await().atMost(Duration.ofSeconds(5)) // the observer callbacks settle with the reply
                    .untilAsserted(() -> assertThat(counts.count(counts.completed, "Beta")).isEqualTo(1));
            assertThat(counts.count(counts.sent, "Beta")).as("announced once, not per hand-over").isEqualTo(1);
            assertThat(counts.count(counts.failed, "Beta")).isZero();
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_query_that_times_out_waiting_for_a_node_is_reported_to_observers() throws Exception {
        FAILING.add("Broken");
        var b = new GraphBuilder();
        add(b, "Broken");
        add(b, "Downstream", "Broken");
        var counts = new QueryCounts();
        var e = engine(cfg(Duration.ofMillis(300), Duration.ofSeconds(1)), counts);
        try {
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions()
                    .until(() -> "Starting".equals(state(e, "Downstream")));

            assertThatThrownBy(() -> e.queryProcess("Downstream", "q", Duration.ofMillis(400))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);

            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(counts.count(counts.failed, "Downstream")).isEqualTo(1));
            assertThat(counts.count(counts.sent, "Downstream")).isEqualTo(1);
            assertThat(counts.reasons).contains("Downstream:timeout");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void close_while_the_first_graph_starts_names_the_node_it_was_waiting_for() throws Exception {
        INIT_DELAY.put("Slow", 5_000L);
        var b = new GraphBuilder();
        add(b, "Slow");
        var e = engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(1)), new EngineObserver() { });
        var install = CompletableFuture.runAsync(() -> e.newGraph(b.build()));
        await().atMost(Duration.ofSeconds(5)).ignoreExceptions()
                .until(() -> "Initializing".equals(state(e, "Slow")));

        e.close();

        assertThatThrownBy(() -> install.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Engine closed while 'Slow' was starting");
    }

    /** Its init queries a dependency repeatedly, like a process building state from it. */
    static final class QueryingNode implements ProcessInitializer, ProcessLoader {
        private final String name;
        private final String dependency;

        QueryingNode(String name, String dependency) {
            this.name = name;
            this.dependency = dependency;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INITS.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            var done = new CompletableFuture<Map<String, byte[]>>();
            Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < 30; i++) { // spans the window where the dependency is replaced
                        ctx.query(dependency, "q").toCompletableFuture().get(10, TimeUnit.SECONDS);
                        Thread.sleep(50);
                    }
                    done.complete(Map.of());
                } catch (Throwable t) {
                    done.completeExceptionally(t);
                }
            });
            return done;
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(name + "-ok"));
        }
    }

    /** Its cleanUp is slow, so a swap leaves the name without an FSM for that long. */
    static final class SlowCleanupNode implements io.fom.api.ParamProcessInitializer<Param>,
            io.fom.api.ParamProcessLoader<Param> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Param param) {
            INITS.computeIfAbsent("Dep", k -> new AtomicInteger()).incrementAndGet();
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, Param param) {
            return CompletableFuture.completedFuture(new Process() {
                @Override
                public CompletionStage<?> compute(QueryableContext c, Object q) {
                    return CompletableFuture.completedFuture("Dep-" + param.value());
                }

                @Override
                public CompletionStage<Void> cleanUp(io.fom.api.ProcessContext c) {
                    var done = new CompletableFuture<Void>();
                    Thread.ofVirtual().start(() -> {
                        try {
                            Thread.sleep(600); // the replacement cannot start before this finishes
                            done.complete(null);
                        } catch (InterruptedException ex) {
                            done.completeExceptionally(ex);
                        }
                    });
                    return done;
                }
            });
        }
    }

    @Test
    @Timeout(60)
    void a_query_from_an_init_waits_while_its_dependency_is_replaced() throws Exception {
        Supplier<Graph> graph = () -> new GraphBuilder()
                .addWithParam("Dep",
                        (Supplier<io.fom.api.ParamProcessInitializer<Param>>) SlowCleanupNode::new,
                        (Supplier<io.fom.api.ParamProcessLoader<Param>>) SlowCleanupNode::new,
                        new Param(PARAM.get()))
                .add("Consumer",
                        (Supplier<ProcessInitializer>) () -> new QueryingNode("Consumer", "Dep"),
                        (Supplier<ProcessLoader>) () -> new QueryingNode("Consumer", "Dep"),
                        "Dep")
                .build();
        var initFailures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                initFailures.add(processName + ": " + cause);
            }
        };
        var e = engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(1)), observer);
        try {
            e.newGraph(graph.get());
            // Consumer re-inits (its init queries Dep in a loop) while a swap replaces Dep.
            e.trigger("Consumer", "again");
            Thread.sleep(150);
            PARAM.set("v1");
            e.newGraph(graph.get());

            await().atMost(Duration.ofSeconds(30)).ignoreExceptions()
                    .until(() -> "Serving".equals(state(e, "Consumer")));
            assertThat(e.queryProcess("Consumer", "q", Duration.ofSeconds(5)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)).isEqualTo("Consumer-ok");
            assertThat(initFailures)
                    .as("an init query to a node being replaced waits for the replacement").isEmpty();
        } finally {
            e.close();
        }
    }

    /** Its compute answers by querying a declared dependency, inheriting the caller's deadline. */
    static final class DelegatingNode implements ProcessInitializer, ProcessLoader {
        private final String dependency;

        DelegatingNode(String dependency) {
            this.dependency = dependency;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> c.query(dependency, q));
        }
    }

    @Test
    @Timeout(30)
    void a_dependency_query_that_times_out_says_which_query_ran_out_of_time() throws Exception {
        HANGING_COMPUTE.add("Dep");
        var b = new GraphBuilder();
        add(b, "Dep");
        b.add("Front",
                (Supplier<ProcessInitializer>) () -> new DelegatingNode("Dep"),
                (Supplier<ProcessLoader>) () -> new DelegatingNode("Dep"),
                "Dep");
        var e = engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            for (int i = 0; i < 10; i++) { // the inherited deadline usually fires first
                Throwable cause = null;
                try {
                    e.queryProcess("Front", "q", Duration.ofMillis(300)).toCompletableFuture()
                            .get(5, TimeUnit.SECONDS);
                } catch (ExecutionException ex) {
                    cause = ex.getCause();
                }
                assertThat(cause).isInstanceOf(java.util.concurrent.TimeoutException.class);
                assertThat(cause.getMessage()).as("a timeout must say which query ran out of time").isNotNull();
                assertThat(cause.getMessage()).containsAnyOf("'Front'", "'Dep'");
            }
        } finally {
            e.close();
        }
    }

    record Ping(String text) implements java.io.Serializable { }

    @Test
    @Timeout(20)
    void a_swap_that_only_changes_the_routing_reports_a_change_and_reroutes() throws Exception {
        var backend = new InMemoryLogBackend();
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var toA = new GraphBuilder();
            add(toA, "A");
            add(toA, "B");
            assertThat(e.newGraph(toA.handlesFor("A", Ping.class).build())).isTrue();
            assertThat(e.query(new Ping("x")).toCompletableFuture().get(3, TimeUnit.SECONDS)).isEqualTo("A-ok");

            var toB = new GraphBuilder();
            add(toB, "A");
            add(toB, "B");
            boolean changed = e.newGraph(toB.handlesFor("B", Ping.class).build());

            assertThat(changed).as("the same query now goes to another process").isTrue();
            assertThat(e.query(new Ping("x")).toCompletableFuture().get(3, TimeUnit.SECONDS)).isEqualTo("B-ok");
            assertThat(inits("A")).as("no node was touched").isEqualTo(1);
            assertThat(inits("B")).isEqualTo(1);
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(20)
    void a_trigger_that_could_not_be_recorded_fails_instead_of_reporting_success() throws Exception {
        var backend = new InMemoryLogBackend();
        var b = new GraphBuilder();
        add(b, "A");
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            // Another instance takes the log over: our appends are refused from now on.
            backend.append(new io.fom.log.LogLeader(0, System.currentTimeMillis(), "someone-else"), "someone-else");

            assertThatThrownBy(() -> e.trigger("A", "refresh"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no longer the leader");
            assertThat(inits("A")).as("nothing was re-initialised").isEqualTo(1);
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(20)
    void rebuilding_the_same_graph_with_a_dynamic_route_reports_no_change() throws Exception {
        var backend = new InMemoryLogBackend();
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            Supplier<Graph> build = () -> {
                var b = new GraphBuilder();
                add(b, "A");
                add(b, "B");
                return b.route(Ping.class, (Ping ping) -> "A").build(); // a fresh lambda every time
            };
            assertThat(e.newGraph(build.get())).isTrue();

            assertThat(e.newGraph(build.get()))
                    .as("the same routing rebuilt is not a change").isFalse();
            assertThat(e.query(new Ping("x")).toCompletableFuture().get(3, TimeUnit.SECONDS)).isEqualTo("A-ok");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_refused_trigger_map_restarts_nothing() throws Exception {
        FAILING.add("D");
        var backend = new InMemoryLogBackend();
        var b = new GraphBuilder();
        add(b, "Alive");
        add(b, "D");
        var e = new Engine(cfg(Duration.ofMillis(300), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "Dead".equals(state(e, "D")));
            FAILING.remove("D");
            int deadInits = inits("D");
            backend.append(new io.fom.log.LogLeader(0, System.currentTimeMillis(), "other"), "other");

            assertThatThrownBy(() -> e.trigger(Map.of("Alive", "x", "D", "y")))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class);

            Thread.sleep(300);
            assertThat(inits("D")).as("the Dead process was not restarted either").isEqualTo(deadInits);
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_watcher_stops_when_this_instance_is_no_longer_the_leader() throws Exception {
        var backend = new InMemoryLogBackend();
        var b = new GraphBuilder();
        add(b, "W");
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            var checks = new AtomicInteger();
            var watcher = new ScheduledWatcher<>("W", Integer.class, 0, Duration.ZERO, Duration.ofMillis(50),
                    (Integer previous) -> java.util.Optional.of(checks.incrementAndGet()), null);
            var handle = e.watch(watcher);
            try {
                await().atMost(Duration.ofSeconds(5)).until(() -> checks.get() >= 2);
                backend.append(new io.fom.log.LogLeader(0, System.currentTimeMillis(), "other"), "other");

                await().atMost(Duration.ofSeconds(5)).until(() -> {
                    int seen = checks.get();
                    Thread.sleep(400);
                    return checks.get() == seen; // it stopped polling
                });
            } finally {
                handle.close();
            }
        } finally {
            e.close();
        }
    }

    /** A process whose cleanUp takes {@code CLEANUP_MILLIS} and records whether it was cancelled. */
    static final java.util.concurrent.atomic.AtomicLong CLEANUP_MILLIS =
            new java.util.concurrent.atomic.AtomicLong();
    static final java.util.concurrent.atomic.AtomicBoolean CLEANUP_CANCELLED =
            new java.util.concurrent.atomic.AtomicBoolean();

    static final class SlowCleanup implements ProcessInitializer, ProcessLoader {
        private final String name;

        SlowCleanup(String name) {
            this.name = name;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INITS.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            long delay = INIT_DELAY.getOrDefault(name, 0L);
            if (delay == 0) return CompletableFuture.completedFuture(Map.of());
            var done = new CompletableFuture<Map<String, byte[]>>();
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(delay);
                    done.complete(Map.of());
                } catch (InterruptedException ex) {
                    done.completeExceptionally(ex);
                }
            });
            return done;
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture(new Process() {
                @Override
                public CompletionStage<?> compute(QueryableContext c, Object q) {
                    return CompletableFuture.completedFuture(name + "-ok");
                }

                @Override
                public CompletionStage<Void> cleanUp(io.fom.api.ProcessContext c) {
                    var done = new CompletableFuture<Void>();
                    long millis = CLEANUP_MILLIS.get();
                    done.whenComplete((v, e) -> {
                        if (e instanceof java.util.concurrent.CancellationException) CLEANUP_CANCELLED.set(true);
                    });
                    if (millis >= 0) {
                        Thread.ofVirtual().start(() -> {
                            try {
                                Thread.sleep(millis);
                                done.complete(null);
                            } catch (InterruptedException ex) {
                                done.completeExceptionally(ex);
                            }
                        });
                    } // millis < 0: never completes on its own
                    return done;
                }
            });
        }
    }

    @Test
    @Timeout(30)
    void a_trigger_for_a_dead_process_is_recorded_and_checks_leadership() throws Exception {
        FAILING.add("X");
        var backend = new InMemoryLogBackend();
        var b = new GraphBuilder();
        add(b, "G");
        add(b, "X");
        // A failed re-init drives it Dead only under RELEASE_FIRST.
        var e = new Engine(cfg(Duration.ofMillis(300), Duration.ofSeconds(1)).withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "Dead".equals(state(e, "X")));
            FAILING.remove("X");

            int triggersBefore = countTriggers(backend, "X");
            assertThat(e.trigger("X", "restart")).isTrue();

            assertThat(countTriggers(backend, "X")).as("the request is durable for a Dead process too")
                    .isEqualTo(triggersBefore + 1);
            await().atMost(Duration.ofSeconds(10)).ignoreExceptions().until(() -> "Serving".equals(state(e, "X")));

            // Drive it Dead again while we are still the leader, then lose the log.
            FAILING.add("X");
            e.trigger("X", "fail-it");
            await().atMost(Duration.ofSeconds(10)).ignoreExceptions().until(() -> "Dead".equals(state(e, "X")));
            int initsBefore = inits("X");
            backend.append(new io.fom.log.LogLeader(0, System.currentTimeMillis(), "other"), "other");

            assertThatThrownBy(() -> e.trigger("X", "again"))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class);
            Thread.sleep(300);
            assertThat(inits("X")).as("a Dead process is not restarted once the log is gone")
                    .isEqualTo(initsBefore);
        } finally {
            e.close();
        }
    }

    private static int countTriggers(InMemoryLogBackend backend, String name) {
        int count = 0;
        for (io.fom.log.LogEvent event : backend.getBetween(0, backend.length())) {
            if (event instanceof io.fom.log.LogTrigger t && t.processNames().contains(name)) count++;
        }
        return count;
    }

    @Test
    @Timeout(30)
    void a_watcher_on_a_dead_process_stops_when_the_log_was_taken_over() throws Exception {
        FAILING.add("X");
        var backend = new InMemoryLogBackend();
        var b = new GraphBuilder();
        add(b, "G");
        add(b, "X");
        var e = new Engine(cfg(Duration.ofMillis(300), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "Dead".equals(state(e, "X")));
            backend.append(new io.fom.log.LogLeader(0, System.currentTimeMillis(), "other"), "other");

            var checks = new AtomicInteger();
            var watcher = new ScheduledWatcher<>("X", Integer.class, 0, Duration.ZERO, Duration.ofMillis(60),
                    (Integer previous) -> java.util.Optional.of(checks.incrementAndGet()), null);
            var handle = e.watch(watcher);
            try {
                await().atMost(Duration.ofSeconds(5)).until(() -> checks.get() >= 1);
                await().atMost(Duration.ofSeconds(5)).until(() -> {
                    int seen = checks.get();
                    Thread.sleep(400);
                    return checks.get() == seen; // it stopped instead of retrying every tick
                });
            } finally {
                handle.close();
            }
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void cleanup_reports_how_long_it_took_and_is_cancelled_when_it_outlives_its_budget() throws Exception {
        var durations = new CopyOnWriteArrayList<Duration>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onCleanupCompleted(String processName, Sid sid, boolean ok, Duration duration) {
                durations.add(duration);
            }
        };
        var b = new GraphBuilder();
        b.add("C", (Supplier<ProcessInitializer>) () -> new SlowCleanup("C"),
                (Supplier<ProcessLoader>) () -> new SlowCleanup("C"));
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofMillis(400)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), observer);
        try {
            CLEANUP_MILLIS.set(150);
            e.newGraph(b.build());
            e.trigger("C", "again"); // retires the first generation: its cleanUp runs
            await().atMost(Duration.ofSeconds(10)).until(() -> !durations.isEmpty());

            assertThat(durations.get(0)).as("the measured cleanup cost, not zero")
                    .isGreaterThanOrEqualTo(Duration.ofMillis(100));

            // A cleanUp that never finishes must be cancelled once its budget is over.
            CLEANUP_CANCELLED.set(false);
            CLEANUP_MILLIS.set(-1);
            e.trigger("C", "and-again");
            await().atMost(Duration.ofSeconds(10)).until(CLEANUP_CANCELLED::get);
        } finally {
            CLEANUP_MILLIS.set(0);
            e.close();
        }
    }

    @Test
    @Timeout(40)
    void a_generation_that_never_served_reports_no_cleanup() throws Exception {
        var durations = new CopyOnWriteArrayList<Duration>();
        var cleaned = new CopyOnWriteArrayList<Sid>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onCleanupCompleted(String processName, Sid sid, boolean ok, Duration duration) {
                durations.add(duration);
                cleaned.add(sid);
            }
        };
        var b = new GraphBuilder();
        b.add("C", (Supplier<ProcessInitializer>) () -> new SlowCleanup("C"),
                (Supplier<ProcessLoader>) () -> new SlowCleanup("C"));
        var e = new Engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), observer);
        try {
            CLEANUP_MILLIS.set(150);
            e.newGraph(b.build());
            e.trigger("C", "again"); // a real cleanup: its duration is measured
            await().atMost(Duration.ofSeconds(10)).until(() -> !durations.isEmpty());
            assertThat(durations.get(0)).isGreaterThanOrEqualTo(Duration.ofMillis(100));

            // The next generation hangs in init, so it never has a process to clean up. (KEEP_OLD: it
            // is made beside the serving version, which close() does clean up.)
            Sid serving = e.introspect().toCompletableFuture().get().graph().nodes().get(0).sid();
            INIT_DELAY.put("C", 15_000L);
            e.trigger("C", "hangs");
            await().atMost(Duration.ofSeconds(10)).ignoreExceptions()
                    .until(() -> "Initializing".equals(replacement(e, "C")));
            int before = cleaned.size();

            e.close();

            Thread.sleep(300);
            assertThat(cleaned.subList(before, cleaned.size()))
                    .as("only the serving version is cleaned up; the one that never had a process reports nothing")
                    .containsExactly(serving);
        } finally {
            INIT_DELAY.remove("C");
            CLEANUP_MILLIS.set(0);
            e.close();
        }
    }

    private static EngineConfig cfgWithDedup(Duration dedupWindow) {
        return new EngineConfig(
                Duration.ofSeconds(20), Duration.ofSeconds(5), Duration.ofSeconds(1),
                Duration.ofSeconds(10), dedupWindow,
                Duration.ofMillis(20), Duration.ofMillis(200), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    @Timeout(30)
    void losing_the_log_between_an_accepted_trigger_and_its_re_init_keeps_the_state() throws Exception {
        var backend = new InMemoryLogBackend();
        var b = new GraphBuilder();
        add(b, "A");
        var e = new Engine(cfgWithDedup(Duration.ofMillis(500)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            assertThat(e.trigger("A", "refresh")).isTrue(); // accepted and recorded while leader

            // Another instance takes the log over before the debounced re-init runs.
            backend.append(new io.fom.log.LogLeader(0, System.currentTimeMillis(), "other"), "other");
            Thread.sleep(1_500);

            assertThat(state(e, "A")).as("it keeps the state it has instead of dying with none")
                    .isEqualTo("Serving");
            assertThat(e.queryProcess("A", "q", Duration.ofSeconds(3)).toCompletableFuture()
                    .get(3, TimeUnit.SECONDS)).isEqualTo("A-ok");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void losing_the_log_during_a_first_init_says_why_the_node_is_dead() throws Exception {
        INIT_DELAY.put("A", 600L);
        var backend = new InMemoryLogBackend();
        var b = new GraphBuilder();
        add(b, "A");
        var e = new Engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var install = CompletableFuture.runAsync(() -> e.newGraph(b.build()));
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions()
                    .until(() -> "Initializing".equals(state(e, "A")));
            backend.append(new io.fom.log.LogLeader(0, System.currentTimeMillis(), "other"), "other");

            assertThatThrownBy(() -> install.get(15, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(io.fom.api.LeadershipLostException.class);
            var node = e.introspect().toCompletableFuture().get().graph().nodes().stream()
                    .filter(n -> n.name().equals("A")).findFirst().orElseThrow();
            assertThat(node.state()).isEqualTo("Dead");
            assertThat(node.lastException()).as("an operator can see why it died").contains("Lost leadership");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(60)
    void removing_a_process_hung_in_load_does_not_wait_for_its_budget() throws Exception {
        LOAD_DELAY.put("Hang", 30_000L);
        var first = new GraphBuilder();
        add(first, "A");
        var next = new GraphBuilder();
        add(next, "A");
        add(next, "Hang");
        var e = new Engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(first.build());
            var install = CompletableFuture.runAsync(() -> {
                try {
                    e.newGraph(next.build());
                } catch (RuntimeException expected) {
                    // the hung node is cancelled out from under it
                }
            });
            await().atMost(Duration.ofSeconds(10)).ignoreExceptions()
                    .until(() -> "Loading".equals(state(e, "Hang")));

            long start = System.nanoTime();
            e.remove(List.of("Hang"));

            assertThat(Duration.ofNanos(System.nanoTime() - start))
                    .as("a node hung in load is cancelled, like one hung in init")
                    .isLessThan(Duration.ofSeconds(5));
            // The loader's own work is not cancelled (nothing can force that); a Process it
            // produces late is released instead — see ProcessFsmRobustnessTest.
            install.get(15, TimeUnit.SECONDS);
            assertThat(e.currentGraph().nodes()).doesNotContainKey("Hang");
        } finally {
            LOAD_DELAY.remove("Hang");
            e.close();
        }
    }

    @Test
    @Timeout(20)
    void archives_can_be_purged_by_hand_after_manual_snapshots(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        var b = new GraphBuilder();
        add(b, "A");
        try (var backend = new io.fom.log.FileLogBackend(dir.resolve("log.bin"))) {
            var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                    new JavaSerializableSerDe(), new EngineObserver() { });
            try {
                e.newGraph(b.build());
                for (int i = 0; i < 4; i++) {
                    e.snapshot().toCompletableFuture().get(10, TimeUnit.SECONDS);
                }
                long archives = countArchives(dir);
                assertThat(archives).as("a manual snapshot does not purge").isGreaterThanOrEqualTo(4);

                e.purgeArchives(2);

                assertThat(countArchives(dir)).isEqualTo(2);
                assertThatThrownBy(() -> e.purgeArchives(-1)).isInstanceOf(IllegalArgumentException.class);
            } finally {
                e.close();
            }
        }
    }

    private static long countArchives(java.nio.file.Path dir) throws Exception {
        try (var files = java.nio.file.Files.list(dir)) {
            return files.filter(f -> f.getFileName().toString().contains(".archived.")).count();
        }
    }

    /** Refuses exactly one kind of event, to reproduce a takeover landing mid-operation. */
    static final class RefusingBackend implements io.fom.log.LogBackend {
        private final io.fom.log.LogBackend delegate;
        private final Class<? extends io.fom.log.LogEvent> refused;
        private final Supplier<RuntimeException> byThrowing;
        volatile RuntimeException purgeFailure;

        RefusingBackend(io.fom.log.LogBackend delegate, Class<? extends io.fom.log.LogEvent> refused) {
            this(delegate, refused, null);
        }

        RefusingBackend(io.fom.log.LogBackend delegate,
                        Class<? extends io.fom.log.LogEvent> refused,
                        Supplier<RuntimeException> byThrowing) {
            this.delegate = delegate;
            this.refused = refused;
            this.byThrowing = byThrowing;
        }

        @Override
        public java.util.Optional<io.fom.log.LogEvent> append(io.fom.log.LogEvent event, String leaderInstanceId) {
            if (refused.isInstance(event)) {
                if (byThrowing != null) throw byThrowing.get();
                return java.util.Optional.empty();
            }
            return delegate.append(event, leaderInstanceId);
        }

        @Override
        public void purgeArchives(int keepHistory) {
            if (purgeFailure != null) throw purgeFailure;
            delegate.purgeArchives(keepHistory);
        }

        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public io.fom.log.LogEvent get(int position) { return delegate.get(position); }
        @Override public io.fom.log.LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public SnapshotResult compact(List<io.fom.log.LogEvent> events, String leaderInstanceId) {
            return delegate.compact(events, leaderInstanceId);
        }
        @Override public io.fom.log.LogBackendReport introspect() { return delegate.introspect(); }
        @Override public void close() { delegate.close(); }
    }

    @Test
    @Timeout(30)
    void a_removal_that_cannot_retire_the_state_says_so() throws Exception {
        var b = new GraphBuilder();
        add(b, "A");
        add(b, "Z");
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)),
                new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogDead.class),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(b.build());

            assertThatThrownBy(() -> e.remove(List.of("Z")))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class)
                    .hasMessageContaining("could not retire the state of [Z]");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void an_init_that_works_before_returning_its_stage_is_still_bounded() throws Exception {
        INIT_SYNC_SLEEP.put("Slow", 2_000L);
        var b = new GraphBuilder();
        add(b, "Slow");
        var e = new Engine(cfg(Duration.ofMillis(400), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            try {
                e.newGraph(b.build());
            } catch (RuntimeException expected) {
                // the node may fail the start outright; either way it must not come up late
            }

            await().atMost(Duration.ofSeconds(10)).until(() -> "Dead".equals(state(e, "Slow")));
            assertThat(e.introspect().toCompletableFuture().get().graph().nodes().stream()
                    .filter(n -> n.name().equals("Slow")).findFirst().orElseThrow().lastException())
                    .as("the init budget bounds the whole attempt, including work done before the stage")
                    .contains("Timeout");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_synchronous_cleanup_over_its_budget_is_reported_as_failed() throws Exception {
        var outcomes = new CopyOnWriteArrayList<Boolean>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onCleanupCompleted(String processName, Sid sid, boolean ok, Duration duration) {
                outcomes.add(ok);
            }
        };
        var b = new GraphBuilder();
        b.add("C", (Supplier<ProcessInitializer>) () -> new SyncCleanup(),
                (Supplier<ProcessLoader>) () -> new SyncCleanup());
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofMillis(300)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(b.build());
            e.trigger("C", "again"); // retires the first generation: its cleanUp blocks for 1.5 s

            await().atMost(Duration.ofSeconds(10)).until(() -> !outcomes.isEmpty());
            assertThat(outcomes.get(0)).as("a cleanUp over its budget is a failed cleanup").isFalse();
        } finally {
            e.close();
        }
    }

    /** Its cleanUp does the work before returning a completed stage. */
    static final class SyncCleanup implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture(new Process() {
                @Override
                public CompletionStage<?> compute(QueryableContext c, Object q) {
                    return CompletableFuture.completedFuture("C-ok");
                }

                @Override
                public CompletionStage<Void> cleanUp(io.fom.api.ProcessContext c) {
                    try {
                        Thread.sleep(1_500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return CompletableFuture.completedFuture(null);
                }
            });
        }
    }

    @Test
    @Timeout(20)
    void a_deposed_leader_cannot_purge_archives_or_pause() throws Exception {
        var backend = new InMemoryLogBackend();
        var b = new GraphBuilder();
        add(b, "A");
        add(b, "B");
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            backend.append(new io.fom.log.LogLeader(0, System.currentTimeMillis(), "other"), "other");

            assertThatThrownBy(() -> e.purgeArchives(1))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class)
                    .hasMessageContaining("no archive was removed");
            assertThatThrownBy(() -> e.pause(List.of("A", "B")))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class)
                    .hasMessageContaining("nothing was paused");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_swap_that_cannot_retire_state_still_starts_the_new_nodes() throws Exception {
        Supplier<Graph> graph = () -> new GraphBuilder()
                .addWithParam("Dep",
                        (Supplier<io.fom.api.ParamProcessInitializer<Param>>) ParamNode::new,
                        (Supplier<io.fom.api.ParamProcessLoader<Param>>) ParamNode::new,
                        new Param(PARAM.get()))
                .add("Z", (Supplier<ProcessInitializer>) () -> new Node("Z"),
                        (Supplier<ProcessLoader>) () -> new Node("Z"))
                .build();
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)),
                new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogDead.class),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(graph.get());
            PARAM.set("v1");

            // Removes Z and changes Dep: the refused LogDead must not skip Dep's respawn.
            assertThatThrownBy(() -> e.newGraph(new GraphBuilder()
                    .addWithParam("Dep",
                            (Supplier<io.fom.api.ParamProcessInitializer<Param>>) ParamNode::new,
                            (Supplier<io.fom.api.ParamProcessLoader<Param>>) ParamNode::new,
                            new Param("v1"))
                    .build()))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class)
                    .hasMessageContaining("could not retire the state of");

            assertThat(state(e, "Dep")).as("the changed node is started despite the refusal").isEqualTo("Serving");
            assertThat(e.queryProcess("Dep", "q", Duration.ofSeconds(5)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)).isEqualTo("v1");
            assertThat(state(e, "Z")).as("the removed node is gone").isEqualTo("absent");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_process_the_engine_refuses_to_serve_is_cleaned_up_exactly_once() throws Exception {
        var cleanups = new AtomicInteger();
        var b = new GraphBuilder();
        b.add("L", (Supplier<ProcessInitializer>) () -> new CountingCleanup(cleanups),
                (Supplier<ProcessLoader>) () -> new CountingCleanup(cleanups));
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)),
                new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogLoaded.class),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            // The load succeeds but its LogLoaded is refused: the Process is released once.
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);

            await().atMost(Duration.ofSeconds(5)).until(() -> cleanups.get() > 0);
            Thread.sleep(300); // a second release would land by now
            assertThat(cleanups).as("cleanUp is called exactly once per Process").hasValue(1);
        } finally {
            e.close();
        }
    }

    /** Counts how often its Process is cleaned up. */
    static final class CountingCleanup implements ProcessInitializer, ProcessLoader {
        private final AtomicInteger cleanups;

        CountingCleanup(AtomicInteger cleanups) {
            this.cleanups = cleanups;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture(new Process() {
                @Override
                public CompletionStage<?> compute(QueryableContext c, Object q) {
                    return CompletableFuture.completedFuture("L-ok");
                }

                @Override
                public CompletionStage<Void> cleanUp(io.fom.api.ProcessContext c) {
                    cleanups.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }
            });
        }
    }

    @Test
    @Timeout(30)
    void an_explicit_stop_interrupts_an_init_that_blocks_before_returning_its_stage() throws Exception {
        INIT_SYNC_SLEEP.put("Blocking", 20_000L);
        var b = new GraphBuilder();
        add(b, "Blocking");
        var e = new Engine(cfg(Duration.ofSeconds(30), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        var starting = CompletableFuture.runAsync(() -> e.newGraph(b.build()));
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> "Initializing".equals(state(e, "Blocking")));

            long startedAt = System.nanoTime();
            e.pause(List.of("Blocking"));
            Duration paused = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(paused).as("a pause does not wait for a blocking init").isLessThan(Duration.ofSeconds(5));
            await().atMost(Duration.ofSeconds(5)).until(INIT_INTERRUPTED::get);
            assertThatThrownBy(() -> starting.get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_resume_that_cannot_be_persisted_leaves_the_process_paused() throws Exception {
        var backend = new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogResumed.class);
        var b = new GraphBuilder();
        add(b, "A");
        add(b, "B");
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            e.pause(List.of("B"));

            assertThatThrownBy(() -> e.resume(List.of("B")))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class)
                    .hasMessageContaining("it stays paused");

            assertThat(state(e, "B")).as("a node the log still calls paused does not serve").isEqualTo("Paused");
            assertThatThrownBy(() -> e.queryProcess("B", "q", Duration.ofSeconds(2)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)).hasMessageContaining("paused");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void losing_the_log_during_a_load_is_reported_to_the_observer() throws Exception {
        var loadFailures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) {
                loadFailures.add(processName + ": " + cause);
            }
        };
        var b = new GraphBuilder();
        add(b, "A");
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)),
                new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogLoaded.class),
                new JavaSerializableSerDe(), observer);
        try {
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);

            await().atMost(Duration.ofSeconds(5)).until(() -> !loadFailures.isEmpty());
            assertThat(loadFailures.get(0)).contains("LeadershipLostException");
        } finally {
            e.close();
        }
    }


    /** The same as above, for a backend that fails by throwing rather than by refusing. */
    @Test
    @Timeout(30)
    void a_resume_whose_append_throws_leaves_the_process_paused() throws Exception {
        var backend = new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogResumed.class,
                () -> new IllegalStateException("append failed (injected)"));
        var b = new GraphBuilder();
        add(b, "A");
        add(b, "B");
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            e.pause(List.of("B"));

            assertThatThrownBy(() -> e.resume(List.of("B")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("injected");

            assertThat(state(e, "B")).as("routing and the report must not disagree").isEqualTo("Paused");
            assertThatThrownBy(() -> e.queryProcess("B", "q", Duration.ofSeconds(2)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)).hasMessageContaining("paused");
        } finally {
            e.close();
        }
    }

    /**
     * A backend that reports a takeover by throwing (Postgres, fenced) must end the node the
     * same way as one that refuses the append — not retry the same permanent failure for the
     * whole init budget and then report a timeout.
     */
    @Test
    @Timeout(20) // below the init budget, so a regression fails the assertion rather than the test
    void a_thrown_leadership_loss_is_not_retried() throws Exception {
        var initFailures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                initFailures.add(processName + "#" + attempt + ": " + cause);
            }
        };
        var b = new GraphBuilder();
        add(b, "A");
        var e = new Engine(cfg(Duration.ofSeconds(30), Duration.ofSeconds(1)),
                new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogInitialized.class,
                        () -> new io.fom.api.LeadershipLostException("fenced (injected)")),
                new JavaSerializableSerDe(), observer);
        try {
            long startedAt = System.nanoTime();
            assertThatThrownBy(() -> e.newGraph(b.build()))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class);
            Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(took).as("no retries: the failure is permanent").isLessThan(Duration.ofSeconds(10));
            assertThat(state(e, "A")).isEqualTo("Dead");
            assertThat(initFailures).as("reported once, not once per retry").hasSize(1);
            assertThat(initFailures.get(0)).contains("LeadershipLostException");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_dropped_reinit_is_reported_to_the_observer() throws Exception {
        var initFailures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                initFailures.add(processName + ": " + cause);
            }
        };
        var backend = new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogDead.class);
        var b = new GraphBuilder();
        add(b, "A");
        // The LogDead a RELEASE_FIRST re-init writes first; under KEEP_OLD it only follows the switch.
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)).withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend,
                new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(b.build());
            e.trigger("A", "again"); // the re-init cannot retire the current state

            await().atMost(Duration.ofSeconds(5)).until(() -> !initFailures.isEmpty());
            assertThat(initFailures.get(0)).contains("cannot re-initialise A");
            assertThat(state(e, "A")).as("it keeps serving the state it has").isEqualTo("Serving");
            assertThat(e.queryProcess("A", "q", Duration.ofSeconds(5)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)).isEqualTo("A-ok");
        } finally {
            e.close();
        }
    }

    @Test
    @Timeout(30)
    void a_refused_archive_purge_fails_the_call() throws Exception {
        var backend = new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogDead.class);
        var b = new GraphBuilder();
        add(b, "A");
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            backend.purgeFailure = new io.fom.api.LeadershipLostException("fenced (injected)");

            assertThatThrownBy(() -> e.purgeArchives(1))
                    .as("the caller asked for the archives to go; a refusal must not be swallowed")
                    .isInstanceOf(io.fom.api.LeadershipLostException.class);
        } finally {
            e.close();
        }
    }

    /** The load half of the same contract. */
    @Test
    @Timeout(20)
    void a_thrown_leadership_loss_during_load_is_not_retried() throws Exception {
        var loadFailures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) {
                loadFailures.add(processName + "#" + attempt + ": " + cause);
            }
        };
        var b = new GraphBuilder();
        add(b, "A");
        var e = new Engine(cfg(Duration.ofSeconds(30), Duration.ofSeconds(1)),
                new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogLoaded.class,
                        () -> new io.fom.api.LeadershipLostException("fenced (injected)")),
                new JavaSerializableSerDe(), observer);
        try {
            long startedAt = System.nanoTime();
            assertThatThrownBy(() -> e.newGraph(b.build()))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class);

            assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                    .as("no retry loop through a fresh init").isLessThan(Duration.ofSeconds(10));
            assertThat(state(e, "A")).isEqualTo("Dead");
            assertThat(loadFailures).hasSize(1);
            assertThat(loadFailures.get(0)).contains("LeadershipLostException");
        } finally {
            e.close();
        }
    }

    /** The same as the refusal, for a backend that reports the takeover by throwing. */
    @Test
    @Timeout(30)
    void a_reinit_the_log_throws_at_is_dropped_not_retried_forever() throws Exception {
        var initFailures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                initFailures.add(processName + ": " + cause);
            }
        };
        var backend = new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogDead.class,
                () -> new io.fom.api.LeadershipLostException("fenced (injected)"));
        var b = new GraphBuilder();
        add(b, "A");
        // The LogDead a RELEASE_FIRST re-init writes first; under KEEP_OLD it only follows the switch.
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)).withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend,
                new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(b.build());
            e.trigger("A", "again");

            await().atMost(Duration.ofSeconds(5)).until(() -> !initFailures.isEmpty());
            assertThat(initFailures.get(0)).contains("fenced (injected)");
            assertThat(state(e, "A")).as("it keeps serving the state it has").isEqualTo("Serving");
            Thread.sleep(700); // a retry loop would report again and again
            assertThat(initFailures).as("dropped once, not retried for ever").hasSize(1);
        } finally {
            e.close();
        }
    }

    /** An event the log can never store is permanent too, and must not be retried for ever. */
    @Test
    @Timeout(30)
    void a_reinit_the_log_refuses_outright_is_dropped_not_retried_forever() throws Exception {
        var initFailures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                initFailures.add(processName + ": " + cause);
            }
        };
        var backend = new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogDead.class,
                () -> new IllegalArgumentException("event too large (injected)"));
        var b = new GraphBuilder();
        add(b, "A");
        // The LogDead a RELEASE_FIRST re-init writes first; under KEEP_OLD it only follows the switch.
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)).withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend,
                new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(b.build());
            e.trigger("A", "again");

            await().atMost(Duration.ofSeconds(5)).until(() -> !initFailures.isEmpty());
            assertThat(initFailures.get(0)).contains("event too large (injected)");
            Thread.sleep(700);
            assertThat(initFailures).as("dropped once, not retried for ever").hasSize(1);
            assertThat(state(e, "A")).isEqualTo("Serving");
        } finally {
            e.close();
        }
    }

    /** A dropped re-init must not report again on every further trigger for the same state. */
    @Test
    @Timeout(30)
    void a_dropped_reinit_is_reported_once_per_state_however_often_it_is_requested() throws Exception {
        var initFailures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                initFailures.add(processName + ": " + cause);
            }
        };
        var backend = new RefusingBackend(new InMemoryLogBackend(), io.fom.log.LogDead.class);
        var b = new GraphBuilder();
        add(b, "A");
        // The LogDead a RELEASE_FIRST re-init writes first; under KEEP_OLD it only follows the switch.
        var e = new Engine(cfgWithDedup(Duration.ofMillis(10)).withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend,
                new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(b.build());
            for (int i = 0; i < 6; i++) {
                e.trigger("A", "again-" + i);
                Thread.sleep(60);
            }

            await().atMost(Duration.ofSeconds(5)).until(() -> !initFailures.isEmpty());
            Thread.sleep(300);
            assertThat(initFailures).as("one report for the state it is stuck on").hasSize(1);
        } finally {
            e.close();
        }
    }

    /** A slow cleanUp must not be reported as a lost state: the LogDead lands before it starts. */
    @Test
    @Timeout(30)
    void a_removal_whose_cleanup_overruns_still_reports_the_state_as_retired() throws Exception {
        var b = new GraphBuilder();
        add(b, "Keep");
        b.add("Drop", (Supplier<ProcessInitializer>) () -> new SlowCleanup("Drop"),
                (Supplier<ProcessLoader>) () -> new SlowCleanup("Drop"));
        var backend = new InMemoryLogBackend();
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofMillis(300)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            CLEANUP_MILLIS.set(1_500); // well past the 300 ms cleanup budget
            e.newGraph(b.build());

            e.remove(List.of("Drop")); // its cleanUp outruns the budget

            long deadRecords = java.util.Arrays.stream(backend.getBetween(0, backend.length()))
                    .filter(ev -> ev instanceof io.fom.log.LogDead).count();
            assertThat(deadRecords).as("the state really was retired").isEqualTo(1);
            assertThat(state(e, "Drop")).isEqualTo("absent");
        } finally {
            e.close();
        }
    }

    /** Independent nodes start in the order they were added, the same way in every JVM. */
    @Test
    @Timeout(30)
    void the_order_of_independent_nodes_is_the_order_they_were_added() {
        var names = List.of("Zulu", "Alpha", "Mike", "Bravo", "Yankee", "Charlie");
        var b = new GraphBuilder();
        for (String name : names) add(b, name);

        assertThat(b.build().topologicalOrder().stream().map(io.fom.ProcessNode::name))
                .as("not Map.copyOf's per-JVM iteration order")
                .containsExactlyElementsOf(names);
    }

    /**
     * A start cancelled after its init landed leaves state in the log that its own FSM can no
     * longer retire (a dead FSM cannot write). Removing the process must retire it, or the caller
     * is told the process is gone while a restart with the same definition would warm-load it.
     */
    @Test
    @Timeout(30)
    void removing_a_cancelled_process_retires_the_state_it_left_behind() throws Exception {
        var backend = new InMemoryLogBackend();
        var b = new GraphBuilder();
        add(b, "Keep");
        LOAD_DELAY.put("Slow", 10_000L);
        b.add("Slow", (Supplier<ProcessInitializer>) () -> new Node("Slow"),
                (Supplier<ProcessLoader>) () -> new Node("Slow"));
        var e = new Engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        var starting = CompletableFuture.runAsync(() -> e.newGraph(b.build()));
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> "Loading".equals(state(e, "Slow")));
            e.cancelInit("Slow").toCompletableFuture().get(5, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(5)).until(() -> "Dead".equals(state(e, "Slow")));
            assertThat(liveInitialized(backend, "Slow"))
                    .as("the init landed, so its state is in the log and nothing retired it").isTrue();

            e.remove(List.of("Slow"));

            assertThat(liveInitialized(backend, "Slow"))
                    .as("the removal retired what the cancelled start left behind").isFalse();
        } finally {
            starting.handle((v, t) -> null).join();
            e.close();
        }
    }

    /** Whether the log still holds an un-retired {@code LogInitialized} for {@code name}. */
    private static boolean liveInitialized(InMemoryLogBackend backend, String name) {
        var live = new java.util.HashSet<Sid>();
        for (io.fom.log.LogEvent event : backend.getBetween(0, backend.length())) {
            if (event instanceof io.fom.log.LogInitialized init && init.sid().processName().equals(name)) {
                live.add(init.sid());
            } else if (event instanceof io.fom.log.LogDead dead) {
                live.remove(dead.sid());
            }
        }
        return !live.isEmpty();
    }


    /** A dependency the node does not declare is a programming error: fail now, not in a minute. */
    @Test
    @Timeout(30)
    void querying_an_undeclared_dependency_fails_the_node_at_once() throws Exception {
        var b = new GraphBuilder();
        add(b, "Dep");
        b.add("Asker", (Supplier<ProcessInitializer>) () -> new AskingNode("Typo", false),
                (Supplier<ProcessLoader>) () -> new AskingNode("Typo", false), "Dep");
        var e = new Engine(cfg(Duration.ofSeconds(30), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            long startedAt = System.nanoTime();
            assertThatThrownBy(() -> e.newGraph(b.build()))
                    .as("not an InitializationTimeoutException a minute later")
                    .isInstanceOf(io.fom.api.UndeclaredDependencyException.class)
                    .hasMessageContaining("No such dependency: Typo")
                    .hasMessageContaining("declared: [Dep]");

            assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                    .as("no retry loop: the graph will not grow the dependency")
                    .isLessThan(Duration.ofSeconds(10));
        } finally {
            e.close();
        }
    }

    /** The same, for a loader that asks — the load path needs its own carve-out. */
    @Test
    @Timeout(30)
    void a_load_querying_an_undeclared_dependency_fails_the_node_at_once() throws Exception {
        var b = new GraphBuilder();
        add(b, "Dep");
        b.add("Asker", (Supplier<ProcessInitializer>) () -> new AskingNode("Typo", true),
                (Supplier<ProcessLoader>) () -> new AskingNode("Typo", true), "Dep");
        var e = new Engine(cfg(Duration.ofSeconds(30), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            long startedAt = System.nanoTime();
            assertThatThrownBy(() -> e.newGraph(b.build()))
                    .isInstanceOf(io.fom.api.UndeclaredDependencyException.class);

            assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                    .as("no fresh-init loop on a name the graph will never grow")
                    .isLessThan(Duration.ofSeconds(10));
        } finally {
            e.close();
        }
    }

    /**
     * A consumer must not inherit a dependency's programming error as its own verdict. The case
     * that matters: the consumer is already running, and its query waits at a dependency whose
     * re-init then dies of a typo. The consumer must retry like after any other dependency failure
     * — it used to find the dependency's UndeclaredDependencyException in the cause chain of its own
     * rejected query and die on the spot, for good.
     */
    @Test
    @Timeout(60)
    void a_consumer_waiting_on_a_broken_dependency_is_not_blamed_for_its_typo() throws Exception {
        var failures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                failures.add(processName + "#" + attempt + ": " + cause);
            }
        };
        SUPPLIER_TYPO.set(false);
        var b = new GraphBuilder();
        b.add("Supplier", (Supplier<ProcessInitializer>) TypoSupplier::new,
                (Supplier<ProcessLoader>) TypoSupplier::new);
        b.add("Consumer", (Supplier<ProcessInitializer>) () -> new QueryingNode("Consumer", "Supplier"),
                (Supplier<ProcessLoader>) () -> new QueryingNode("Consumer", "Supplier"), "Supplier");
        // RELEASE_FIRST: a KEEP_OLD supplier whose re-init dies keeps answering with its old version.
        var e = new Engine(cfg(Duration.ofSeconds(10), Duration.ofSeconds(1))
                .withReinitStrategy(ReinitStrategy.RELEASE_FIRST), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), observer);
        try {
            e.newGraph(b.build());
            failures.clear();

            // Both re-init at once: the consumer's query waits at the supplier, whose re-init dies.
            SUPPLIER_TYPO.set(true);
            e.trigger(java.util.Map.of("Supplier", "again", "Consumer", "again"));

            await().atMost(Duration.ofSeconds(10)).until(() -> "Dead".equals(state(e, "Supplier")));
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> failures.stream().filter(f -> f.startsWith("Consumer#")).count() >= 2);
            assertThat(failures)
                    .as("the consumer's own failures must not be the dependency's typo")
                    .noneMatch(f -> f.startsWith("Consumer#") && f.contains("UndeclaredDependencyException:"));
            assertThat(state(e, "Consumer")).as("it keeps retrying rather than dying of someone else's typo")
                    .isNotEqualTo("Dead");
        } finally {
            SUPPLIER_TYPO.set(false);
            e.close();
        }
    }

    static final java.util.concurrent.atomic.AtomicBoolean SUPPLIER_TYPO =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** Its re-init, once {@link #SUPPLIER_TYPO} is set, queries a dependency it does not declare. */
    static final class TypoSupplier implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            if (!SUPPLIER_TYPO.get()) return CompletableFuture.completedFuture(Map.of());
            // Slow enough that the consumer's query is already waiting when the typo lands.
            return CompletableFuture.runAsync(() -> sleepQuietly(400))
                    .thenCompose(ignored -> ctx.query("Typo", "q"))
                    .thenApply(answer -> Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("S-ok"));
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Queries a dependency by name, from its init or from its load. */
    record AskingNode(String dependency, boolean fromLoad) implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            if (fromLoad) return CompletableFuture.completedFuture(Map.of());
            return ctx.query(dependency, "q").thenApply(answer -> Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            if (!fromLoad) {
                return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
            }
            return ctx.query(dependency, "q")
                    .thenApply(answer -> (c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    /**
     * A node that will not start must strand only what depends on it. It used to abort the swap's
     * spawn loop, leaving every later node unstarted for good: a retry ran into the same broken
     * node first and aborted again, so they never came up at all.
     */
    @Test
    @Timeout(60)
    void a_node_that_fails_to_start_does_not_strand_the_nodes_after_it() throws Exception {
        var e = new Engine(cfg(Duration.ofMillis(600), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var first = new GraphBuilder();
            add(first, "Keep");
            e.newGraph(first.build());

            Supplier<Graph> withBroken = () -> {
                var b = new GraphBuilder();
                add(b, "Keep");
                b.add("Broken", (Supplier<ProcessInitializer>) FailingInit::new,
                        (Supplier<ProcessLoader>) FailingInit::new);
                add(b, "Later"); // independent of Broken, ordered after it
                return b.build();
            };
            assertThatThrownBy(() -> e.newGraph(withBroken.get())).isInstanceOf(RuntimeException.class);

            // Broken is the only casualty: a node that does not depend on it must be serving,
            // whatever its position in the topological order.
            await().atMost(Duration.ofSeconds(10)).until(() -> "Serving".equals(state(e, "Later")));
            assertThat(state(e, "Broken")).isEqualTo("Dead");
            assertThat(e.queryProcess("Later", "q", Duration.ofSeconds(5)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)).isEqualTo("Later-ok");
        } finally {
            e.close();
        }
    }

    /** Its init never succeeds. */
    static final class FailingInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.failedFuture(new IllegalStateException("init always fails (injected)"));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("never"));
        }
    }

    /** Several broken nodes in one swap cost one start budget, not one each, in sequence. */
    @Test
    @Timeout(60)
    void several_broken_nodes_in_a_swap_cost_one_start_budget_not_one_each() throws Exception {
        var e = new Engine(cfg(Duration.ofMillis(800), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var first = new GraphBuilder();
            add(first, "Keep");
            e.newGraph(first.build());

            var b = new GraphBuilder();
            add(b, "Keep");
            for (int i = 1; i <= 4; i++) {
                b.add("Broken" + i, (Supplier<ProcessInitializer>) FailingInit::new,
                        (Supplier<ProcessLoader>) FailingInit::new);
            }
            add(b, "Dependent", "Broken1"); // must stay down: its dependency never serves
            add(b, "Fine");
            long startedAt = System.nanoTime();
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);
            Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

            // The start budget here is 800 ms init + 5 s load; four broken nodes one after another
            // would cost at least 4 x 800 ms of init retries before the swap could even finish.
            assertThat(took).as("the broken nodes fail side by side").isLessThan(Duration.ofMillis(2_500));
            assertThat(state(e, "Fine")).isEqualTo("Serving");
            assertThat(state(e, "Dependent")).as("a node whose dependency never served does not start")
                    .isNotEqualTo("Serving");
        } finally {
            e.close();
        }
    }

    /** Closing the engine while a swap starts its nodes must not start any further one. */
    @Test
    @Timeout(60)
    void closing_during_a_swap_starts_nothing_more() throws Exception {
        INIT_DELAY.put("Slow", 3_000L);
        var e = new Engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        var first = new GraphBuilder();
        add(first, "Keep");
        e.newGraph(first.build());

        var b = new GraphBuilder();
        add(b, "Keep");
        add(b, "Slow");
        add(b, "AfterSlow", "Slow"); // cannot start before Slow serves
        var swapping = CompletableFuture.runAsync(() -> e.newGraph(b.build()));
        await().atMost(Duration.ofSeconds(5)).until(() -> inits("Slow") > 0);

        e.close();

        swapping.handle((v, t) -> null).get(10, TimeUnit.SECONDS);
        Thread.sleep(3_500); // longer than Slow's init: had anything waited on it, it would run now
        assertThat(inits("AfterSlow")).as("nothing is started on a closing engine").isZero();
    }

    /** A cleanUp that blocks before returning its stage must not hold a re-init past its budget. */
    @Test
    @Timeout(30)
    void a_blocking_cleanup_does_not_hold_a_reinit_past_its_budget() throws Exception {
        var b = new GraphBuilder();
        b.add("C", (Supplier<ProcessInitializer>) () -> new SyncCleanup(),
                (Supplier<ProcessLoader>) () -> new SyncCleanup()); // its cleanUp blocks 1.5 s inline
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofMillis(300)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            e.newGraph(b.build());
            long startedAt = System.nanoTime();
            e.trigger("C", "again");

            // A query sent during the re-init is answered by the new generation — without waiting
            // out the old generation's 1.5 s cleanUp, only its 300 ms budget.
            assertThat(e.queryProcess("C", "q", Duration.ofSeconds(5)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)).isEqualTo("C-ok");
            await().atMost(Duration.ofSeconds(5)).until(() -> "Serving".equals(state(e, "C")));
            assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                    .as("the re-init moved on at the cleanup budget").isLessThan(Duration.ofMillis(1_200));
        } finally {
            e.close();
        }
    }

    /** close() on an interrupted thread still stops consumers before their dependencies. */
    @Test
    @Timeout(30)
    void close_on_an_interrupted_thread_still_waits_and_keeps_the_flag() throws Exception {
        var b = new GraphBuilder();
        b.add("Slow", (Supplier<ProcessInitializer>) () -> new SlowCleanup("Slow"),
                (Supplier<ProcessLoader>) () -> new SlowCleanup("Slow"));
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(2)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        CLEANUP_MILLIS.set(400);
        e.newGraph(b.build());

        var stopped = new java.util.concurrent.atomic.AtomicLong();
        var flagKept = new java.util.concurrent.atomic.AtomicBoolean();
        var closer = Thread.ofVirtual().start(() -> {
            Thread.currentThread().interrupt();
            long t0 = System.nanoTime();
            e.close();
            stopped.set(System.nanoTime() - t0);
            flagKept.set(Thread.currentThread().isInterrupted());
        });
        closer.join(10_000);

        assertThat(Duration.ofNanos(stopped.get()))
                .as("close() waited for the 400 ms cleanUp instead of giving up at once")
                .isGreaterThanOrEqualTo(Duration.ofMillis(350));
        assertThat(flagKept).as("and the caller's interrupt is still set afterwards").isTrue();
    }

    /**
     * A dependency's typo can also arrive as the plain answer of its {@code compute}, with no
     * rejection around it. It is still that dependency's typo: the consumer asking must retry, not
     * die of it — it used to go Dead on its first attempt, quoting the dependency's declared list.
     */
    @Test
    @Timeout(60)
    void a_consumer_is_not_blamed_for_a_typo_in_its_dependencys_compute() throws Exception {
        var failures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                failures.add(processName + "#" + attempt + ": " + cause);
            }
        };
        var b = new GraphBuilder();
        b.add("D", (Supplier<ProcessInitializer>) TypoInCompute::new, (Supplier<ProcessLoader>) TypoInCompute::new);
        b.add("C", (Supplier<ProcessInitializer>) () -> new AskingNode("D", false),
                (Supplier<ProcessLoader>) () -> new AskingNode("D", false), "D");
        var e = new Engine(cfg(Duration.ofSeconds(2), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), observer);
        try {
            assertThatThrownBy(() -> e.newGraph(b.build()))
                    .as("C fails its start, but not with a permanent verdict about D's typo")
                    .isNotInstanceOf(io.fom.api.UndeclaredDependencyException.class);

            assertThat(failures.stream().filter(f -> f.startsWith("C#")).count())
                    .as("C retried: D's typo is not C's").isGreaterThan(1);
            assertThat(state(e, "D")).isEqualTo("Serving");
        } finally {
            e.close();
        }
    }

    /** Serves, but its compute asks for a dependency it does not declare. */
    static final class TypoInCompute implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> c.query("Typo", q));
        }
    }

    /** The log must say which failed dependency kept a node from starting, not leave it "Starting". */
    @Test
    @Timeout(60)
    void the_log_names_the_dependency_that_kept_a_node_down() throws Exception {
        var captured = new java.io.ByteArrayOutputStream();
        var originalErr = System.err;
        var e = new Engine(cfg(Duration.ofMillis(600), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var first = new GraphBuilder();
            add(first, "Keep");
            e.newGraph(first.build());

            var b = new GraphBuilder();
            add(b, "Keep");
            b.add("Broken", (Supplier<ProcessInitializer>) FailingInit::new,
                    (Supplier<ProcessLoader>) FailingInit::new);
            add(b, "D1", "Broken");
            add(b, "D2", "D1");
            System.setErr(new java.io.PrintStream(captured, true, java.nio.charset.StandardCharsets.UTF_8));
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);
        } finally {
            System.setErr(originalErr);
            e.close();
        }
        String log = captured.toString(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(log).contains("not starting D1: its dependency Broken did not start");
        assertThat(log).contains("not starting D2: its dependency D1 did not start");
    }

    /**
     * A trigger for a node that has not started yet asks for a fresh init. If that fresh start then
     * fails, the request must survive it: a retried newGraph used to warm-load the very state the
     * trigger meant to replace, because the "must cold-init" flag was consumed by the failed start.
     */
    @Test
    @Timeout(60)
    void a_trigger_before_a_start_survives_that_start_failing() throws Exception {
        var backend = new InMemoryLogBackend();
        java.util.function.Supplier<Graph> graph = () -> {
            var b = new GraphBuilder();
            add(b, "B");
            b.add("D", (Supplier<ProcessInitializer>) SourceNode::new, (Supplier<ProcessLoader>) SourceNode::new, "B");
            return b.build();
        };
        SOURCE.set("v1");
        SOURCE_INIT_FAILS.set(false);
        var first = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        first.newGraph(graph.get());
        first.close();

        // A restart on the same log: B loads slowly, so D is still waiting for it when a trigger
        // for D arrives. D's fresh init then fails for the whole first start.
        SOURCE.set("v2");
        SOURCE_INIT_FAILS.set(true);
        LOAD_DELAY.put("B", 1_500L);
        // RELEASE_FIRST: a KEEP_OLD node warm-loads and re-inits in the background, so its start
        // does not fail (see KeepOldReinitTest for that order).
        var e = new Engine(cfg(Duration.ofMillis(800), Duration.ofSeconds(1))
                .withReinitStrategy(ReinitStrategy.RELEASE_FIRST), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var starting = CompletableFuture.runAsync(() -> e.newGraph(graph.get()));
            await().atMost(Duration.ofSeconds(5)).until(() -> e.currentGraph() != null);
            Thread.sleep(300); // B is loading; D has no FSM yet
            e.trigger("D", "fresh please");
            starting.handle((v, t) -> null).get(20, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(10)).until(() -> "Dead".equals(state(e, "D")));

            SOURCE_INIT_FAILS.set(false);
            LOAD_DELAY.remove("B");
            try {
                e.newGraph(graph.get()); // the retry
            } catch (RuntimeException ignored) {
                // the point is what D serves afterwards
            }
            await().atMost(Duration.ofSeconds(10)).until(() -> "Serving".equals(state(e, "D")));
            assertThat(e.queryProcess("D", "q", Duration.ofSeconds(5)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS))
                    .as("the trigger asked for a fresh init; the old state must not come back")
                    .isEqualTo("v2");
        } finally {
            SOURCE_INIT_FAILS.set(false);
            e.close();
        }
    }

    static final java.util.concurrent.atomic.AtomicReference<String> SOURCE =
            new java.util.concurrent.atomic.AtomicReference<>("v1");
    static final java.util.concurrent.atomic.AtomicBoolean SOURCE_INIT_FAILS =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** Its init snapshots {@link #SOURCE}; it serves what it snapshotted. */
    static final class SourceNode implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            if (SOURCE_INIT_FAILS.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("source unavailable (injected)"));
            }
            return CompletableFuture.completedFuture(
                    Map.of("v", SOURCE.get().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            String value = new String(props.get("v"), java.nio.charset.StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(value));
        }
    }

    /**
     * After a restart whose first start fails, a retried install must still rebuild the reactive
     * consumers of a producer that changed: they were built on state the producer has replaced. The
     * decision used to live only in the failed install's snapshot map, so the retry warm-loaded them.
     */
    @Test
    @Timeout(60)
    void a_retry_after_a_failed_restart_still_rebuilds_stale_consumers() throws Exception {
        var backend = new InMemoryLogBackend();
        java.util.function.Function<String, Graph> graph = version -> new GraphBuilder()
                .addWithParam("P",
                        (Supplier<io.fom.api.ParamProcessInitializer<Param>>) FlakyParamNode::new,
                        (Supplier<io.fom.api.ParamProcessLoader<Param>>) FlakyParamNode::new,
                        new Param(version))
                .add("C", (Supplier<ProcessInitializer>) () -> new EchoConsumer("P"),
                        (Supplier<ProcessLoader>) () -> new EchoConsumer("P"), "P")
                .build();
        FLAKY_PARAM_FAILS.set(false);
        var first = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        first.newGraph(graph.apply("v1"));
        first.close();

        FLAKY_PARAM_FAILS.set(true); // P's definition changed, and its fresh init fails at first
        var e = new Engine(cfg(Duration.ofMillis(800), Duration.ofSeconds(1)), backend,
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            assertThatThrownBy(() -> e.newGraph(graph.apply("v2"))).isInstanceOf(RuntimeException.class);

            FLAKY_PARAM_FAILS.set(false);
            e.newGraph(graph.apply("v2")); // the retry

            // KEEP_OLD: C serves what it built on the old P at once and is rebuilt in the background.
            await().atMost(Duration.ofSeconds(10)).ignoreExceptions().untilAsserted(() ->
                    assertThat(e.queryProcess("C", "q", Duration.ofSeconds(5)).toCompletableFuture()
                            .get(5, TimeUnit.SECONDS))
                            .as("C must be rebuilt on the new P, not keep what it built on the old one")
                            .isEqualTo("C<-v2"));
        } finally {
            FLAKY_PARAM_FAILS.set(false);
            e.close();
        }
    }

    static final java.util.concurrent.atomic.AtomicBoolean FLAKY_PARAM_FAILS =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** Serves its param; its init fails while {@link #FLAKY_PARAM_FAILS} is set. */
    static final class FlakyParamNode implements io.fom.api.ParamProcessInitializer<Param>,
            io.fom.api.ParamProcessLoader<Param> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Param param) {
            if (FLAKY_PARAM_FAILS.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("P unavailable (injected)"));
            }
            return CompletableFuture.completedFuture(
                    Map.of("v", param.value().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, Param param) {
            String value = new String(props.get("v"), java.nio.charset.StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(value));
        }
    }

    /** Its init records what its dependency answered; it serves "C<-" + that. */
    record EchoConsumer(String dependency) implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return ctx.query(dependency, "q").thenApply(answer ->
                    Map.of("v", ("C<-" + answer).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            String value = new String(props.get("v"), java.nio.charset.StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(value));
        }
    }

    /** Every onLoadStarted gets a terminal callback, also when the load is cancelled by a removal. */
    @Test
    @Timeout(30)
    void a_load_cancelled_by_a_removal_is_reported_to_the_observer() throws Exception {
        var events = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onLoadStarted(String processName, Sid sid, int attempt) {
                events.add("started " + processName);
            }

            @Override
            public void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) {
                events.add("failed " + processName + ": " + cause.getClass().getSimpleName());
            }
        };
        LOAD_DELAY.put("Slow", 10_000L);
        var b = new GraphBuilder();
        add(b, "Keep");
        add(b, "Slow");
        var e = new Engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), observer);
        var starting = CompletableFuture.runAsync(() -> e.newGraph(b.build()));
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> events.contains("started Slow"));
            e.remove(List.of("Slow"));

            await().atMost(Duration.ofSeconds(5)).until(() -> events.stream().anyMatch(ev -> ev.startsWith("failed Slow")));
            assertThat(events).contains("failed Slow: AttemptCancelledException"); // an engine stop, not a load failure
        } finally {
            starting.handle((v, t) -> null).join();
            e.close();
        }
    }

    /** A blocked common ForkJoinPool must not stop query deadlines from firing. */
    @Test
    @Timeout(60)
    void query_deadlines_fire_while_the_common_pool_is_blocked() throws Exception {
        var b = new GraphBuilder();
        b.add("Never", (Supplier<ProcessInitializer>) NeverAnswers::new, (Supplier<ProcessLoader>) NeverAnswers::new);
        var e = new Engine(cfg(Duration.ofSeconds(5), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        var release = new java.util.concurrent.CountDownLatch(1);
        try {
            // With a parallelism of 1 the JDK runs async tasks on a thread each: nothing to block.
            org.junit.jupiter.api.Assumptions.assumeTrue(
                    java.util.concurrent.ForkJoinPool.getCommonPoolParallelism() > 1,
                    "the common pool is not used on this machine");
            e.newGraph(b.build());
            // Occupy every common-pool worker, the way a compute doing supplyAsync(blockingCall) would.
            int workers = java.util.concurrent.ForkJoinPool.commonPool().getParallelism() + 2;
            for (int i = 0; i < workers; i++) {
                CompletableFuture.runAsync(() -> {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            long startedAt = System.nanoTime();
            assertThatThrownBy(() -> e.queryProcess("Never", "q", Duration.ofMillis(300)).toCompletableFuture()
                    .get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                    .as("the 300 ms deadline fired on time").isLessThan(Duration.ofSeconds(2));
        } finally {
            release.countDown();
            e.close();
        }
    }

    /** Its compute never completes. */
    static final class NeverAnswers implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> new CompletableFuture<>());
        }
    }

    /** An init that keeps failing goes Dead within its budget, not one backoff after it. */
    @Test
    @Timeout(30)
    void a_failing_init_is_given_up_within_its_budget_not_a_backoff_later() throws Exception {
        var b = new GraphBuilder();
        b.add("Broken", (Supplier<ProcessInitializer>) FailingInit::new, (Supplier<ProcessLoader>) FailingInit::new);
        // A 1 s budget and a 6 s backoff (3-9 s after jitter): an uncapped retry lands at least two
        // seconds past the budget, every time.
        var config = cfg(Duration.ofSeconds(1), Duration.ofSeconds(1))
                .withBackoff(Duration.ofSeconds(6), Duration.ofSeconds(6));
        var e = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            long startedAt = System.nanoTime();
            assertThatThrownBy(() -> e.newGraph(b.build())).isInstanceOf(RuntimeException.class);
            await().atMost(Duration.ofSeconds(10)).until(() -> "Dead".equals(state(e, "Broken")));
            assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                    .as("no retry was scheduled past the budget").isLessThan(Duration.ofMillis(2_500));
        } finally {
            e.close();
        }
    }

    /**
     * A graph change on an interrupted thread honours the interrupt without leaving the graph half
     * started in silence: what was already starting finishes, what was not is named in the
     * exception, the flag is kept, and a retry starts the rest. (It used to leave such nodes in
     * "Starting" for good, with no report at all.)
     */
    @Test
    @Timeout(60)
    void a_swap_on_an_interrupted_thread_names_what_it_left_and_a_retry_starts_it() throws Exception {
        INIT_DELAY.put("N1", 600L);
        var e = new Engine(cfg(Duration.ofSeconds(10), Duration.ofSeconds(1)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new EngineObserver() { });
        try {
            var first = new GraphBuilder();
            add(first, "Keep");
            e.newGraph(first.build());

            var b = new GraphBuilder();
            add(b, "Keep");
            add(b, "N1");
            add(b, "N2", "N1"); // new, and waits for another new node
            var flagKept = new java.util.concurrent.atomic.AtomicBoolean();
            var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            var swapper = Thread.ofVirtual().start(() -> {
                Thread.currentThread().interrupt();
                try {
                    e.newGraph(b.build());
                } catch (RuntimeException expected) {
                    failure.set(expected);
                }
                flagKept.set(Thread.currentThread().isInterrupted());
            });
            swapper.join(15_000);

            assertThat(failure.get()).as("the interrupt is reported, with what was left unstarted")
                    .hasMessageContaining("Interrupted while starting the graph")
                    .hasMessageContaining("N2");
            assertThat(flagKept).as("the caller's interrupt is still set afterwards").isTrue();

            e.newGraph(b.build()); // the retry the message asks for
            assertThat(state(e, "N2")).isEqualTo("Serving");
        } finally {
            e.close();
        }
    }
}
