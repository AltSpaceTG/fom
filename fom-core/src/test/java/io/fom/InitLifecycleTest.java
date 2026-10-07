package io.fom;

import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Init/load lifecycle edge cases: failing loads, cancellation, pauses and swaps
 * racing a re-init, consumers of a producer that falls back to a fresh init,
 * closing during startup and concurrent startup of independent nodes.
 */
class InitLifecycleTest {

    private static EngineConfig cfg(Duration initTimeout, Duration loadTimeout) {
        return new EngineConfig(
                initTimeout, loadTimeout, Duration.ofSeconds(1),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(20), Duration.ofMillis(200), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    private static EngineConfig cfg() {
        return cfg(Duration.ofSeconds(5), Duration.ofSeconds(5));
    }

    private static Engine engine(EngineConfig cfg, LogBackend backend) {
        return new Engine(cfg, backend, new JavaSerializableSerDe());
    }

    private static Object ask(Engine e, String process) throws Exception {
        return e.queryProcess(process, "q").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    /** Behaviour of a test process, looked up by name so factories stay trivial. */
    interface Behaviour {
        CompletionStage<Map<String, byte[]>> init(String name, int call, QueryableContext ctx);

        default CompletionStage<Process> load(String name, Map<String, byte[]> props) {
            String v = new String(props.getOrDefault("v", new byte[0]), StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(v));
        }
    }

    static final Map<String, Behaviour> BEHAVIOURS = new ConcurrentHashMap<>();
    static final Map<String, AtomicInteger> INIT_CALLS = new ConcurrentHashMap<>();

    static final class Node implements ProcessInitializer, ProcessLoader {
        private final String name;

        Node(String name) {
            this.name = name;
        }

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            int call = INIT_CALLS.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            return BEHAVIOURS.get(name).init(name, call, ctx);
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return BEHAVIOURS.get(name).load(name, props);
        }
    }

    private static GraphBuilder add(GraphBuilder b, String name, String... deps) {
        return b.add(name, (Supplier<ProcessInitializer>) () -> new Node(name),
                (Supplier<ProcessLoader>) () -> new Node(name), deps);
    }

    private static CompletableFuture<Map<String, byte[]>> value(String v) {
        return CompletableFuture.completedFuture(Map.of("v", v.getBytes(StandardCharsets.UTF_8)));
    }

    private static int inits(String name) {
        return INIT_CALLS.getOrDefault(name, new AtomicInteger()).get();
    }

    private static void reset() {
        BEHAVIOURS.clear();
        INIT_CALLS.clear();
    }

    // ───────────────── a load that never works ─────────────────

    @Test
    @Timeout(30)
    void a_load_that_always_fails_ends_dead_within_the_init_budget_instead_of_looping() throws Exception {
        reset();
        BEHAVIOURS.put("B", new Behaviour() {
            @Override
            public CompletionStage<Map<String, byte[]>> init(String name, int call, QueryableContext ctx) {
                return value("b" + call);
            }

            @Override
            public CompletionStage<Process> load(String name, Map<String, byte[]> props) {
                return CompletableFuture.failedFuture(new IllegalStateException("load is broken"));
            }
        });
        var backend = new InMemoryLogBackend();
        try (var e = engine(cfg(Duration.ofSeconds(1), Duration.ofSeconds(1)), backend)) {
            assertThatThrownBy(() -> e.newGraph(add(new GraphBuilder(), "B").build()));
            Thread.sleep(1_500);
            int length = backend.length();
            int initsSoFar = inits("B");
            Thread.sleep(1_000);
            assertThat(backend.length()).as("the log stops growing").isEqualTo(length);
            assertThat(inits("B")).as("no more inits").isEqualTo(initsSoFar).isLessThan(20);
        }
    }

    // ───────────────── cancelInit ─────────────────

    @Test
    @Timeout(30)
    void cancel_init_by_name_cancels_a_cold_init_and_a_reinit_and_trigger_restarts_a_dead_process() throws Exception {
        reset();
        List<CompletableFuture<Map<String, byte[]>>> hanging = new CopyOnWriteArrayList<>();
        AtomicBoolean hang = new AtomicBoolean(true);
        BEHAVIOURS.put("A", (name, call, ctx) -> {
            if (!hang.get()) return value("a" + call);
            var never = new CompletableFuture<Map<String, byte[]>>();
            hanging.add(never);
            return never;
        });
        try (var e = engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(5)), new InMemoryLogBackend())) {
            Thread installer = Thread.ofVirtual().start(() -> {
                try {
                    e.newGraph(add(new GraphBuilder(), "A").build());
                } catch (RuntimeException expected) {
                    // cancelled before serving
                }
            });
            await().atMost(Duration.ofSeconds(5)).until(() -> hanging.size() == 1);
            e.cancelInit("A").toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(hanging.get(0).isCancelled()).isTrue();
            installer.join(Duration.ofSeconds(5));
            assertThat(installer.isAlive()).isFalse();

            // A Dead process starts again on trigger.
            hang.set(false);
            assertThat(e.trigger("A", "restart")).isTrue();
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> ask(e, "A").toString().startsWith("a"));

            // A re-init has no Sid either: cancel it by name.
            hang.set(true);
            e.trigger("A", "reinit");
            await().atMost(Duration.ofSeconds(5)).until(() -> hanging.size() == 2);
            e.cancelInit("A").toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(hanging.get(1).isCancelled()).isTrue();
        }
    }

    // ───────────────── pause, remove and close during init ─────────────────

    @Test
    @Timeout(30)
    void pausing_mid_reinit_cancels_that_init_and_resume_inits_once() throws Exception {
        reset();
        List<CompletableFuture<Map<String, byte[]>>> slow = new CopyOnWriteArrayList<>();
        BEHAVIOURS.put("A", (name, call, ctx) -> {
            if (call != 2) return value("a" + call);
            var pending = new CompletableFuture<Map<String, byte[]>>();
            slow.add(pending);
            return pending;
        });
        try (var e = engine(cfg(), new InMemoryLogBackend())) {
            e.newGraph(add(new GraphBuilder(), "A").build());
            e.trigger("A", "reinit");
            await().atMost(Duration.ofSeconds(5)).until(() -> slow.size() == 1);

            e.pause(List.of("A"));
            assertThat(slow.get(0).isCancelled()).as("the abandoned init is cancelled").isTrue();

            e.resume(List.of("A"));
            // KEEP_OLD: the old state serves at once; the re-init the pause interrupted runs in the background.
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "a3".equals(ask(e, "A")));
            Thread.sleep(300); // a redundant second re-init would start within the dedup window
            assertThat(inits("A")).as("resume initialises once").isEqualTo(3);
            assertThat(ask(e, "A")).isEqualTo("a3");
        }
    }

    @Test
    @Timeout(30)
    void removing_a_process_mid_init_cancels_its_init() throws Exception {
        reset();
        List<CompletableFuture<Map<String, byte[]>>> slow = new CopyOnWriteArrayList<>();
        BEHAVIOURS.put("A", (name, call, ctx) -> {
            if (call == 1) return value("a1");
            var pending = new CompletableFuture<Map<String, byte[]>>();
            slow.add(pending);
            return pending;
        });
        BEHAVIOURS.put("Other", (name, call, ctx) -> value("o"));
        try (var e = engine(cfg(), new InMemoryLogBackend())) {
            e.newGraph(add(add(new GraphBuilder(), "A"), "Other").build());
            e.trigger("A", "reinit");
            await().atMost(Duration.ofSeconds(5)).until(() -> slow.size() == 1);
            assertThat(e.remove(List.of("A"))).isTrue();
            await().atMost(Duration.ofSeconds(3)).until(() -> slow.get(0).isCancelled());
        }
    }

    @Test
    @Timeout(30)
    void close_during_the_first_new_graph_does_not_wait_for_the_init_budget() throws Exception {
        reset();
        List<CompletableFuture<Map<String, byte[]>>> hanging = new CopyOnWriteArrayList<>();
        BEHAVIOURS.put("A", (name, call, ctx) -> {
            var never = new CompletableFuture<Map<String, byte[]>>();
            hanging.add(never);
            return never;
        });
        var e = engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(20)), new InMemoryLogBackend());
        Thread installer = Thread.ofVirtual().start(() -> {
            try {
                e.newGraph(add(new GraphBuilder(), "A").build());
            } catch (RuntimeException expected) {
                // closed while starting
            }
        });
        await().atMost(Duration.ofSeconds(5)).until(() -> hanging.size() == 1);
        long start = System.nanoTime();
        e.close();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
        installer.join(Duration.ofSeconds(5));
        assertThat(installer.isAlive()).isFalse();
        await().atMost(Duration.ofSeconds(3)).until(() -> hanging.get(0).isCancelled());
    }

    // ───────────────── cascades ─────────────────

    record Mode(String v) implements Serializable { }

    static final List<CompletableFuture<Map<String, byte[]>>> GATED = new CopyOnWriteArrayList<>();
    static final AtomicInteger PRODUCER_INITS = new AtomicInteger();

    static final class Producer implements ParamProcessInitializer<Mode>, ParamProcessLoader<Mode> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Mode mode) {
            int call = PRODUCER_INITS.incrementAndGet();
            if (call == 2) { // the triggered re-init hangs until the swap replaces the node
                var pending = new CompletableFuture<Map<String, byte[]>>();
                GATED.add(pending);
                return pending;
            }
            return value(mode.v());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, Mode mode) {
            String v = new String(props.get("v"), StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(v));
        }
    }

    private static Graph producerGraph(String mode) {
        BEHAVIOURS.put("B", (name, call, ctx) -> {
            try {
                return value("B<" + ctx.query("A", "q").toCompletableFuture().get(3, TimeUnit.SECONDS) + ">");
            } catch (Exception ex) {
                return CompletableFuture.failedFuture(ex);
            }
        });
        return add(new GraphBuilder()
                .addWithParam("A", (Supplier<ParamProcessInitializer<Mode>>) Producer::new,
                        (Supplier<ParamProcessLoader<Mode>>) Producer::new, new Mode(mode)), "B", "A")
                .build();
    }

    @Test
    @Timeout(30)
    void a_swap_that_changes_a_node_mid_reinit_still_cascades_to_its_consumers() throws Exception {
        reset();
        GATED.clear();
        PRODUCER_INITS.set(0);
        try (var e = engine(cfg(), new InMemoryLogBackend())) {
            e.newGraph(producerGraph("v1"));
            assertThat(ask(e, "B")).isEqualTo("B<v1>");
            e.trigger("A", "reinit");
            await().atMost(Duration.ofSeconds(5)).until(() -> GATED.size() == 1);

            e.newGraph(producerGraph("v2"));
            assertThat(ask(e, "A")).isEqualTo("v2");
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "B<v2>".equals(ask(e, "B")));
        }
    }

    @Test
    @Timeout(30)
    void a_producer_falling_back_from_a_failed_warm_load_re_inits_its_consumers() throws Exception {
        reset();
        AtomicBoolean breakLoadOnce = new AtomicBoolean();
        BEHAVIOURS.put("A", new Behaviour() {
            @Override
            public CompletionStage<Map<String, byte[]>> init(String name, int call, QueryableContext ctx) {
                return value("a" + call);
            }

            @Override
            public CompletionStage<Process> load(String name, Map<String, byte[]> props) {
                if (breakLoadOnce.getAndSet(false)) {
                    return CompletableFuture.failedFuture(new IllegalStateException("old schema"));
                }
                return Behaviour.super.load(name, props);
            }
        });
        BEHAVIOURS.put("C", (name, call, ctx) -> {
            try {
                return value("C<" + ctx.query("A", "q").toCompletableFuture().get(3, TimeUnit.SECONDS) + ">");
            } catch (Exception ex) {
                return CompletableFuture.failedFuture(ex);
            }
        });
        Graph g = add(add(new GraphBuilder(), "A"), "C", "A").build();
        var backend = new InMemoryLogBackend();
        try (var e = engine(cfg(), backend)) {
            e.newGraph(g);
            assertThat(ask(e, "C")).isEqualTo("C<a1>");
        }
        breakLoadOnce.set(true);
        try (var e = engine(cfg(), backend)) {
            e.newGraph(g);
            assertThat(ask(e, "A")).isEqualTo("a2");
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "C<a2>".equals(ask(e, "C")));
        }
    }

    @Test
    @Timeout(30)
    void pausing_a_producer_mid_reinit_with_its_consumer_re_inits_the_consumer_on_resume() throws Exception {
        reset();
        List<CompletableFuture<Map<String, byte[]>>> slow = new CopyOnWriteArrayList<>();
        AtomicBoolean hang = new AtomicBoolean();
        BEHAVIOURS.put("P", (name, call, ctx) -> {
            if (!hang.get()) return value("p" + call);
            var pending = new CompletableFuture<Map<String, byte[]>>();
            slow.add(pending);
            return pending;
        });
        BEHAVIOURS.put("W", (name, call, ctx) -> {
            try {
                return value("W<" + ctx.query("P", "q").toCompletableFuture().get(3, TimeUnit.SECONDS) + ">");
            } catch (Exception ex) {
                return CompletableFuture.failedFuture(ex);
            }
        });
        try (var e = engine(cfg(), new InMemoryLogBackend())) {
            e.newGraph(add(add(new GraphBuilder(), "P"), "W", "P").build());
            assertThat(ask(e, "W")).isEqualTo("W<p1>");
            hang.set(true);
            e.trigger("P", "reinit");
            await().atMost(Duration.ofSeconds(5)).until(() -> slow.size() == 1);
            e.pause(List.of("P", "W"));
            hang.set(false);
            e.resume(List.of("P", "W"));
            // KEEP_OLD: P serves its old state at once and finishes the interrupted re-init in the background.
            Settle.awaitNoReinit(e);
            String p = ask(e, "P").toString();
            assertThat(p).as("the re-init happened").isNotEqualTo("p1");
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> ("W<" + p + ">").equals(ask(e, "W")));
        }
    }

    @Test
    @Timeout(60)
    void a_trigger_racing_a_pause_is_never_lost() throws Exception {
        reset();
        BEHAVIOURS.put("A", (name, call, ctx) -> value("a" + call));
        try (var e = engine(cfg(), new InMemoryLogBackend())) { // dedupWindow 10 ms
            e.newGraph(add(new GraphBuilder(), "A").build());
            for (int i = 0; i < 120; i++) {
                int before = inits("A");
                e.trigger("A", "t" + i);
                java.util.concurrent.locks.LockSupport.parkNanos((i % 24) * 500_000L); // 0..11.5 ms
                e.pause(List.of("A"));
                e.resume(List.of("A"));
                int iteration = i;
                await().atMost(Duration.ofSeconds(5)).alias("iteration " + iteration + " re-inits")
                        .until(() -> inits("A") > before);
            }
        }
    }

    @Test
    @Timeout(30)
    void a_query_right_after_trigger_restarts_a_dead_process_waits_for_it() throws Exception {
        reset();
        AtomicBoolean hang = new AtomicBoolean(true);
        BEHAVIOURS.put("D", (name, call, ctx) -> hang.get()
                ? new CompletableFuture<>()
                : CompletableFuture.supplyAsync(() -> Map.of("v", ("d" + call).getBytes(StandardCharsets.UTF_8)),
                        CompletableFuture.delayedExecutor(200, TimeUnit.MILLISECONDS)));
        try (var e = engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(5)), new InMemoryLogBackend())) {
            Thread installer = Thread.ofVirtual().start(() -> {
                try {
                    e.newGraph(add(new GraphBuilder(), "D").build());
                } catch (RuntimeException expected) {
                    // cancelled
                }
            });
            await().atMost(Duration.ofSeconds(5)).until(() -> inits("D") == 1);
            e.cancelInit("D").toCompletableFuture().get(3, TimeUnit.SECONDS);
            installer.join(Duration.ofSeconds(5));
            hang.set(false);
            assertThat(e.trigger(Map.of("D", "restart-via-map"))).isTrue();
            assertThat(ask(e, "D").toString()).as("waits for the restart, not 'D is Dead'").startsWith("d");
        }
    }

    @Test
    @Timeout(30)
    void a_failing_node_at_startup_does_not_keep_unrelated_nodes_from_starting() throws Exception {
        reset();
        BEHAVIOURS.put("Broken", (name, call, ctx) -> CompletableFuture.failedFuture(new IllegalStateException("broken")));
        BEHAVIOURS.put("Slow", (name, call, ctx) -> CompletableFuture.supplyAsync(() -> Map.of("v", "slow".getBytes(StandardCharsets.UTF_8)),
                CompletableFuture.delayedExecutor(1_200, TimeUnit.MILLISECONDS)));
        BEHAVIOURS.put("Alpha", (name, call, ctx) -> value("alpha"));
        BEHAVIOURS.put("Beta", (name, call, ctx) -> value("beta"));
        BEHAVIOURS.put("Needy", (name, call, ctx) -> value("needy"));
        Graph g = add(add(add(add(add(new GraphBuilder(), "Broken"), "Slow"), "Alpha"), "Beta", "Alpha"), "Needy", "Broken").build();
        try (var e = engine(cfg(Duration.ofMillis(2_500), Duration.ofSeconds(2)), new InMemoryLogBackend())) {
            long start = System.nanoTime();
            Thread installer = Thread.ofVirtual().start(() -> {
                try {
                    e.newGraph(g);
                } catch (RuntimeException expected) {
                    // Broken fails
                }
            });
            // Beta depends only on Alpha: it serves while unrelated Slow is still initialising.
            await().atMost(Duration.ofSeconds(1)).ignoreExceptions().until(() -> "beta".equals(ask(e, "Beta")));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(1_100));
            installer.join(Duration.ofSeconds(10));
            assertThat(ask(e, "Slow")).isEqualTo("slow");
            var states = e.introspect().toCompletableFuture().get(3, TimeUnit.SECONDS).graph().nodes().stream()
                    .collect(java.util.stream.Collectors.toMap(EngineReport.NodeReport::name, EngineReport.NodeReport::state));
            assertThat(states).containsEntry("Broken", "Dead").containsEntry("Needy", "Starting");
        }
    }

    @Test
    @Timeout(30)
    void removing_a_process_whose_first_init_hangs_inside_new_graph_does_not_wait_for_its_budget() throws Exception {
        reset();
        BEHAVIOURS.put("A", (name, call, ctx) -> value("a"));
        BEHAVIOURS.put("Hang", (name, call, ctx) -> new CompletableFuture<>());
        try (var e = engine(cfg(Duration.ofSeconds(20), Duration.ofSeconds(5)), new InMemoryLogBackend())) {
            e.newGraph(add(new GraphBuilder(), "A").build());
            Thread swap = Thread.ofVirtual().start(() -> {
                try {
                    e.newGraph(add(add(new GraphBuilder(), "A"), "Hang").build());
                } catch (RuntimeException expected) {
                    // cancelled
                }
            });
            await().atMost(Duration.ofSeconds(5)).until(() -> inits("Hang") == 1);
            long start = System.nanoTime();
            e.remove(List.of("Hang"));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));
            swap.join(Duration.ofSeconds(5));
            assertThat(e.currentGraph().nodes()).containsOnlyKeys("A");
        }
    }

    // ───────────────── startup ─────────────────

    @Test
    @Timeout(30)
    void independent_nodes_initialise_concurrently_at_startup() {
        reset();
        GraphBuilder b = new GraphBuilder();
        for (int i = 0; i < 6; i++) {
            String name = "N" + i;
            BEHAVIOURS.put(name, (n, call, ctx) -> CompletableFuture.supplyAsync(
                    () -> Map.of("v", n.getBytes(StandardCharsets.UTF_8)),
                    CompletableFuture.delayedExecutor(400, TimeUnit.MILLISECONDS)));
            add(b, name);
        }
        try (var e = engine(cfg(), new InMemoryLogBackend())) {
            long start = System.nanoTime();
            e.newGraph(b.build());
            assertThat(Duration.ofNanos(System.nanoTime() - start))
                    .as("6 x 400 ms inits in one wave, not one after another")
                    .isLessThan(Duration.ofMillis(1_600));
        }
    }
}
