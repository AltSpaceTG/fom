package io.fom;

import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogDead;
import io.fom.log.LogEvent;
import io.fom.serde.JavaSerializableSerDe;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stage 7 — in-place graph swap (TZ §6.4): {@code Engine.newGraph(next)}
 * computes a diff vs the running graph and only re-initialises nodes whose
 * structural identity changed.
 */
class GraphSwapTest {

    @BeforeEach
    void reset() {
        Counters.reset();
    }

    private EngineConfig fastConfig() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofMillis(100), Duration.ofMillis(100),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    void diff_identifies_added_removed_changed_unchanged() {
        Graph prev = new GraphBuilder()
                .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                        (SerializableSupplier<ProcessLoader>) AInit::new)
                .add("B", (SerializableSupplier<ProcessInitializer>) BInit::new,
                        (SerializableSupplier<ProcessLoader>) BInit::new, "A")
                .build();
        Graph next = new GraphBuilder()
                .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                        (SerializableSupplier<ProcessLoader>) AInit::new)
                .addWithParam("B",
                        (SerializableSupplier<io.fom.api.ParamProcessInitializer<TenantParam>>) ParamBInit::new,
                        (SerializableSupplier<io.fom.api.ParamProcessLoader<TenantParam>>) ParamBInit::new,
                        new TenantParam("v2"), "A")
                .add("C", (SerializableSupplier<ProcessInitializer>) AInit::new,
                        (SerializableSupplier<ProcessLoader>) AInit::new, "A")
                .build();

        var diff = GraphDiff.compute(prev, next);
        assertThat(diff.added()).containsExactly("C");
        assertThat(diff.removed()).isEmpty();
        assertThat(diff.changed()).containsExactly("B"); // param differs (null → TenantParam("v2"))
        assertThat(diff.unchanged()).containsExactly("A");
        assertThat(diff.hasChanges()).isTrue();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void unchanged_node_keeps_its_sid_across_newGraph() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend();
             var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
            Graph g1 = new GraphBuilder()
                    .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                            (SerializableSupplier<ProcessLoader>) AInit::new)
                        .handles(String.class)
                    .build();
            engine.newGraph(g1);

            int initBefore = Counters.A_INIT.get();
            var sidBefore = engine.introspect().toCompletableFuture().get()
                    .graph().nodes().get(0).sid();

            // Identical graph (same factories, no deps, null param) → diff is empty.
            Graph g2 = new GraphBuilder()
                    .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                            (SerializableSupplier<ProcessLoader>) AInit::new)
                        .handles(String.class)
                    .build();
            boolean changed = engine.newGraph(g2);

            assertThat(changed).isFalse();
            assertThat(Counters.A_INIT).hasValue(initBefore);
            var sidAfter = engine.introspect().toCompletableFuture().get()
                    .graph().nodes().get(0).sid();
            assertThat(sidAfter).isEqualTo(sidBefore);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void changed_node_gets_new_sid_and_writes_log_dead() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend();
             var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
            Graph g1 = new GraphBuilder()
                    .addWithParam("A",
                            (SerializableSupplier<io.fom.api.ParamProcessInitializer<TenantParam>>) ParamAInit::new,
                            (SerializableSupplier<io.fom.api.ParamProcessLoader<TenantParam>>) ParamAInit::new,
                            new TenantParam("v1"))
                        .handles(String.class)
                    .build();
            engine.newGraph(g1);

            int initBefore = Counters.PARAM_A_INIT.get();
            var sidBefore = engine.introspect().toCompletableFuture().get()
                    .graph().nodes().get(0).sid();

            // Same name + factory, different param → changed node.
            Graph g2 = new GraphBuilder()
                    .addWithParam("A",
                            (SerializableSupplier<io.fom.api.ParamProcessInitializer<TenantParam>>) ParamAInit::new,
                            (SerializableSupplier<io.fom.api.ParamProcessLoader<TenantParam>>) ParamAInit::new,
                            new TenantParam("v2"))
                        .handles(String.class)
                    .build();
            boolean changed = engine.newGraph(g2);

            assertThat(changed).isTrue();
            assertThat(Counters.PARAM_A_INIT.get()).isEqualTo(initBefore + 1);
            var sidAfter = engine.introspect().toCompletableFuture().get()
                    .graph().nodes().get(0).sid();
            assertThat(sidAfter).isNotEqualTo(sidBefore);

            // Log should contain LogDead for the prior Sid.
            LogEvent[] events = backend.getBetween(0, backend.length());
            boolean hasDead = false;
            for (LogEvent e : events) {
                if (e instanceof LogDead d && d.sid().equals(sidBefore)) {
                    hasDead = true;
                    break;
                }
            }
            assertThat(hasDead).as("LogDead should be written for retired Sid").isTrue();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void reactive_consumer_cascades_when_dep_replaced() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend();
             var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
            Graph g1 = new GraphBuilder()
                    .addWithParam("A",
                            (SerializableSupplier<io.fom.api.ParamProcessInitializer<TenantParam>>) ParamAInit::new,
                            (SerializableSupplier<io.fom.api.ParamProcessLoader<TenantParam>>) ParamAInit::new,
                            new TenantParam("v1"))
                    .add("B", (SerializableSupplier<ProcessInitializer>) BInit::new,
                            (SerializableSupplier<ProcessLoader>) BInit::new, "A")
                        .handles(String.class)
                    .build();
            engine.newGraph(g1);

            int bInitBefore = Counters.B_INIT.get();

            // Replace A (change param). B is reactive consumer → should re-init via cascade.
            Graph g2 = new GraphBuilder()
                    .addWithParam("A",
                            (SerializableSupplier<io.fom.api.ParamProcessInitializer<TenantParam>>) ParamAInit::new,
                            (SerializableSupplier<io.fom.api.ParamProcessLoader<TenantParam>>) ParamAInit::new,
                            new TenantParam("v2"))
                    .add("B", (SerializableSupplier<ProcessInitializer>) BInit::new,
                            (SerializableSupplier<ProcessLoader>) BInit::new, "A")
                        .handles(String.class)
                    .build();
            engine.newGraph(g2);

            Awaitility.await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(Counters.B_INIT.get()).isEqualTo(bInitBefore + 1));
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void added_node_is_spawned_and_serves_queries() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend();
             var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
            Graph g1 = new GraphBuilder()
                    .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                            (SerializableSupplier<ProcessLoader>) AInit::new)
                        .handles(String.class)
                    .build();
            engine.newGraph(g1);

            // Add a brand-new node B that depends on A.
            Graph g2 = new GraphBuilder()
                    .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                            (SerializableSupplier<ProcessLoader>) AInit::new)
                    .add("B", (SerializableSupplier<ProcessInitializer>) BInit::new,
                            (SerializableSupplier<ProcessLoader>) BInit::new, "A")
                        .handles(String.class)
                    .build();
            assertThat(engine.newGraph(g2)).isTrue();

            Object r = engine.queryProcess("B", "ping").toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertThat(r).isEqualTo("B-served:ping");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void removed_node_is_shutdown_and_unreachable() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend();
             var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
            Graph g1 = new GraphBuilder()
                    .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                            (SerializableSupplier<ProcessLoader>) AInit::new)
                    .add("B", (SerializableSupplier<ProcessInitializer>) BInit::new,
                            (SerializableSupplier<ProcessLoader>) BInit::new)
                        .handles(String.class)
                    .build();
            engine.newGraph(g1);

            // Drop B.
            Graph g2 = new GraphBuilder()
                    .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                            (SerializableSupplier<ProcessLoader>) AInit::new)
                        .handles(String.class)
                    .build();
            engine.newGraph(g2);

            var report = engine.introspect().toCompletableFuture().get();
            assertThat(report.graph().nodes()).hasSize(1);
            assertThat(report.graph().nodes().get(0).name()).isEqualTo("A");
        }
    }

    // ───────────────── fixtures ─────────────────

    record TenantParam(String version) implements Serializable { }

    static final class Counters {

        static final AtomicInteger A_INIT = new AtomicInteger();
        static final AtomicInteger B_INIT = new AtomicInteger();
        static final AtomicInteger PARAM_A_INIT = new AtomicInteger();

        static void reset() {
            A_INIT.set(0);
            B_INIT.set(0);
            PARAM_A_INIT.set(0);
        }
    }

    static final class AInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            Counters.A_INIT.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("v", new byte[]{1}));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("A-served:" + q));
        }
    }

    static final class BInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            Counters.B_INIT.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("v", new byte[]{2}));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("B-served:" + q));
        }
    }

    static final class ParamAInit implements
            io.fom.api.ParamProcessInitializer<TenantParam>,
            io.fom.api.ParamProcessLoader<TenantParam> {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, TenantParam param) {
            Counters.PARAM_A_INIT.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("v", param.version().getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p, TenantParam param) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("A(" + param.version() + "):" + q));
        }
    }

    static final class ParamBInit implements
            io.fom.api.ParamProcessInitializer<TenantParam>,
            io.fom.api.ParamProcessLoader<TenantParam> {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, TenantParam param) {
            return CompletableFuture.completedFuture(Map.of("v", param.version().getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p, TenantParam param) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("B(" + param.version() + "):" + q));
        }
    }
}
