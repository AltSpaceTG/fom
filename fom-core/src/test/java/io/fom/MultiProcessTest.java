package io.fom;

import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.api.Routable;
import io.fom.api.QueryException;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Stage 2 end-to-end integration: multi-process graphs, cross-process queries,
 * topological init order, and the routing-priority chain (Routable, typeRouting,
 * queryProcess, unknown).
 */
class MultiProcessTest {

    @BeforeEach
    void reset() {
        OrderTracker.INIT_ORDER.clear();
        AInit.INIT_CALLS.set(0);
        BInit.INIT_CALLS.set(0);
        CInit.INIT_CALLS.set(0);
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
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void three_node_chain_cold_start_with_cross_process_queries() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = chainGraph();

            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);

                // engine.query routes GetC → C; C internally does ctx.query("B", GetB.INSTANCE),
                // B does ctx.query("A", GetA.INSTANCE).
                Object r = engine.query(GetC.INSTANCE).toCompletableFuture().get(3, TimeUnit.SECONDS);
                assertThat(r).isEqualTo("A→B→C");
            }

            assertThat(OrderTracker.INIT_ORDER).containsExactly("A", "B", "C");
            assertThat(AInit.INIT_CALLS).hasValue(1);
            assertThat(BInit.INIT_CALLS).hasValue(1);
            assertThat(CInit.INIT_CALLS).hasValue(1);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void three_node_chain_warm_restart_skips_all_inits() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = chainGraph();
            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                engine.query(GetC.INSTANCE).toCompletableFuture().get(3, TimeUnit.SECONDS);
            }
            int initAfterFirst = AInit.INIT_CALLS.get() + BInit.INIT_CALLS.get() + CInit.INIT_CALLS.get();

            try (var engine2 = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine2.newGraph(g);
                Object r = engine2.query(GetC.INSTANCE).toCompletableFuture().get(3, TimeUnit.SECONDS);
                assertThat(r).isEqualTo("A→B→C");
            }
            int initAfterSecond = AInit.INIT_CALLS.get() + BInit.INIT_CALLS.get() + CInit.INIT_CALLS.get();
            assertThat(initAfterSecond).isEqualTo(initAfterFirst); // no fresh init in restart
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void undeclared_dependency_query_throws() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            // C declares dependency on B; we'll make C try to query "A" directly (not declared).
            Graph g = new GraphBuilder()
                    .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                            (SerializableSupplier<ProcessLoader>) AInit::new)
                    .add("B", (SerializableSupplier<ProcessInitializer>) BInit::new,
                            (SerializableSupplier<ProcessLoader>) BInit::new, "A")
                    .add("Rogue", (SerializableSupplier<ProcessInitializer>) RogueInit::new,
                            (SerializableSupplier<ProcessLoader>) RogueInit::new, "B")
                        .handles(QueryUndeclared.class)
                    .build();

            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                var stage = engine.query(QueryUndeclared.INSTANCE);
                assertThatThrownBy(() -> stage.toCompletableFuture().get(2, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(QueryException.class)
                        .hasMessageContaining("No such dependency: A");
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void routable_wins_over_type_routing() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = new GraphBuilder()
                    .addWithParam("Tenant_X",
                            (SerializableSupplier<ParamProcessInitializer<TenantId>>) TenantInit::new,
                            (SerializableSupplier<ParamProcessLoader<TenantId>>) TenantInit::new,
                            new TenantId("X"))
                    .addWithParam("Tenant_Y",
                            (SerializableSupplier<ParamProcessInitializer<TenantId>>) TenantInit::new,
                            (SerializableSupplier<ParamProcessLoader<TenantId>>) TenantInit::new,
                            new TenantId("Y"))
                    .build();

            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                Object r = engine.query(new TenantQuery("X", "hi")).toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(r).isEqualTo("X:hi");
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void dynamic_route_for_multi_tenant() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = new GraphBuilder()
                    .addWithParam("Inv_PUB1",
                            (SerializableSupplier<ParamProcessInitializer<TenantId>>) TenantInit::new,
                            (SerializableSupplier<ParamProcessLoader<TenantId>>) TenantInit::new,
                            new TenantId("PUB1"))
                    .addWithParam("Inv_PUB2",
                            (SerializableSupplier<ParamProcessInitializer<TenantId>>) TenantInit::new,
                            (SerializableSupplier<ParamProcessLoader<TenantId>>) TenantInit::new,
                            new TenantId("PUB2"))
                    .route(NonRoutableTenantQuery.class, q -> "Inv_" + q.publisherId())
                    .build();

            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                Object r1 = engine.query(new NonRoutableTenantQuery("PUB1", "alpha")).toCompletableFuture().get(2, TimeUnit.SECONDS);
                Object r2 = engine.query(new NonRoutableTenantQuery("PUB2", "beta")).toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(r1).isEqualTo("PUB1:alpha");
                assertThat(r2).isEqualTo("PUB2:beta");
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void unknown_type_fails_with_helpful_message() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = new GraphBuilder()
                    .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                            (SerializableSupplier<ProcessLoader>) AInit::new)
                    .build();

            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                var stage = engine.query("string-without-route");
                assertThatThrownBy(() -> stage.toCompletableFuture().get(1, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(QueryException.class)
                        .hasMessageContaining("No route for type java.lang.String")
                        .hasMessageContaining("queryProcess");
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void queryProcess_bypasses_type_routing() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = new GraphBuilder()
                    .addWithParam("Tenant_Z",
                            (SerializableSupplier<ParamProcessInitializer<TenantId>>) TenantInit::new,
                            (SerializableSupplier<ParamProcessLoader<TenantId>>) TenantInit::new,
                            new TenantId("Z"))
                    .build();

            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                Object r = engine.queryProcess("Tenant_Z", new NonRoutableTenantQuery("Z", "direct"))
                        .toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(r).isEqualTo("Z:direct");
            }
        }
    }

    // ───────────────── chain graph fixture ─────────────────

    private static Graph chainGraph() {
        return new GraphBuilder()
                .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                        (SerializableSupplier<ProcessLoader>) AInit::new)
                .add("B", (SerializableSupplier<ProcessInitializer>) BInit::new,
                        (SerializableSupplier<ProcessLoader>) BInit::new, "A")
                .add("C", (SerializableSupplier<ProcessInitializer>) CInit::new,
                        (SerializableSupplier<ProcessLoader>) CInit::new, "B")
                    .handles(GetC.class)
                .build();
    }

    static final class OrderTracker {

        static final List<String> INIT_ORDER = new CopyOnWriteArrayList<>();
    }

    record GetA() implements Serializable {

        static final GetA INSTANCE = new GetA();
    }

    record GetB() implements Serializable {

        static final GetB INSTANCE = new GetB();
    }

    record GetC() implements Serializable {

        static final GetC INSTANCE = new GetC();
    }

    record QueryUndeclared() implements Serializable {

        static final QueryUndeclared INSTANCE = new QueryUndeclared();
    }

    record TenantId(String id) implements Serializable { }

    record TenantQuery(String tenant, String payload) implements Routable, Serializable {

        @Override
        public String targetProcess() {
            return "Tenant_" + tenant;
        }
    }

    record NonRoutableTenantQuery(String publisherId, String payload) implements Serializable { }

    static final class AInit implements ProcessInitializer, ProcessLoader {

        static final AtomicInteger INIT_CALLS = new AtomicInteger();

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INIT_CALLS.incrementAndGet();
            OrderTracker.INIT_ORDER.add("A");
            return CompletableFuture.completedFuture(Map.of("m", "a".getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("A"));
        }
    }

    static final class BInit implements ProcessInitializer, ProcessLoader {

        static final AtomicInteger INIT_CALLS = new AtomicInteger();

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INIT_CALLS.incrementAndGet();
            OrderTracker.INIT_ORDER.add("B");
            return CompletableFuture.completedFuture(Map.of("m", "b".getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p) {
            return CompletableFuture.completedFuture(
                    (c, q) -> c.query("A", GetA.INSTANCE).thenApply(a -> a + "→B"));
        }
    }

    static final class CInit implements ProcessInitializer, ProcessLoader {

        static final AtomicInteger INIT_CALLS = new AtomicInteger();

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INIT_CALLS.incrementAndGet();
            OrderTracker.INIT_ORDER.add("C");
            return CompletableFuture.completedFuture(Map.of("m", "c".getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p) {
            return CompletableFuture.completedFuture(
                    (c, q) -> c.query("B", GetB.INSTANCE).thenApply(b -> b + "→C"));
        }
    }

    /** Process whose compute deliberately queries a name NOT in its dependencies. */
    static final class RogueInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p) {
            return CompletableFuture.completedFuture(
                    (c, q) -> c.query("A", GetA.INSTANCE));
        }
    }

    static final class TenantInit implements ParamProcessInitializer<TenantId>, ParamProcessLoader<TenantId> {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, TenantId p) {
            return CompletableFuture.completedFuture(Map.of("tenant", p.id().getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, TenantId p) {
            String tenant = p.id();
            return CompletableFuture.completedFuture((c, q) -> {
                String payload = switch (q) {
                    case TenantQuery tq -> tq.payload();
                    case NonRoutableTenantQuery nq -> nq.payload();
                    default -> "?";
                };
                return CompletableFuture.completedFuture(tenant + ":" + payload);
            });
        }
    }
}
