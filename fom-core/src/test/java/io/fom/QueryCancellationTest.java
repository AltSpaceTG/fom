package io.fom;

import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** A query nobody waits for any more must not keep its compute running or stall cleanup. */
class QueryCancellationTest {

    static final List<CompletableFuture<Object>> COMPUTES = new CopyOnWriteArrayList<>();

    static final class Hanging implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> {
                var never = new CompletableFuture<Object>();
                COMPUTES.add(never);
                return never;
            });
        }
    }

    static final List<CompletableFuture<Map<String, byte[]>>> INITS = new CopyOnWriteArrayList<>();

    static final class NeverInits implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            var never = new CompletableFuture<Map<String, byte[]>>();
            INITS.add(never);
            return never;
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    @Test
    @Timeout(30)
    void an_init_that_runs_out_of_its_budget_is_cancelled() {
        INITS.clear();
        var cfg = new EngineConfig(
                Duration.ofMillis(300), Duration.ofSeconds(5), Duration.ofSeconds(1),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        try (var engine = new Engine(cfg, new InMemoryLogBackend(), new JavaSerializableSerDe())) {
            assertThatThrownBy(() -> engine.newGraph(new GraphBuilder()
                    .add("N", (Supplier<ProcessInitializer>) NeverInits::new, (Supplier<ProcessLoader>) NeverInits::new)
                    .build()));
            await().atMost(Duration.ofSeconds(3)).until(() -> !INITS.isEmpty() && INITS.stream().allMatch(CompletableFuture::isCancelled));
        }
    }

    static final List<CompletableFuture<Map<String, byte[]>>> FALLBACK_INITS = new CopyOnWriteArrayList<>();

    /** First init succeeds, load always fails, so the engine falls back to a second init that never ends. */
    static final class LoadBrokenInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            var future = new CompletableFuture<Map<String, byte[]>>();
            FALLBACK_INITS.add(future);
            if (FALLBACK_INITS.size() == 1) future.complete(Map.of());
            return future;
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.failedFuture(new IllegalStateException("load is broken"));
        }
    }

    @Test
    @Timeout(30)
    void cancel_init_during_the_init_that_follows_failed_loads_cancels_that_init() throws Exception {
        FALLBACK_INITS.clear();
        var cfg = new EngineConfig(
                Duration.ofSeconds(20), Duration.ofSeconds(5), Duration.ofSeconds(1),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        try (var engine = new Engine(cfg, new InMemoryLogBackend(), new JavaSerializableSerDe())) {
            Thread installer = Thread.ofVirtual().start(() -> {
                try {
                    engine.newGraph(new GraphBuilder()
                            .add("F", (Supplier<ProcessInitializer>) LoadBrokenInit::new,
                                    (Supplier<ProcessLoader>) LoadBrokenInit::new)
                            .build());
                } catch (RuntimeException expected) {
                    // the node is cancelled before it serves
                }
            });
            await().atMost(Duration.ofSeconds(5)).until(() -> FALLBACK_INITS.size() == 2);
            Sid sid = engine.introspect().toCompletableFuture().get(3, TimeUnit.SECONDS)
                    .graph().nodes().stream().filter(n -> n.name().equals("F")).findFirst().orElseThrow().sid();
            assertThat(sid).as("the fallback init keeps the Sid of the state whose load failed").isNotNull();

            engine.cancelInit(sid).toCompletableFuture().get(3, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(3)).until(() -> FALLBACK_INITS.get(1).isCancelled());
            installer.join(Duration.ofSeconds(10));
        }
    }

    @Test
    @Timeout(30)
    void a_timed_out_query_cancels_its_compute_and_does_not_hold_up_shutdown() throws Exception {
        COMPUTES.clear();
        var cfg = new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(10),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        var engine = new Engine(cfg, new InMemoryLogBackend(), new JavaSerializableSerDe());
        try {
            engine.newGraph(new GraphBuilder()
                    .add("H", (Supplier<ProcessInitializer>) Hanging::new, (Supplier<ProcessLoader>) Hanging::new)
                    .build());
            var reply = engine.queryProcess("H", "q", Duration.ofMillis(100)).toCompletableFuture();
            assertThatThrownBy(() -> reply.get(3, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            await().atMost(Duration.ofSeconds(2)).until(() -> !COMPUTES.isEmpty() && COMPUTES.get(0).isCancelled());
        } finally {
            long start = System.nanoTime();
            engine.close();
            assertThat(Duration.ofNanos(System.nanoTime() - start))
                    .as("no in-flight query left to drain for the 10s cleanup budget")
                    .isLessThan(Duration.ofSeconds(5));
        }
    }
}
