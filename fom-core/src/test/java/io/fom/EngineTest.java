package io.fom;

import io.fom.api.LoadException;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogCleanedUp;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogLoaded;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Stage 1 integration tests — drive a single-node graph through the full
 * lifecycle via the public {@link Engine} API.
 */
class EngineTest {

    @BeforeEach
    void resetCounters() {
        EchoInit.INIT_CALLS.set(0);
        EchoInit.LOAD_CALLS.set(0);
        FlakyInit.INIT_CALLS.set(0);
        FlakyInit.FAIL_INITS_REMAINING.set(0);
        FlakyLoader.LOAD_CALLS.set(0);
        FlakyLoader.FAIL_LOADS_REMAINING.set(0);
        TrackingProcess.CLEANUP_CALLS.set(0);
    }

    private EngineConfig fastConfig() {
        return new EngineConfig(
                Duration.ofSeconds(5),
                Duration.ofSeconds(5),
                Duration.ofSeconds(5),
                Duration.ofSeconds(5),
                Duration.ofSeconds(5),
                Duration.ofSeconds(5),
                Duration.ofMillis(100),
                Duration.ofMillis(100),
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void cold_start_init_query_shutdown() throws Exception {
        var backend = new InMemoryLogBackend();
        var node = new GraphBuilder()
                .add("Echo",
                        (SerializableSupplier<ProcessInitializer>) EchoInit::new,
                        (SerializableSupplier<ProcessLoader>) EchoInit::new)
                    .handles(String.class)
                .build();

        try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
            assertThat(engine.newGraph(node)).isTrue();
            Object result = engine.query("hello").toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertThat(result).isEqualTo("echo:hello");
        }

        assertThat(EchoInit.INIT_CALLS).hasValue(1);
        assertThat(EchoInit.LOAD_CALLS).hasValue(1);
        assertThat(TrackingProcess.CLEANUP_CALLS).hasValue(1);

        // Log should contain LogLeader, LogChangeGraph, LogInitialized, LogLoaded, LogCleanedUp.
        LogEvent[] events = backend.getBetween(0, backend.length());
        assertThat(events).hasSize(5);
        assertThat(events[0]).isInstanceOf(LogLeader.class);
        assertThat(events[1]).isInstanceOf(LogChangeGraph.class);
        assertThat(events[2]).isInstanceOf(LogInitialized.class);
        assertThat(events[3]).isInstanceOf(LogLoaded.class);
        assertThat(events[4]).isInstanceOf(LogCleanedUp.class);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void warm_restart_skips_init_uses_load() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            var node = new GraphBuilder()
                    .add("Echo",
                            (SerializableSupplier<ProcessInitializer>) EchoInit::new,
                            (SerializableSupplier<ProcessLoader>) EchoInit::new)
                        .handles(String.class)
                    .build();

            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(node);
                engine.query("first").toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
            int initAfterFirst = EchoInit.INIT_CALLS.get();
            int loadAfterFirst = EchoInit.LOAD_CALLS.get();

            try (var engine2 = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine2.newGraph(node);
                Object r = engine2.query("second").toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(r).isEqualTo("echo:second");
            }

            assertThat(EchoInit.INIT_CALLS).hasValue(initAfterFirst);          // no new init
            assertThat(EchoInit.LOAD_CALLS).hasValue(loadAfterFirst + 1);      // one extra load
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void init_fails_then_succeeds_after_backoff() throws Exception {
        FlakyInit.FAIL_INITS_REMAINING.set(2);
        var backend = new InMemoryLogBackend();
        var node = new GraphBuilder()
                .add("Flaky",
                        (SerializableSupplier<ProcessInitializer>) FlakyInit::new,
                        (SerializableSupplier<ProcessLoader>) FlakyInit::new)
                    .handles(String.class)
                .build();

        try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
            engine.newGraph(node);
            Object r = engine.query("ping").toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(r).isEqualTo("echo:ping");
        }
        assertThat(FlakyInit.INIT_CALLS).hasValueGreaterThanOrEqualTo(3);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void load_failure_falls_back_to_init() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            var node = new GraphBuilder()
                    .add("Echo",
                            (SerializableSupplier<ProcessInitializer>) FlakyLoader::new,
                            (SerializableSupplier<ProcessLoader>) FlakyLoader::new)
                        .handles(String.class)
                    .build();

            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(node);
            }
            int initAfterFirst = FlakyLoader.INIT_CALLS.get();

            FlakyLoader.FAIL_LOADS_REMAINING.set(1);
            try (var engine2 = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine2.newGraph(node);
                Object r = engine2.query("second").toCompletableFuture().get(3, TimeUnit.SECONDS);
                assertThat(r).isEqualTo("echo:second");
            }

            assertThat(FlakyLoader.LOAD_CALLS).hasValueGreaterThanOrEqualTo(2);
            assertThat(FlakyLoader.INIT_CALLS).hasValue(initAfterFirst + 1);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void compute_exception_propagates_to_caller() {
        var backend = new InMemoryLogBackend();
        var node = new GraphBuilder()
                .add("Echo",
                        (SerializableSupplier<ProcessInitializer>) EchoInit::new,
                        (SerializableSupplier<ProcessLoader>) EchoInit::new)
                    .handles(String.class)
                .build();

        try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
            engine.newGraph(node);
            CompletionStage<Object> stage = engine.query("boom");
            assertThatThrownBy(() -> stage.toCompletableFuture().get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("boom");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void query_after_shutdown_fails_fast() throws Exception {
        var backend = new InMemoryLogBackend();
        var node = new GraphBuilder()
                .add("Echo",
                        (SerializableSupplier<ProcessInitializer>) EchoInit::new,
                        (SerializableSupplier<ProcessLoader>) EchoInit::new)
                    .handles(String.class)
                .build();

        Engine engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true);
        engine.newGraph(node);
        engine.shutdown(Duration.ofSeconds(2)).toCompletableFuture().get(3, TimeUnit.SECONDS);
        try {
            CompletionStage<Object> stage = engine.query("late");
            assertThatThrownBy(() -> stage.toCompletableFuture().get(1, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
        } finally {
            engine.close();
        }
    }

    // ───────────────── fixtures ─────────────────

    static final class EchoInit implements ProcessInitializer, ProcessLoader {

        static final AtomicInteger INIT_CALLS = new AtomicInteger();
        static final AtomicInteger LOAD_CALLS = new AtomicInteger();

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INIT_CALLS.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("marker", "echo".getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            LOAD_CALLS.incrementAndGet();
            return CompletableFuture.completedFuture(new TrackingProcess());
        }
    }

    static final class FlakyInit implements ProcessInitializer, ProcessLoader {

        static final AtomicInteger INIT_CALLS = new AtomicInteger();
        static final AtomicInteger FAIL_INITS_REMAINING = new AtomicInteger();

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INIT_CALLS.incrementAndGet();
            if (FAIL_INITS_REMAINING.getAndDecrement() > 0) {
                var f = new CompletableFuture<Map<String, byte[]>>();
                f.completeExceptionally(new RuntimeException("synthetic init failure"));
                return f;
            }
            return CompletableFuture.completedFuture(Map.of("marker", "flaky".getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            return CompletableFuture.completedFuture(new TrackingProcess());
        }
    }

    static final class FlakyLoader implements ProcessInitializer, ProcessLoader {

        static final AtomicInteger INIT_CALLS = new AtomicInteger();
        static final AtomicInteger LOAD_CALLS = new AtomicInteger();
        static final AtomicInteger FAIL_LOADS_REMAINING = new AtomicInteger();

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INIT_CALLS.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("marker", "fl".getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            LOAD_CALLS.incrementAndGet();
            if (FAIL_LOADS_REMAINING.getAndDecrement() > 0) {
                var f = new CompletableFuture<Process>();
                f.completeExceptionally(new LoadException("synthetic load failure"));
                return f;
            }
            return CompletableFuture.completedFuture(new TrackingProcess());
        }
    }

    static final class TrackingProcess implements Process {

        static final AtomicInteger CLEANUP_CALLS = new AtomicInteger();

        @Override
        public CompletionStage<?> compute(QueryableContext ctx, Object query) {
            if ("boom".equals(query)) {
                var f = new CompletableFuture<Object>();
                f.completeExceptionally(new IllegalArgumentException("boom"));
                return f;
            }
            return CompletableFuture.completedFuture("echo:" + query);
        }

        @Override
        public CompletionStage<Void> cleanUp(io.fom.api.ProcessContext ctx) {
            CLEANUP_CALLS.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }
}
