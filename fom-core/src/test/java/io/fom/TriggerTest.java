package io.fom;

import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogDead;
import io.fom.log.LogDependencyChanged;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogTrigger;
import io.fom.serde.JavaSerializableSerDe;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Stage 4 integration: trigger → re-init, reactive cascade, dedup window, watcher. */
class TriggerTest {

    @BeforeEach
    void reset() {
        Counters.reset();
    }

    private EngineConfig fastConfig() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofMillis(50), Duration.ofMillis(50),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void trigger_causes_reinit_and_new_sid() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = singleNodeGraph();
            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                engine.query("first").toCompletableFuture().get(2, TimeUnit.SECONDS);
                int initBefore = Counters.A_INIT.get();

                engine.trigger("A", new TriggerValue("refresh"));

                Awaitility.await().atMost(Duration.ofSeconds(3))
                        .untilAsserted(() -> assertThat(Counters.A_INIT).hasValue(initBefore + 1));

                // After re-init, query still works, returns "from-A".
                Object r = engine.query("second").toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(r).isEqualTo("from-A:second");

                // Log should contain LogTrigger and LogDead for the old Sid.
                LogEvent[] events = backend.getBetween(0, backend.length());
                assertThat(events).filteredOn(e -> e instanceof LogTrigger).hasSize(1);
                assertThat(events).filteredOn(e -> e instanceof LogDead).hasSize(1);
                assertThat(events).filteredOn(e -> e instanceof LogInitialized).hasSize(2);
            }
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void reactive_dependency_cascade_propagates() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = chainGraphWithReactive();
            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                engine.query("warm").toCompletableFuture().get(2, TimeUnit.SECONDS);

                int aInit = Counters.A_INIT.get();
                int bInit = Counters.B_INIT.get();

                engine.trigger("A", new TriggerValue("v2"));

                // A re-inits, then B (reactive consumer) re-inits via cascade.
                Awaitility.await().atMost(Duration.ofSeconds(5))
                        .untilAsserted(() -> {
                            assertThat(Counters.A_INIT).hasValue(aInit + 1);
                            assertThat(Counters.B_INIT).hasValue(bInit + 1);
                        });

                LogEvent[] events = backend.getBetween(0, backend.length());
                assertThat(events).filteredOn(e -> e instanceof LogDependencyChanged).hasSize(1);
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void stable_dependency_skips_cascade() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            // B declared with Stable dep on A → no cascade.
            Graph g = new GraphBuilder()
                    .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                            (SerializableSupplier<ProcessLoader>) AInit::new)
                    .addDeps("B",
                            (SerializableSupplier<ProcessInitializer>) BInit::new,
                            (SerializableSupplier<ProcessLoader>) BInit::new,
                            Dependency.stable("A"))
                        .handles(String.class)
                    .build();

            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                engine.query("warm").toCompletableFuture().get(2, TimeUnit.SECONDS);
                int bInit = Counters.B_INIT.get();
                int aInit = Counters.A_INIT.get();

                engine.trigger("A", new TriggerValue("change"));
                Awaitility.await().atMost(Duration.ofSeconds(3))
                        .untilAsserted(() -> assertThat(Counters.A_INIT).hasValue(aInit + 1));

                // Give cascade window time to NOT fire.
                Thread.sleep(300);
                assertThat(Counters.B_INIT).hasValue(bInit); // B did NOT re-init
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void dedup_window_collapses_multiple_triggers() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = singleNodeGraph();
            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                engine.query("warm").toCompletableFuture().get(2, TimeUnit.SECONDS);
                int aInit = Counters.A_INIT.get();

                // Fire 5 triggers within the 10ms dedup window — only 1 re-init expected.
                for (int i = 0; i < 5; i++) {
                    engine.trigger("A", new TriggerValue("burst-" + i));
                }
                Thread.sleep(500);
                assertThat(Counters.A_INIT.get() - aInit).isEqualTo(1);
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void watcher_fires_trigger_on_change() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = singleNodeGraph();
            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                engine.query("warm").toCompletableFuture().get(2, TimeUnit.SECONDS);
                int aInit = Counters.A_INIT.get();

                AtomicInteger tick = new AtomicInteger();
                var watcher = new ScheduledWatcher<>(
                        "A", TriggerValue.class, new TriggerValue("v0"),
                        Duration.ofMillis(10), Duration.ofMillis(50),
                        (TriggerValue prev) -> {
                            int n = tick.incrementAndGet();
                            return n == 1 ? Optional.of(new TriggerValue("v" + n)) : Optional.empty();
                        },
                        null);

                AutoCloseable handle = engine.watch(watcher);
                try {
                    Awaitility.await().atMost(Duration.ofSeconds(2))
                            .untilAsserted(() -> assertThat(Counters.A_INIT).hasValue(aInit + 1));
                } finally {
                    handle.close();
                }
            }
        }
    }

    // ───────────────── fixtures ─────────────────

    private static Graph singleNodeGraph() {
        return new GraphBuilder()
                .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                        (SerializableSupplier<ProcessLoader>) AInit::new)
                .handles(String.class)
                .build();
    }

    private static Graph chainGraphWithReactive() {
        return new GraphBuilder()
                .add("A", (SerializableSupplier<ProcessInitializer>) AInit::new,
                        (SerializableSupplier<ProcessLoader>) AInit::new)
                .add("B", (SerializableSupplier<ProcessInitializer>) BInit::new,
                        (SerializableSupplier<ProcessLoader>) BInit::new, "A")
                    .handles(String.class)
                .build();
    }

    record TriggerValue(String tag) implements Serializable { }

    static final class Counters {

        static final AtomicInteger A_INIT = new AtomicInteger();
        static final AtomicInteger B_INIT = new AtomicInteger();
        static final AtomicReference<Object> LAST_TRIGGER = new AtomicReference<>();

        static void reset() {
            A_INIT.set(0);
            B_INIT.set(0);
            LAST_TRIGGER.set(null);
        }
    }

    static final class AInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            Counters.A_INIT.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("v", new byte[]{1}));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("from-A:" + q));
        }
    }

    static final class BInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            Counters.B_INIT.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("v", new byte[]{2}));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            return CompletableFuture.completedFuture(
                    (c, q) -> c.query("A", q).thenApply(a -> "B(" + a + ")"));
        }
    }
}
