package io.fom.micrometer;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.SnapshotPolicy;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The observer against a real engine: what the engine reports for stops vs failures. */
class MicrometerEngineLifecycleTest {

    private static EngineConfig config(Duration initTimeout) {
        return new EngineConfig(initTimeout, Duration.ofSeconds(5), Duration.ofSeconds(2),
                Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    private static final CountDownLatch hangStarted = new CountDownLatch(1);

    /** An init that never completes on its own. */
    static final class Hang implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            hangStarted.countDown();
            return new CompletableFuture<>();
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    /** An init that always fails. */
    static final class Broken implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.failedFuture(new IllegalStateException("upstream down"));
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not met in time");
            Thread.sleep(10);
        }
    }

    private static double count(SimpleMeterRegistry registry, String meter, String name) {
        return registry.find(meter).tag("name", name).counter().count();
    }

    private static Double dead(SimpleMeterRegistry registry, String name) {
        Gauge g = registry.find("engine_process_dead").tag("name", name).gauge();
        return g == null ? null : g.value();
    }

    @Test
    @Timeout(30)
    void pausing_a_process_mid_init_is_a_cancellation_not_a_failure_and_not_dead() throws Exception {
        var registry = new SimpleMeterRegistry();
        var engine = new Engine(config(Duration.ofSeconds(20)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new MicrometerEngineObserver(registry));
        try {
            Thread starter = Thread.ofVirtual().start(() -> {
                try {
                    engine.newGraph(new GraphBuilder().add("Slow", Hang::new, Hang::new).build());
                } catch (RuntimeException expected) {
                    // the start is cut short by the pause
                }
            });
            assertThat(hangStarted.await(10, TimeUnit.SECONDS)).isTrue();

            engine.pause(List.of("Slow"));
            starter.join(Duration.ofSeconds(10));

            // The counter is primed at 0 when the process is first seen: wait for the increment.
            await(() -> count(registry, "engine_process_init_cancellations_total", "Slow") >= 1.0);
            assertThat(registry.find("engine_process_init_cancellations_total").tag("name", "Slow").counter().count())
                    .isEqualTo(1.0);
            assertThat(count(registry, "engine_process_init_failures_total", "Slow")).isEqualTo(0.0);
            assertThat(dead(registry, "Slow")).isEqualTo(0.0);
        } finally {
            engine.close();
        }
        assertThat(count(registry, "engine_process_init_failures_total", "Slow")).isEqualTo(0.0);
    }

    @Test
    @Timeout(30)
    void a_process_that_dies_of_failures_reads_dead_until_removed() throws Exception {
        var registry = new SimpleMeterRegistry();
        try (var engine = new Engine(config(Duration.ofMillis(300)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new MicrometerEngineObserver(registry))) {
            assertThatThrownBy(() -> engine.newGraph(new GraphBuilder()
                    .add("Bad", Broken::new, Broken::new)
                    .add("Ok", Fine::new, Fine::new)
                    .build()));

            await(() -> Double.valueOf(1.0).equals(dead(registry, "Bad")));
            assertThat(registry.find("engine_process_init_failures_total").tag("name", "Bad").counter().count())
                    .isGreaterThanOrEqualTo(1.0);
            assertThat(count(registry, "engine_process_init_cancellations_total", "Bad")).isEqualTo(0.0);

            engine.remove(List.of("Bad"));
            await(() -> dead(registry, "Bad") == null);
        }
    }

    @Test
    @Timeout(30)
    void a_serving_process_reads_not_dead_and_an_orderly_close_keeps_it_at_zero() throws Exception {
        var registry = new SimpleMeterRegistry();
        var engine = new Engine(config(Duration.ofSeconds(5)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new MicrometerEngineObserver(registry));
        engine.newGraph(new GraphBuilder().add("Ok", Fine::new, Fine::new).build());
        assertThat(dead(registry, "Ok")).isEqualTo(0.0);
        engine.close();
        assertThat(dead(registry, "Ok")).isEqualTo(0.0);
    }

    /** An init whose own code keeps failing with a plain CancellationException (its own timeout, say). */
    static final class SelfCancelling implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.failedFuture(new CancellationException("my own call timed out"));
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    @Test
    @Timeout(30)
    void a_user_cancellation_exception_is_a_failure_and_the_process_reads_dead() throws Exception {
        var registry = new SimpleMeterRegistry();
        try (var engine = new Engine(config(Duration.ofMillis(300)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new MicrometerEngineObserver(registry))) {
            assertThatThrownBy(() -> engine.newGraph(new GraphBuilder()
                    .add("SelfCancel", SelfCancelling::new, SelfCancelling::new).build()));

            await(() -> Double.valueOf(1.0).equals(dead(registry, "SelfCancel")));
            assertThat(count(registry, "engine_process_init_failures_total", "SelfCancel"))
                    .isGreaterThanOrEqualTo(1.0);
            assertThat(count(registry, "engine_process_init_cancellations_total", "SelfCancel")).isEqualTo(0.0);
        }
    }

    @Test
    @Timeout(30)
    void pausing_a_process_that_died_of_a_failure_resets_the_dead_gauge() throws Exception {
        var registry = new SimpleMeterRegistry();
        try (var engine = new Engine(config(Duration.ofMillis(300)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new MicrometerEngineObserver(registry))) {
            assertThatThrownBy(() -> engine.newGraph(new GraphBuilder().add("Bad", Broken::new, Broken::new).build()));
            await(() -> Double.valueOf(1.0).equals(dead(registry, "Bad")));
            double failures = count(registry, "engine_process_init_failures_total", "Bad");

            engine.pause(List.of("Bad"));

            await(() -> Double.valueOf(0.0).equals(dead(registry, "Bad")));
            assertThat(count(registry, "engine_process_init_failures_total", "Bad")).isEqualTo(failures);
        }
    }

    @Test
    @Timeout(30)
    void lifecycle_counters_of_a_healthy_process_are_exported_at_zero() throws Exception {
        var registry = new SimpleMeterRegistry();
        try (var engine = new Engine(config(Duration.ofSeconds(5)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new MicrometerEngineObserver(registry))) {
            engine.newGraph(new GraphBuilder().add("Ok", Fine::new, Fine::new).build());
            for (String meter : List.of("engine_process_init_failures_total", "engine_process_load_failures_total",
                    "engine_process_cleanup_failures_total", "engine_process_init_cancellations_total")) {
                assertThat(count(registry, meter, "Ok")).as(meter).isEqualTo(0.0);
            }
        }
    }

    private static final AtomicBoolean loadFails = new AtomicBoolean();

    /** Inits once; its load fails while {@link #loadFails} is set. */
    static final class Flaky implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            if (loadFails.get()) return CompletableFuture.failedFuture(new IllegalStateException("load broken"));
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    @Test
    @Timeout(60)
    void a_new_engine_on_the_same_registry_and_tags_drives_the_dead_gauge_of_a_closed_one() throws Exception {
        var registry = new SimpleMeterRegistry();
        var tags = io.micrometer.core.instrument.Tags.of("engine", "orders");
        try (var engine1 = new Engine(config(Duration.ofMillis(300)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new MicrometerEngineObserver(registry, tags))) {
            assertThatThrownBy(() -> engine1.newGraph(new GraphBuilder().add("P", Broken::new, Broken::new).build()));
            await(() -> Double.valueOf(1.0).equals(dead(registry, "P")));
        }
        assertThat(dead(registry, "P")).isEqualTo(1.0); // close keeps the last value

        loadFails.set(false);
        try (var engine2 = new Engine(config(Duration.ofMillis(300)), new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new MicrometerEngineObserver(registry, tags))) {
            engine2.newGraph(new GraphBuilder().add("P", Flaky::new, Flaky::new).build());
            assertThat(dead(registry, "P")).isEqualTo(0.0);

            loadFails.set(true);
            engine2.pause(List.of("P"));
            try {
                engine2.resume(List.of("P"));
            } catch (RuntimeException expected) {
                // the load keeps failing: the resume reports it, and P ends Dead
            }
            await(() -> Double.valueOf(1.0).equals(dead(registry, "P")));
        } finally {
            loadFails.set(false);
        }
    }

    static final class Fine implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    private static final AtomicBoolean reinitFails = new AtomicBoolean();
    private static final java.util.concurrent.atomic.AtomicInteger inits = new java.util.concurrent.atomic.AtomicInteger();

    /** The first init succeeds; later ones fail while {@link #reinitFails} is set. */
    static final class FailsOnReinit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            if (inits.getAndIncrement() > 0 && reinitFails.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("upstream down"));
            }
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    @Test
    @Timeout(30)
    void a_failed_keep_old_reinit_reads_stale_until_a_reinit_succeeds() throws Exception {
        var registry = new SimpleMeterRegistry();
        inits.set(0);
        reinitFails.set(true);
        var cfg = config(Duration.ofMillis(300)).withReinitRetryBackoff(Duration.ZERO, null);
        try (var engine = new Engine(cfg, new InMemoryLogBackend(),
                new JavaSerializableSerDe(), new MicrometerEngineObserver(registry))) {
            engine.newGraph(new GraphBuilder().add("P", FailsOnReinit::new, FailsOnReinit::new).build());
            Gauge stale = registry.find("engine_process_stale").tag("name", "P").gauge();
            assertThat(stale.value()).isEqualTo(0.0);

            engine.trigger("P", "refresh");
            await(() -> count(registry, "engine_process_reinit_failures_total", "P") >= 1.0);
            assertThat(stale.value()).isEqualTo(1.0);
            assertThat(dead(registry, "P")).isEqualTo(0.0);
            assertThat(engine.queryProcess("P", "q").toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("ok");
            assertThat(registry.find("engine_process_reinit_duration_seconds").timer()).isNull();

            reinitFails.set(false);
            engine.trigger("P", "refresh again");
            await(() -> stale.value() == 0.0);
            await(() -> registry.find("engine_process_reinit_duration_seconds").tag("name", "P").timer() != null);
            assertThat(registry.find("engine_process_reinit_duration_seconds").tag("name", "P").timer().count())
                    .isEqualTo(1);
        } finally {
            reinitFails.set(false);
        }
    }
}
