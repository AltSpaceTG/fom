package io.fom;

import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogDead;
import io.fom.log.LogEvent;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * RELEASE_FIRST: a re-init that cannot persist LogDead retries with growing backoff, not in a hot
 * loop. KEEP_OLD writes that LogDead only after the switch, as hygiene: its failure blocks nothing.
 */
class ReinitBackoffTest {

    /** Fails every LogDead append while {@code failing} is set. */
    static final class FailingDeadBackend implements LogBackend {
        final LogBackend delegate = new InMemoryLogBackend();
        final AtomicBoolean failing = new AtomicBoolean();
        final AtomicInteger deadAttempts = new AtomicInteger();

        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int clock) { return delegate.get(clock); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String id) { return delegate.compact(events, id); }
        @Override public void close() { delegate.close(); }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
            if (event instanceof LogDead && failing.get()) {
                deadAttempts.incrementAndGet();
                throw new IllegalStateException("No space left on device (injected)");
            }
            return delegate.append(event, leaderInstanceId);
        }
    }

    @Test
    @Timeout(30)
    void failed_log_dead_backs_off_and_the_reinit_happens_once_storage_recovers() throws Exception {
        EngineLogConsistencyTest.GEN.set(0);
        var cfg = new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(1),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                SnapshotPolicy.Disabled.INSTANCE).withReinitStrategy(ReinitStrategy.RELEASE_FIRST);
        var backend = new FailingDeadBackend();
        try (var engine = new Engine(cfg, backend, new JavaSerializableSerDe())) {
            engine.newGraph(new GraphBuilder()
                    .add("A", (Supplier<ProcessInitializer>) EngineLogConsistencyTest.GenInit::new,
                            (Supplier<ProcessLoader>) EngineLogConsistencyTest.GenInit::new)
                    .handles(String.class)
                    .build());
            backend.failing.set(true);
            for (int i = 0; i < 5; i++) { // more requests during the outage join the one retry loop
                engine.trigger("A", "t" + i);
                Thread.sleep(100);
            }
            Thread.sleep(1_500);
            // Without backoff (10–15 ms per retry) ~2s of outage means well over 100 attempts.
            assertThat(backend.deadAttempts.get()).isBetween(1, 45);
            assertThat(engine.query("x").toCompletableFuture().get(2, TimeUnit.SECONDS))
                    .as("keeps serving the current state meanwhile").isEqualTo("gen1");

            backend.failing.set(false);
            await().atMost(Duration.ofSeconds(3)).until(() ->
                    "gen2".equals(engine.query("x").toCompletableFuture().get(2, TimeUnit.SECONDS)));
            assertThat(EngineLogConsistencyTest.GEN).hasValue(2);
        }
    }

    @Test
    @Timeout(30)
    void keep_old_switches_even_when_the_old_versions_log_dead_fails() throws Exception {
        EngineLogConsistencyTest.GEN.set(0);
        var cfg = new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(1),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        var backend = new FailingDeadBackend();
        try (var engine = new Engine(cfg, backend, new JavaSerializableSerDe())) {
            engine.newGraph(new GraphBuilder()
                    .add("A", (Supplier<ProcessInitializer>) EngineLogConsistencyTest.GenInit::new,
                            (Supplier<ProcessLoader>) EngineLogConsistencyTest.GenInit::new)
                    .handles(String.class)
                    .build());
            backend.failing.set(true);
            engine.trigger("A", "t");
            await().atMost(Duration.ofSeconds(3)).until(() ->
                    "gen2".equals(engine.query("x").toCompletableFuture().get(2, TimeUnit.SECONDS)));
            Thread.sleep(300);
            assertThat(backend.deadAttempts.get()).as("tried once after the switch, not retried").isEqualTo(1);
        }
    }
}
