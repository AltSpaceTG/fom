package io.fom;

import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.function.Supplier;

class SizeBasedSnapshotPolicyTest {

    static EngineConfig configWith(SnapshotPolicy policy) {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofMillis(100),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                policy);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void size_based_policy_fires_snapshot_above_threshold() throws Exception {
        var policy = new SizeBasedSnapshotPolicy(8, Duration.ofMillis(100), 2);
        try (CountingLogBackend backend = new CountingLogBackend()) {
            Graph g = singleNodeGraph();
            try (Engine engine = new Engine(configWith(policy), backend, new JavaSerializableSerDe())) {
                engine.newGraph(g);

                // Pile up trigger events to grow the log above the 8-event threshold.
                for (int i = 0; i < 12; i++) {
                    engine.trigger("Echo", "tick-" + i);
                }

                // Wait for the size-based poller to notice and snapshot. The threshold
                // counts growth since the last snapshot, so the log stays below
                // threshold + the compacted floor (4 events for one live process).
                Awaitility.await().atMost(Duration.ofSeconds(3))
                        .untilAsserted(() -> {
                            assertThat(backend.compacts.get())
                                    .as("log should have been compacted by SizeBasedSnapshotPolicy")
                                    .isPositive();
                            assertThat(backend.length()).isLessThan(8 + 4);
                        });
            }
        }
    }

    /** Regression: threshold <= compacted log size used to snapshot on every poll forever. */
    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void threshold_below_compacted_size_does_not_loop() throws Exception {
        var policy = new SizeBasedSnapshotPolicy(3, Duration.ofMillis(50), 2);
        try (CountingLogBackend backend = new CountingLogBackend();
             Engine engine = new Engine(configWith(policy), backend, new JavaSerializableSerDe())) {
            engine.newGraph(singleNodeGraph());
            for (int i = 0; i < 5; i++) engine.trigger("Echo", "a-" + i);

            Awaitility.await().atMost(Duration.ofSeconds(3))
                    .until(() -> backend.compacts.get() >= 1);
            // Triggers are applied asynchronously, so a snapshot can land mid-burst and
            // the remaining events legitimately count as growth. Wait until compaction
            // goes quiet (the pre-fix code never does: it compacted on every poll).
            int[] last = {-1};
            long[] stableSince = {System.nanoTime()};
            Awaitility.await().pollInterval(Duration.ofMillis(25)).atMost(Duration.ofSeconds(4)).until(() -> {
                int now = backend.compacts.get();
                if (now != last[0]) {
                    last[0] = now;
                    stableSince[0] = System.nanoTime();
                    return false;
                }
                return System.nanoTime() - stableSince[0] >= Duration.ofMillis(400).toNanos();
            });
            int settled = backend.compacts.get();

            // ~16 polls with no new events: no further compaction may happen.
            Awaitility.await().pollDelay(Duration.ofMillis(800)).atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(backend.compacts.get())
                            .as("no snapshot without log growth")
                            .isEqualTo(settled));

            // Growth beyond the threshold snapshots again.
            for (int i = 0; i < 5; i++) engine.trigger("Echo", "b-" + i);
            Awaitility.await().atMost(Duration.ofSeconds(3))
                    .until(() -> backend.compacts.get() > settled);
        }
    }

    /** Regression: the poll used to block the engine's single scheduler thread on snapshot().get(). */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void poll_never_blocks_the_scheduler_and_skips_while_in_flight() throws Exception {
        try (FakeSnapshotContext ctx = new FakeSnapshotContext()) {
            ctx.length.set(100);
            var slow = new CompletableFuture<SnapshotResult>();
            ctx.nextResult.set(slow);

            var policy = new SizeBasedSnapshotPolicy(10, Duration.ofMillis(20), 2);
            AutoCloseable handle = policy.activate(ctx);
            try {
                Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> ctx.snapshotCalls.get() == 1);

                // Snapshot is still running — the scheduler must stay responsive.
                long t0 = System.nanoTime();
                ctx.scheduler.submit(() -> { }).get(500, TimeUnit.MILLISECONDS);
                assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofMillis(500));

                // Polls during the in-flight snapshot are skipped.
                Thread.sleep(300);
                assertThat(ctx.snapshotCalls.get()).isEqualTo(1);
                assertThat(ctx.purgeCalls.get()).isZero();

                slow.complete(new SnapshotResult("new", "old", 0, 0));
                Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> ctx.purgeCalls.get() == 1);

                // Baseline is now the compacted length (4): no loop.
                Thread.sleep(300);
                assertThat(ctx.snapshotCalls.get()).isEqualTo(1);

                ctx.length.set(4 + 10);
                Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> ctx.snapshotCalls.get() == 2);
            } finally {
                handle.close();
            }
        }
    }

    @Test
    void composite_policy_holds_all_children() {
        var size = new SizeBasedSnapshotPolicy(100, Duration.ofMillis(50), 2);
        var composite = new CompositeSnapshotPolicy(List.of(size));
        assertThat(composite.policies()).hasSize(1);
    }

    static Graph singleNodeGraph() {
        return new GraphBuilder()
                .add("Echo",
                        (Supplier<ProcessInitializer>) EchoInit::new,
                        (Supplier<ProcessLoader>) EchoInit::new)
                .handles(String.class)
                .build();
    }

    static final class EchoInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of("k", new byte[]{1}));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("echo:" + q));
        }
    }
}
