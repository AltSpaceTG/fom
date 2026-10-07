package io.fom;

import io.fom.serde.JavaSerializableSerDe;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeSnapshotPolicyTest {

    /** Regression: a FixedInterval child used to be silently dropped (activate() returned null). */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void fixed_interval_child_snapshots_periodically_in_engine() throws Exception {
        var policy = new CompositeSnapshotPolicy(
                new SnapshotPolicy.FixedInterval(Duration.ofMillis(100), 1));
        try (CountingLogBackend backend = new CountingLogBackend();
             Engine engine = new Engine(SizeBasedSnapshotPolicyTest.configWith(policy), backend,
                     new JavaSerializableSerDe())) {
            engine.newGraph(SizeBasedSnapshotPolicyTest.singleNodeGraph());
            for (int i = 0; i < 12; i++) engine.trigger("Echo", "tick-" + i);

            Awaitility.await().atMost(Duration.ofSeconds(3))
                    .until(() -> backend.compacts.get() >= 3);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void size_or_time_composite_with_disabled_child_and_close_cancels() throws Exception {
        try (FakeSnapshotContext ctx = new FakeSnapshotContext()) {
            var policy = new CompositeSnapshotPolicy(
                    SnapshotPolicy.Disabled.INSTANCE,
                    new SizeBasedSnapshotPolicy(1_000, Duration.ofMillis(20), 2),
                    new SnapshotPolicy.FixedInterval(Duration.ofMillis(50), 2));
            AutoCloseable handle = policy.activate(ctx);
            Awaitility.await().atMost(Duration.ofSeconds(2))
                    .until(() -> ctx.snapshotCalls.get() >= 2 && ctx.purgeCalls.get() >= 2);

            handle.close();
            Thread.sleep(100); // let an already-running tick finish
            int after = ctx.snapshotCalls.get();
            Thread.sleep(300);
            assertThat(ctx.snapshotCalls.get()).as("closed handle cancels the fixed-interval child").isEqualTo(after);
        }
    }

    @Test
    void disabled_only_composite_never_snapshots() throws Exception {
        try (FakeSnapshotContext ctx = new FakeSnapshotContext()) {
            ctx.length.set(1_000_000);
            AutoCloseable handle = new CompositeSnapshotPolicy(SnapshotPolicy.Disabled.INSTANCE).activate(ctx);
            Thread.sleep(200);
            handle.close();
            assertThat(ctx.snapshotCalls.get()).isZero();
        }
    }
}
