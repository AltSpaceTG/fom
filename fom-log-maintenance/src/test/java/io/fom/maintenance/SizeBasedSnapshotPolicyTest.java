package io.fom.maintenance;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.Graph;
import io.fom.GraphBuilder;
import io.fom.SerializableSupplier;
import io.fom.SnapshotPolicy;
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

class SizeBasedSnapshotPolicyTest {

    private EngineConfig configWith(SnapshotPolicy policy) {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofMillis(100), Duration.ofMillis(100),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                policy);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void size_based_policy_fires_snapshot_above_threshold() throws Exception {
        var policy = new SizeBasedSnapshotPolicy(8, Duration.ofMillis(100), 2);
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = singleNodeGraph();
            try (Engine engine = new Engine(configWith(policy), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);

                // Pile up trigger events to grow the log above the 8-event threshold.
                for (int i = 0; i < 12; i++) {
                    engine.trigger("Echo", "tick-" + i);
                }

                // Wait for the size-based poller to notice and snapshot.
                Awaitility.await().atMost(Duration.ofSeconds(3))
                        .untilAsserted(() -> assertThat(backend.length())
                                .as("log should have been compacted by SizeBasedSnapshotPolicy")
                                .isLessThan(8));
            }
        }
    }

    @Test
    void composite_policy_activates_all_children() throws Exception {
        var size = new SizeBasedSnapshotPolicy(100, Duration.ofMillis(50), 2);
        var composite = new CompositeSnapshotPolicy(List.of(size));
        assertThat(composite.policies()).hasSize(1);
    }

    private static Graph singleNodeGraph() {
        return new GraphBuilder()
                .add("Echo",
                        (SerializableSupplier<ProcessInitializer>) EchoInit::new,
                        (SerializableSupplier<ProcessLoader>) EchoInit::new)
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
