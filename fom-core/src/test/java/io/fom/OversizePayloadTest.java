package io.fom;

import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.log.FileLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** An init whose result the log refuses outright is not retried for the whole init budget. */
class OversizePayloadTest {

    static final AtomicInteger INITS = new AtomicInteger();

    @Test
    @Timeout(30)
    void an_init_result_over_the_payload_limits_ends_dead_at_once_naming_the_limits(@TempDir Path dir) throws Exception {
        ProcessInitializer init = ctx -> {
            INITS.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("blob", new byte[10_000_001]));
        };
        ProcessLoader load = (ctx, props) -> CompletableFuture.<Process>completedFuture(
                (c, q) -> CompletableFuture.completedFuture("never"));
        Graph graph = new GraphBuilder()
                .add("Big", (Supplier<ProcessInitializer>) () -> init, (Supplier<ProcessLoader>) () -> load)
                .build();
        var cfg = new EngineConfig(
                Duration.ofSeconds(20), Duration.ofSeconds(5), Duration.ofSeconds(1),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(20), Duration.ofMillis(200), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        try (var backend = new FileLogBackend(dir.resolve("log"));
             var engine = new Engine(cfg, backend, new JavaSerializableSerDe())) {
            long start = System.nanoTime();
            assertThatThrownBy(() -> engine.newGraph(graph)).isInstanceOf(RuntimeException.class);

            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(10));
            assertThat(INITS.get()).as("not retried").isEqualTo(1);
            var node = engine.introspect().toCompletableFuture().get().graph().nodes().get(0);
            assertThat(node.state()).isEqualTo("Dead");
            assertThat(node.lastException()).contains("10,000,000 elements");
        }
    }
}
