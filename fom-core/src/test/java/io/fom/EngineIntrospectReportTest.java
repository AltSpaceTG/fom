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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.function.Supplier;

/** {@link EngineReport.NodeReport} carries real retry counts and the last failure. */
class EngineIntrospectReportTest {

    static final AtomicInteger FLAKY_CALLS = new AtomicInteger();

    /** Fails its first two init attempts, then succeeds. */
    static final class FlakyInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            int call = FLAKY_CALLS.incrementAndGet();
            if (call <= 2) {
                return CompletableFuture.failedFuture(new IllegalStateException("boom " + call));
            }
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    static final class SteadyInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    private static EngineConfig cfg() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(20), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    @Timeout(20)
    void node_report_counts_failed_attempts_and_keeps_the_last_failure() throws Exception {
        FLAKY_CALLS.set(0);
        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(new GraphBuilder()
                    .add("Flaky", (Supplier<ProcessInitializer>) FlakyInit::new,
                            (Supplier<ProcessLoader>) FlakyInit::new)
                    .add("Steady", (Supplier<ProcessInitializer>) SteadyInit::new,
                            (Supplier<ProcessLoader>) SteadyInit::new)
                    .build());

            var nodes = engine.introspect().toCompletableFuture().get().graph().nodes();
            var flaky = nodes.stream().filter(n -> n.name().equals("Flaky")).findFirst().orElseThrow();
            var steady = nodes.stream().filter(n -> n.name().equals("Steady")).findFirst().orElseThrow();

            assertThat(flaky.state()).isEqualTo("Serving");
            assertThat(flaky.initRetries()).isEqualTo(2);
            assertThat(flaky.loadRetries()).isZero();
            assertThat(flaky.lastException()).contains("IllegalStateException").contains("boom 2");

            assertThat(steady.initRetries()).isZero();
            assertThat(steady.lastException()).isNull();
        }
    }
}
