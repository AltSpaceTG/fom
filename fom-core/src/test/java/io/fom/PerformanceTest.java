package io.fom;

import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance budget test for the two headline targets:
 *
 * <ul>
 *   <li><b>Startup:</b> install a {@value #NODES}-node graph in under
 *       {@value #STARTUP_BUDGET_MS} ms (cold init + load of every node, blocking
 *       until all reach {@code Serving}).</li>
 *   <li><b>Query latency:</b> {@code queryProcess} p99 under
 *       {@value #P99_BUDGET_MS} ms once the graph is warm.</li>
 * </ul>
 *
 * <p>Uses {@link InMemoryLogBackend} so the numbers reflect the engine's own
 * dispatch/compute overhead, not disk or network I/O. Nodes are independent
 * (no dependencies) and addressed directly with {@code queryProcess}, so the
 * measurement isolates the per-query mailbox → dispatcher → worker round-trip.</p>
 *
 * <p>Sizes are overridable for ad-hoc scaling:
 * {@code -Dfom.perf.nodes=…}, {@code -Dfom.perf.queries=…},
 * {@code -Dfom.perf.warmup=…}.</p>
 */
class PerformanceTest {

    private static final int NODES =
            Integer.getInteger("fom.perf.nodes", 1_000);
    private static final int MEASURED_QUERIES =
            Integer.getInteger("fom.perf.queries", 50_000);
    private static final int WARMUP_QUERIES =
            Integer.getInteger("fom.perf.warmup", 10_000);

    private static final long STARTUP_BUDGET_MS = 5_000;
    private static final double P99_BUDGET_MS = 5.0;

    private EngineConfig perfConfig() {
        return new EngineConfig(
                Duration.ofSeconds(10),   // defaultInitTimeout
                Duration.ofSeconds(10),   // defaultLoadTimeout
                Duration.ofSeconds(10),   // defaultCleanupTimeout
                Duration.ofSeconds(10),   // defaultComputeTimeout
                Duration.ofSeconds(10),   // defaultCancelInitTimeout
                Duration.ofSeconds(5),    // queryTimeout
                Duration.ofMillis(100),   // logLookupInterval
                Duration.ofMillis(100),   // dedupWindow
                Duration.ofMillis(10),    // backoffMin
                Duration.ofMillis(100),   // backoffMax
                1,                        // maxLoadRetries
                SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void thousand_node_startup_and_query_latency() throws Exception {
        Graph graph = buildGraph(NODES);

        try (LogBackend backend = new InMemoryLogBackend();
             var engine = new Engine(perfConfig(), backend, new JavaSerializableSerDe(), true)) {

            // ---- Startup budget: newGraph blocks until every node is Serving ----
            long startNanos = System.nanoTime();
            engine.newGraph(graph);
            long startupMs = (System.nanoTime() - startNanos) / 1_000_000;

            // Sanity: a node actually serves.
            Object first = engine.queryProcess("P0", new Ping(1), Duration.ofSeconds(5))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(first).isEqualTo(1);

            // ---- Warm up the JIT / thread pools before measuring ----
            for (int i = 0; i < WARMUP_QUERIES; i++) {
                String target = "P" + (i % NODES);
                engine.queryProcess(target, new Ping(i), Duration.ofSeconds(5))
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
            }

            // ---- Measure per-query latency (serial round-trips) ----
            long[] latenciesNanos = new long[MEASURED_QUERIES];
            // A cheap deterministic spread across nodes without Random overhead.
            int idx = 0;
            int step = 7919; // prime, so it visits a wide spread of node indices
            for (int i = 0; i < MEASURED_QUERIES; i++) {
                idx = (idx + step) % NODES;
                String target = "P" + idx;
                long t0 = System.nanoTime();
                Object r = engine.queryProcess(target, new Ping(i), Duration.ofSeconds(5))
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
                latenciesNanos[i] = System.nanoTime() - t0;
                if (r == null) throw new AssertionError("null result at " + i);
            }

            Arrays.sort(latenciesNanos);
            double p50 = latenciesNanos[(int) (MEASURED_QUERIES * 0.50)] / 1_000_000.0;
            double p90 = latenciesNanos[(int) (MEASURED_QUERIES * 0.90)] / 1_000_000.0;
            double p99 = latenciesNanos[(int) (MEASURED_QUERIES * 0.99)] / 1_000_000.0;
            double p999 = latenciesNanos[(int) (MEASURED_QUERIES * 0.999)] / 1_000_000.0;
            double max = latenciesNanos[MEASURED_QUERIES - 1] / 1_000_000.0;

            System.out.printf(
                    "%n=== FOM performance ===%n"
                            + "nodes=%d  startup=%d ms (budget %d)%n"
                            + "queries measured=%d (warmup %d)%n"
                            + "latency ms: p50=%.3f  p90=%.3f  p99=%.3f  p999=%.3f  max=%.3f%n"
                            + "=======================%n",
                    NODES, startupMs, STARTUP_BUDGET_MS,
                    MEASURED_QUERIES, WARMUP_QUERIES,
                    p50, p90, p99, p999, max);

            assertThat(startupMs)
                    .as("startup of %d nodes (ms)", NODES)
                    .isLessThan(STARTUP_BUDGET_MS);
            assertThat(p99)
                    .as("query p99 latency (ms)")
                    .isLessThan(P99_BUDGET_MS);
        }
    }

    private static Graph buildGraph(int nodes) {
        var builder = new GraphBuilder();
        for (int i = 0; i < nodes; i++) {
            builder.add("P" + i, PerfInit::new, PerfInit::new);
        }
        return builder.build();
    }

    /** Minimal process: trivial init, and a compute that echoes the ping's seq. */
    static final class PerfInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            Process live = (c, query) ->
                    CompletableFuture.completedFuture(((Ping) query).seq());
            return CompletableFuture.completedFuture(live);
        }
    }

    record Ping(int seq) implements Serializable { }
}
