package io.fom;

import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.api.InitInProgressException;
import io.fom.api.QueryException;
import io.fom.api.QueryRejectedException;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrency regression for the in-place graph swap (review findings H-1/H-2/H-3):
 * a flood of queries runs on many virtual threads while the main thread performs
 * repeated {@link Engine#newGraph} swaps.
 *
 * <p>Invariants asserted:</p>
 * <ul>
 *   <li><b>Unchanged nodes stay reachable.</b> Stable nodes (identical across
 *       every swap, so they keep their Sid) must answer every query — no
 *       mis-routing window where a swap makes a live node temporarily unknown.</li>
 *   <li><b>No internal corruption.</b> No querier ever sees an unexpected
 *       throwable (e.g. {@code ConcurrentModificationException}, {@code NPE},
 *       {@code IllegalStateException}) — only the documented lifecycle outcomes.</li>
 *   <li><b>The engine ends consistent</b> with the final graph.</li>
 * </ul>
 */
class GraphSwapConcurrencyTest {

    private static final int STABLE_NODES = 5;
    private static final int SWAPS = 40;
    private static final int QUERIER_THREADS = 16;

    private EngineConfig fastConfig() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofMillis(100), Duration.ofMillis(100),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void queries_survive_concurrent_graph_swaps() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend();
             var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {

            engine.newGraph(graph("v0"));

            var stop = new AtomicBoolean(false);
            var stableQueries = new AtomicLong();
            var churnQueries = new AtomicLong();
            var unexpected = new ConcurrentLinkedQueue<Throwable>();
            var stableFailures = new ConcurrentLinkedQueue<Throwable>();

            var started = new CountDownLatch(QUERIER_THREADS);
            List<Thread> queriers = new ArrayList<>();
            for (int t = 0; t < QUERIER_THREADS; t++) {
                final int seed = t;
                Thread th = Thread.ofVirtual().start(() -> {
                    started.countDown();
                    long i = seed;
                    while (!stop.get()) {
                        i += 7919; // prime stride
                        boolean queryStable = (i & 1) == 0;
                        String target = queryStable
                                ? "S" + (int) (i % STABLE_NODES)
                                : "Churn";
                        try {
                            Object r = engine.queryProcess(target, "ping", Duration.ofSeconds(2))
                                    .toCompletableFuture().get(3, TimeUnit.SECONDS);
                            if (queryStable) {
                                stableQueries.incrementAndGet();
                                if (!("S-served:ping".equals(r))) {
                                    stableFailures.add(new AssertionError(
                                            "stable node returned " + r));
                                }
                            } else {
                                churnQueries.incrementAndGet();
                            }
                        } catch (ExecutionException ee) {
                            Throwable cause = ee.getCause();
                            if (queryStable) {
                                // A stable, unchanged node must never fail — that is the
                                // mis-routing-window guard (H-3).
                                stableFailures.add(cause);
                            } else if (!isAcceptableChurnOutcome(cause)) {
                                // The churn node is perpetually mid-replace under this storm;
                                // rejected/unknown/timeout are all fine. Anything else —
                                // CME/NPE/IllegalStateException — is a real corruption bug.
                                unexpected.add(cause);
                            }
                        } catch (java.util.concurrent.TimeoutException getTimeout) {
                            // .get() budget exceeded. Acceptable only for the churn node.
                            if (queryStable) stableFailures.add(getTimeout);
                        } catch (Throwable other) {
                            unexpected.add(other);
                        }
                    }
                });
                queriers.add(th);
            }

            started.await(5, TimeUnit.SECONDS);

            // Hammer the graph with swaps while queriers run.
            for (int s = 1; s <= SWAPS; s++) {
                engine.newGraph(graph("v" + s));   // Churn param changes → changed node
            }

            stop.set(true);
            for (Thread th : queriers) th.join(10_000);

            // ── assertions ──
            assertThat(unexpected)
                    .as("no unexpected throwable during concurrent swaps: %s", describe(unexpected))
                    .isEmpty();
            assertThat(stableFailures)
                    .as("unchanged nodes must never become unreachable: %s", describe(stableFailures))
                    .isEmpty();
            assertThat(stableQueries.get())
                    .as("stable queries actually ran")
                    .isGreaterThan(0);

            // Engine ends consistent: the 5 stable nodes + Churn are present and serving.
            var report = engine.introspect().toCompletableFuture().get();
            assertThat(report.graph().nodes()).hasSize(STABLE_NODES + 1);
            Object r = engine.queryProcess("Churn", "ping", Duration.ofSeconds(2))
                    .toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(r).isEqualTo("Churn-served:ping");
        }
    }

    /**
     * Outcomes a query to the constantly-replaced churn node may legitimately
     * see: it's mid-shutdown ({@link QueryRejectedException}), momentarily not in
     * the map ({@link QueryException}), or it couldn't be served within its
     * deadline because the node was perpetually mid-replace
     * ({@link java.util.concurrent.TimeoutException}, from the engine's
     * {@code orTimeout}), or it arrived while the fresh Sid had not yet produced
     * a servable state ({@link InitInProgressException}, state {@code NotPresent}).
     * What must NEVER appear is a corruption-class throwable
     * (CME/NPE/IllegalStateException) — that's the actual bug this test guards.
     */
    private static boolean isAcceptableChurnOutcome(Throwable cause) {
        return cause instanceof QueryRejectedException
                || cause instanceof QueryException
                || cause instanceof InitInProgressException
                || cause instanceof java.util.concurrent.TimeoutException;
    }

    private static String describe(ConcurrentLinkedQueue<Throwable> q) {
        if (q.isEmpty()) return "none";
        Throwable first = q.peek();
        return q.size() + " error(s), first = "
                + (first == null ? "null" : first.getClass().getName() + ": " + first.getMessage());
    }

    /** 5 stable nodes (identical every version) + one Churn node whose param changes. */
    private static Graph graph(String churnVersion) {
        var b = new GraphBuilder();
        for (int i = 0; i < STABLE_NODES; i++) {
            b.add("S" + i,
                    (SerializableSupplier<ProcessInitializer>) StableInit::new,
                    (SerializableSupplier<ProcessLoader>) StableInit::new);
        }
        b.addWithParam("Churn",
                (SerializableSupplier<ParamProcessInitializer<Ver>>) ChurnInit::new,
                (SerializableSupplier<ParamProcessLoader<Ver>>) ChurnInit::new,
                new Ver(churnVersion));
        return b.build();
    }

    record Ver(String v) implements Serializable { }

    static final class StableInit implements ProcessInitializer, ProcessLoader {
        @Override public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }
        @Override public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("S-served:" + q));
        }
    }

    static final class ChurnInit implements ParamProcessInitializer<Ver>, ParamProcessLoader<Ver> {
        @Override public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Ver param) {
            return CompletableFuture.completedFuture(Map.of("v", param.v().getBytes()));
        }
        @Override public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p, Ver param) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("Churn-served:" + q));
        }
    }
}
