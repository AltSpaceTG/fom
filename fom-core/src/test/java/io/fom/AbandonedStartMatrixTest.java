package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.Process;
import io.fom.api.ProcessContext;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A start can be abandoned in six ways, and a loader can react to that in two ways. Whichever pair
 * you get, four things must hold:
 *
 * <ul>
 *   <li>the caller is not held for the load's whole budget — an abandoned start returns promptly;</li>
 *   <li>the node does not end up {@code Serving} a state nobody asked for;</li>
 *   <li>a {@code Process} that arrives late is released <b>at most once</b> and never serves a query;</li>
 *   <li>nothing the loader started outlives {@code engine.close()} — as long as the loader honours
 *       either the cancellation or the interrupt (a loader that honours neither owns its own
 *       threads, which is the documented contract).</li>
 * </ul>
 *
 * Individual cells have their own tests with exact assertions; this matrix exists because every
 * round of field testing found another cell of it, one at a time.
 */
class AbandonedStartMatrixTest {

    /** Counters the fixtures report into; one engine per cell, so statics are safe here. */
    static final AtomicInteger CLEANUPS = new AtomicInteger();
    static final AtomicInteger PROCESSES_BUILT = new AtomicInteger();
    static final AtomicInteger TICKS = new AtomicInteger();
    static final AtomicInteger SERVED = new AtomicInteger();
    static volatile CountDownLatch loadEntered;
    static volatile boolean stubborn;

    @BeforeEach
    void reset() {
        CLEANUPS.set(0);
        PROCESSES_BUILT.set(0);
        TICKS.set(0);
        SERVED.set(0);
        loadEntered = new CountDownLatch(1);
        gate = new CountDownLatch(1);
        LOADS.set(0);
        hangLoadNumber = 0;
    }

    @AfterEach
    void clearFlag() {
        stubborn = false;
    }

    /**
     * How the start is abandoned. {@code mayServeAgain} marks the two ways that legitimately end
     * with a <em>new</em> generation serving: a graph swap starts the replacement it installed, and
     * a load that merely ran out of budget is retried and can recover. Everywhere else the node
     * must not be left serving.
     */
    record Abandon(String name, boolean mayServeAgain, Step step) {
        Abandon(String name, Step step) {
            this(name, false, step);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    interface Step {
        void run(Engine engine) throws Exception;
    }

    static List<Object[]> matrix() {
        var ways = List.of(
                new Abandon("cancelInit", e -> e.cancelInit("A").toCompletableFuture().get(5, TimeUnit.SECONDS)),
                new Abandon("pause", e -> e.pause(List.of("A"))),
                new Abandon("remove", e -> e.remove(List.of("A"))),
                new Abandon("a swap replacing the node", true, e -> e.newGraph(graph("v2"))),
                new Abandon("close", Engine::close),
                new Abandon("the load budget running out", true, e -> Thread.sleep(1_200)));
        var cells = new ArrayList<Object[]>();
        for (ReinitStrategy strategy : ReinitStrategy.values()) {
            for (Abandon way : ways) {
                for (boolean stubbornLoader : List.of(false, true)) {
                    cells.add(new Object[]{way, stubbornLoader, strategy});
                }
            }
        }
        return cells;
    }

    @ParameterizedTest(name = "[{index}] {0}, stubborn loader={1}, {2}")
    @MethodSource("matrix")
    @Timeout(60)
    void an_abandoned_start_is_prompt_releases_its_process_and_outlives_nothing(
            Abandon way, boolean stubbornLoader, ReinitStrategy strategy) throws Exception {
        stubborn = stubbornLoader;
        var config = new EngineConfig(
                Duration.ofSeconds(20), Duration.ofSeconds(1), Duration.ofMillis(500),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE).withReinitStrategy(strategy);
        var engine = new Engine(config, new InMemoryLogBackend(), new JavaSerializableSerDe(),
                new EngineObserver() { });
        try {
            // The first generation serves, so the abandoned start is the *second* one: that is the
            // shape in which a late Process can collide with a live one.
            engine.newGraph(graph("v1"));
            assertThat(engine.query("ping").toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("v1");
            hangLoadNumber = 2; // the re-init's load, not the one that just served
            engine.trigger("A", "again");
            assertThat(loadEntered.await(5, TimeUnit.SECONDS)).as("the second load is in flight").isTrue();
            // The counters describe only the Process of the abandoned load (see Node.process).

            long startedAt = System.nanoTime();
            try {
                way.step().run(engine);
            } catch (RuntimeException expected) {
                // several of these legitimately fail (a swap whose node cannot start, for instance)
            }
            Duration took = Duration.ofNanos(System.nanoTime() - startedAt);
            assertThat(took).as("%s must not wait out the load", way).isLessThan(Duration.ofSeconds(8));

            boolean oldKeepsServing = strategy == ReinitStrategy.KEEP_OLD
                    && (way.name().equals("cancelInit") || way.name().equals("remove"));
            if (oldKeepsServing && !engine.isClosed()) {
                // KEEP_OLD: the re-init was being made beside the serving version, which stays (a
                // removal of the only node is refused). The abandoned Process must not serve.
                assertThat(state(engine, "A")).isEqualTo("Serving");
                assertThat(engine.query("ping").toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("v1");
            } else if (!engine.isClosed() && !way.mayServeAgain()) {
                assertThat(state(engine, "A"))
                        .as("%s must not leave the node serving a state nobody asked for", way)
                        .isNotEqualTo("Serving");
            }
        } finally {
            hangLoadNumber = 0;
            gate.countDown(); // let a stubborn loader finish, as it would in real life
            if (!engine.isClosed()) engine.close();
        }

        // The Process the loader may have produced late belongs to nobody: released at most once,
        // and it must never have answered a query.
        Thread.sleep(600);
        assertThat(CLEANUPS.get()).as("%s: a Process is released at most once", way).isLessThanOrEqualTo(1);
        assertThat(SERVED.get()).as("%s: an abandoned Process never serves", way).isZero();

        int ticks = TICKS.get();
        Thread.sleep(400);
        assertThat(TICKS.get()).as("%s: nothing the loader started outlives close()", way).isEqualTo(ticks);
    }

    /** Which load to hang; the replacement a swap installs must be free to start. */
    static final AtomicInteger LOADS = new AtomicInteger();
    static volatile int hangLoadNumber;
    static volatile CountDownLatch gate;

    private static Graph graph(String value) {
        return new GraphBuilder().addWithParam("A",
                (Supplier<io.fom.api.ParamProcessInitializer<String>>) Node::new,
                (Supplier<io.fom.api.ParamProcessLoader<String>>) Node::new,
                value).handles(String.class).build();
    }

    private static String state(Engine e, String name) throws Exception {
        return e.introspect().toCompletableFuture().get().graph().nodes().stream()
                .filter(n -> n.name().equals(name))
                .map(EngineReport.NodeReport::state)
                .findFirst().orElse("absent");
    }

    /**
     * Its second load hangs. A cooperative loader stops when its stage is cancelled; a stubborn one
     * ignores that and produces a {@code Process} later — but both stop on an interrupt, which is
     * what {@code close()} does to them.
     */
    static final class Node implements io.fom.api.ParamProcessInitializer<String>,
            io.fom.api.ParamProcessLoader<String> {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, String param) {
            return CompletableFuture.completedFuture(
                    Map.of("v", param.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, String param) {
            String value = new String(props.get("v"), java.nio.charset.StandardCharsets.UTF_8);
            if (LOADS.incrementAndGet() != hangLoadNumber) {
                return CompletableFuture.completedFuture(process(value, false));
            }
            var result = new CompletableFuture<Process>();
            Thread.ofVirtual().name("test-loader").start(() -> {
                loadEntered.countDown();
                try {
                    while (!gate.await(25, TimeUnit.MILLISECONDS)) {
                        TICKS.incrementAndGet();
                        if (!stubborn && result.isCancelled()) return; // honours the cancellation
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return; // both kinds honour an interrupt
                }
                PROCESSES_BUILT.incrementAndGet();
                result.complete(process(value, true)); // a no-op once the stage was cancelled
            });
            return result;
        }

        /** {@code late}: made by the abandoned load; only such a Process is counted. */
        private static Process process(String value, boolean late) {
            return new Process() {
                @Override
                public CompletionStage<?> compute(QueryableContext ctx, Object query) {
                    if (late) SERVED.incrementAndGet();
                    return CompletableFuture.completedFuture(value);
                }

                @Override
                public CompletionStage<Void> cleanUp(ProcessContext ctx) {
                    if (late) CLEANUPS.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }
            };
        }
    }
}
