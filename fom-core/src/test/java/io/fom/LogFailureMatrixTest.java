package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.LeadershipLostException;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogBackendReport;
import io.fom.log.LogCleanedUp;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogDead;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLoaded;
import io.fom.log.LogPaused;
import io.fom.log.LogResumed;
import io.fom.log.LogTrigger;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every durable append can fail, and a backend may report it in three ways: by refusing it
 * ({@code Optional.empty()}), by throwing {@link LeadershipLostException}, or by throwing
 * {@link IllegalArgumentException} — see {@code LogBackend.append}. Whichever cell of that matrix
 * you land in, two things must hold:
 *
 * <ul>
 *   <li>the engine stops hammering the log — a permanent failure is never retried for ever;</li>
 *   <li>the failure is visible: thrown to the caller, or reported to the observer, or on
 *       {@code NodeReport.lastException}. Never all three silent.</li>
 * </ul>
 *
 * Individual cells have their own tests with exact assertions; this one closes the matrix so a
 * newly added append, or a new call site, cannot fail silently or spin.
 */
class LogFailureMatrixTest {

    /**
     * How the backend reports the failure, and what the engine owes the caller for it.
     * {@code permanent} shapes must stop the engine appending; a transient one may keep retrying,
     * but must still not do it in silence.
     */
    enum Shape {
        REFUSED(null, true, "leader"),
        THROWS_LEADERSHIP(() -> new LeadershipLostException("fenced (injected)"), true, "(injected)"),
        THROWS_ILLEGAL_ARGUMENT(() -> new IllegalArgumentException("unstorable (injected)"), true, "(injected)"),
        THROWS_TRANSIENT(() -> new IllegalStateException("io blip (injected)"), false, "(injected)");

        final Supplier<RuntimeException> thrower;
        final boolean permanent;
        /** Text the visible failure must contain, so a cell cannot pass on an unrelated error. */
        final String marker;

        Shape(Supplier<RuntimeException> thrower, boolean permanent, String marker) {
            this.thrower = thrower;
            this.permanent = permanent;
            this.marker = marker;
        }
    }

    /**
     * One operation of the control plane and the event it must persist. {@code informational}
     * marks an event whose loss the engine deliberately swallows with a WARN, because nothing reads
     * it back: {@code LogCleanedUp} (the retirement itself is the {@code LogDead} that landed
     * before it) and {@code LogDependencyChanged} (the consumer is re-initialised either way).
     * Such a cell must still be attempted and must not spin, but it is allowed to be silent to the
     * caller and the observer.
     */
    record Operation(String name, Class<? extends LogEvent> event, boolean informational, Step step) {
        Operation(String name, Class<? extends LogEvent> event, Step step) {
            this(name, event, false, step);
        }

        @Override
        public String toString() {
            return name + " / " + event.getSimpleName();
        }
    }

    interface Step {
        void run(Engine engine, FaultyBackend backend) throws Exception;
    }

    static List<Object[]> matrix() {
        var operations = List.of(
                new Operation("leader claim", io.fom.log.LogLeader.class, (e, b) -> e.newGraph(graph())),
                new Operation("newGraph", LogChangeGraph.class, (e, b) -> e.newGraph(graph())),
                new Operation("first init", LogInitialized.class, (e, b) -> e.newGraph(graph())),
                new Operation("first load", LogLoaded.class, (e, b) -> e.newGraph(graph())),
                new Operation("trigger", LogTrigger.class, (e, b) -> {
                    e.newGraph(graph());
                    e.trigger("A", "again");
                }),
                // A re-init writes its new version beside the serving one; each of its appends must
                // report its own failure (the old version keeps serving).
                new Operation("re-init", LogInitialized.class, (e, b) -> {
                    b.armed = false;
                    e.newGraph(graph());
                    b.armed = true;
                    e.trigger("A", "again");
                }),
                new Operation("re-init", LogLoaded.class, (e, b) -> {
                    b.armed = false;
                    e.newGraph(graph());
                    b.armed = true;
                    e.trigger("A", "again");
                }),
                // The re-init's LogDead retires the replaced version after the switch: hygiene only
                // (the LogLoaded already made the new version the live one), so a WARN is enough.
                // The other LogDead call sites each report their own failure: the cells below are not
                // redundant — a bug fixed in one of them was live in another.
                new Operation("re-init", LogDead.class, true, (e, b) -> {
                    e.newGraph(graph());
                    e.trigger("A", "again");
                }),
                // RELEASE_FIRST writes it first, and cannot re-init without it.
                new Operation("re-init, release-first", LogDead.class, (e, b) -> {
                    e.newGraph(graph());
                    e.updateConfig(e.config().withReinitStrategy(ReinitStrategy.RELEASE_FIRST));
                    e.trigger("A", "again");
                }),
                new Operation("removal", LogDead.class, (e, b) -> {
                    e.newGraph(producerAndConsumer());
                    e.remove(List.of("B"));
                }),
                new Operation("swap replacing a node", LogDead.class, (e, b) -> {
                    e.newGraph(paramGraph("v0"));
                    e.newGraph(paramGraph("v1"));
                }),
                new Operation("retiring a paused node", LogDead.class, (e, b) -> {
                    e.newGraph(producerAndConsumer());
                    e.pause(List.of("B"));
                    e.remove(List.of("B"));
                }),
                new Operation("pause", LogPaused.class, (e, b) -> {
                    e.newGraph(graph());
                    e.pause(List.of("A"));
                }),
                new Operation("resume", LogResumed.class, (e, b) -> {
                    e.newGraph(graph());
                    e.pause(List.of("A"));
                    e.resume(List.of("A"));
                }),
                new Operation("cleanup", LogCleanedUp.class, true, (e, b) -> {
                    e.newGraph(graph());
                    e.trigger("A", "again");
                }),
                // Also informational: the cascade's own marker. The consumer is re-initialised
                // either way, so losing the marker must not stop the cascade.
                new Operation("cascade marker", io.fom.log.LogDependencyChanged.class, true, (e, b) -> {
                    e.newGraph(producerAndConsumer());
                    e.trigger("A", "again");
                }));
        var cells = new ArrayList<Object[]>();
        for (Operation operation : operations) {
            for (Shape shape : Shape.values()) {
                cells.add(new Object[]{operation, shape});
            }
        }
        return cells;
    }

    @ParameterizedTest(name = "[{index}] {0} {1}")
    @MethodSource("matrix")
    @Timeout(60)
    void a_failing_append_is_never_retried_for_ever_and_never_silent(Operation operation, Shape shape)
            throws Exception {
        var failures = new CopyOnWriteArrayList<String>();
        EngineObserver observer = new EngineObserver() {
            @Override
            public void onInitFailed(String processName, int attempt, Throwable cause) {
                failures.add("init " + cause);
            }

            @Override
            public void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) {
                failures.add("load " + cause);
            }
        };
        var backend = new FaultyBackend(new InMemoryLogBackend(), operation.event(), shape);
        var config = new EngineConfig(
                Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofMillis(500),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        var engine = new Engine(config, backend, new JavaSerializableSerDe(), observer);
        String thrown = null;
        try {
            backend.armed = true;
            try {
                operation.step().run(engine, backend);
            } catch (RuntimeException e) {
                thrown = withCauses(e); // the injected failure is often the cause, not the message
            }

            // Give every retry loop time to show itself, then require the appends to have stopped.
            Thread.sleep(1_500);
            int settled = backend.attempts.get();
            Thread.sleep(1_500);
            if (shape.permanent) {
                assertThat(backend.attempts.get())
                        .as("%s: the engine must stop appending an event it can never write", operation)
                        .isEqualTo(settled);
            }

            String lastException = engine.introspect().toCompletableFuture().get()
                    .graph().nodes().stream()
                    .map(EngineReport.NodeReport::lastException)
                    .filter(java.util.Objects::nonNull)
                    .findFirst().orElse(null);
            assertThat(backend.attempts.get())
                    .as("%s: the event was attempted, so the cell really was exercised", operation)
                    .isPositive();
            if (!operation.informational()) {
                String visible = String.join(" | ",
                        thrown == null ? "" : thrown, String.join(",", failures),
                        lastException == null ? "" : lastException);
                assertThat(visible)
                        .as("%s %s: the failure must be visible, and be the injected one", operation, shape)
                        .contains(shape.marker);
            }
        } finally {
            backend.armed = false; // let close() write what it needs
            engine.close();
        }
    }

    /** Renders the whole chain, as the engine's own logs do: a cause carries the real reason. */
    private static String withCauses(Throwable t) {
        var text = new StringBuilder();
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (text.length() > 0) text.append(" <- ");
            text.append(current);
        }
        return text.toString();
    }

    private static Graph paramGraph(String value) {
        return new GraphBuilder().addWithParam("A",
                (Supplier<io.fom.api.ParamProcessInitializer<String>>) ParamNode::new,
                (Supplier<io.fom.api.ParamProcessLoader<String>>) ParamNode::new,
                value).build();
    }

    static final class ParamNode implements io.fom.api.ParamProcessInitializer<String>,
            io.fom.api.ParamProcessLoader<String> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, String param) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, String param) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(param));
        }
    }

    private static Graph producerAndConsumer() {
        return new GraphBuilder()
                .add("A", (Supplier<ProcessInitializer>) Node::new, (Supplier<ProcessLoader>) Node::new)
                .add("B", (Supplier<ProcessInitializer>) Node::new, (Supplier<ProcessLoader>) Node::new, "A")
                .build();
    }

    private static Graph graph() {
        return new GraphBuilder()
                .add("A", (Supplier<ProcessInitializer>) Node::new, (Supplier<ProcessLoader>) Node::new)
                .build();
    }

    static final class Node implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            return CompletableFuture.completedFuture(
                    (Process) (c, q) -> CompletableFuture.completedFuture("A-ok"));
        }
    }

    /** Fails one kind of event, in one of the three documented shapes, and counts the attempts. */
    static final class FaultyBackend implements LogBackend {
        private final LogBackend delegate;
        private final Class<? extends LogEvent> failing;
        private final Shape shape;
        final AtomicInteger attempts = new AtomicInteger();
        volatile boolean armed;

        FaultyBackend(LogBackend delegate, Class<? extends LogEvent> failing, Shape shape) {
            this.delegate = delegate;
            this.failing = failing;
            this.shape = shape;
        }

        @Override
        public Optional<LogEvent> append(LogEvent event, String leaderInstanceId) {
            if (armed && failing.isInstance(event)) {
                attempts.incrementAndGet();
                if (shape.thrower == null) return Optional.empty();
                throw shape.thrower.get();
            }
            return delegate.append(event, leaderInstanceId);
        }

        @Override public String logId() { return delegate.logId(); }
        @Override public int length() { return delegate.length(); }
        @Override public LogEvent get(int position) { return delegate.get(position); }
        @Override public LogEvent[] getBetween(int from, int to) { return delegate.getBetween(from, to); }
        @Override public LogBackendReport introspect() { return delegate.introspect(); }
        @Override public SnapshotResult compact(List<LogEvent> events, String id) {
            return delegate.compact(events, id);
        }
        @Override public void close() { delegate.close(); }
    }
}
