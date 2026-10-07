package io.fom;

import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.QueryRejectedException;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogEvent;
import io.fom.log.LogPaused;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import java.util.function.Supplier;

/** {@link Engine#pause}, {@link Engine#resume} and {@link Engine#remove}. */
class EngineProcessLifecycleTest {

    static final Map<String, AtomicInteger> INITS = new ConcurrentHashMap<>();

    record Name(String value) implements Serializable { }

    static final class Node implements ParamProcessInitializer<Name>, ParamProcessLoader<Name> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Name name) {
            int gen = inits(name.value()).incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("gen", ByteBuffer.allocate(4).putInt(gen).array()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, Name name) {
            int gen = ByteBuffer.wrap(props.get("gen")).getInt();
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture(name.value() + ":" + gen));
        }
    }

    static AtomicInteger inits(String name) {
        return INITS.computeIfAbsent(name, k -> new AtomicInteger());
    }

    @BeforeEach
    void reset() {
        INITS.clear();
    }

    private static EngineConfig cfg() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    private static void add(GraphBuilder b, String name, String... deps) {
        b.addWithParam(name,
                (Supplier<ParamProcessInitializer<Name>>) Node::new,
                (Supplier<ParamProcessLoader<Name>>) Node::new,
                new Name(name), deps);
    }

    /** G (independent), A, and B depending reactively on A. */
    private static Graph graph() {
        var b = new GraphBuilder();
        add(b, "G");
        add(b, "A");
        add(b, "B", "A");
        return b.build();
    }

    private static Engine engine(InMemoryLogBackend backend) {
        return new Engine(cfg(), backend, new JavaSerializableSerDe());
    }

    private static Object query(Engine e, String name) throws Exception {
        return e.queryProcess(name, "q").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(30)
    void paused_processes_reject_queries_and_resume_warm() throws Exception {
        try (var backend = new InMemoryLogBackend(); var e = engine(backend)) {
            e.newGraph(graph());
            e.pause(List.of("A", "B"));

            assertThatThrownBy(() -> query(e, "A"))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(QueryRejectedException.class)
                    .hasMessageContaining("paused");
            assertThat(query(e, "G")).isEqualTo("G:1");
            var states = e.introspect().toCompletableFuture().get().graph().nodes().stream()
                    .filter(n -> n.state().equals("Paused")).map(EngineReport.NodeReport::name).toList();
            assertThat(states).containsExactlyInAnyOrder("A", "B");

            e.resume(List.of("A", "B"));
            assertThat(query(e, "A")).isEqualTo("A:1");
            assertThat(query(e, "B")).isEqualTo("B:1");
            assertThat(inits("A")).as("resume warm-loads").hasValue(1);
            assertThat(inits("B")).hasValue(1);
        }
    }

    @Test
    @Timeout(30)
    void consumers_must_be_paused_with_their_producers_and_resumed_after_them() throws Exception {
        try (var backend = new InMemoryLogBackend(); var e = engine(backend)) {
            e.newGraph(graph());
            assertThatThrownBy(() -> e.pause(List.of("A")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'B' depends on it");
            assertThatThrownBy(() -> e.pause(List.of("nope")))
                    .isInstanceOf(IllegalArgumentException.class);

            e.pause(List.of("A", "B"));
            assertThatThrownBy(() -> e.resume(List.of("B")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("still paused");
        }
    }

    @Test
    @Timeout(30)
    void trigger_while_paused_reinitialises_on_resume() throws Exception {
        try (var backend = new InMemoryLogBackend(); var e = engine(backend)) {
            e.newGraph(graph());
            e.pause(List.of("A", "B"));
            assertThat(e.trigger("A", "refresh")).isTrue();
            assertThat(inits("A")).as("nothing runs while paused").hasValue(1);

            e.resume(List.of("A", "B"));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(query(e, "A")).isEqualTo("A:2"));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(query(e, "B")).isEqualTo("B:2"));
        }
    }

    @Test
    @Timeout(30)
    void producer_change_while_consumer_is_paused_reinitialises_it_on_resume() throws Exception {
        try (var backend = new InMemoryLogBackend(); var e = engine(backend)) {
            e.newGraph(graph());
            e.pause(List.of("B"));
            e.trigger("A", "refresh");
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(query(e, "A")).isEqualTo("A:2"));

            e.resume(List.of("B"));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(query(e, "B")).isEqualTo("B:2"));
        }
    }

    @Test
    @Timeout(30)
    void removed_processes_lose_their_state() throws Exception {
        try (var backend = new InMemoryLogBackend(); var e = engine(backend)) {
            e.newGraph(graph());
            assertThatThrownBy(() -> e.remove(List.of("A")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'B' depends on it");

            assertThat(e.remove(List.of("B"))).isTrue();
            assertThat(e.currentGraph().nodes()).doesNotContainKey("B");
            assertThatThrownBy(() -> query(e, "B")).isInstanceOf(ExecutionException.class);

            e.newGraph(graph());
            assertThat(query(e, "B")).as("re-added process cold-inits").isEqualTo("B:2");
        }
    }

    @Test
    @Timeout(30)
    void removing_a_paused_process_retires_its_state_too() throws Exception {
        try (var backend = new InMemoryLogBackend()) {
            try (var e = engine(backend)) {
                e.newGraph(graph());
                e.pause(List.of("B"));
                e.remove(List.of("B"));
                assertThat(e.introspect().toCompletableFuture().get().graph().nodes())
                        .noneMatch(n -> n.name().equals("B"));
            }
            try (var e = engine(backend)) {
                e.newGraph(graph());
                assertThat(query(e, "B")).as("no warm load of removed state after restart").isEqualTo("B:2");
            }
        }
    }

    @Test
    @Timeout(30)
    void graph_swap_cannot_add_a_running_consumer_of_a_paused_process() throws Exception {
        try (var backend = new InMemoryLogBackend(); var e = engine(backend)) {
            e.newGraph(graph());
            e.pause(List.of("A", "B"));
            var next = new GraphBuilder();
            add(next, "G");
            add(next, "A");
            add(next, "B", "A");
            add(next, "C", "A");
            assertThatThrownBy(() -> e.newGraph(next.build()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("paused");
        }
    }

    // ───────────────── the pause is persisted ─────────────────

    private static void assertPaused(Engine e, String name) {
        assertThatThrownBy(() -> query(e, name))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(QueryRejectedException.class)
                .hasMessageContaining("paused");
    }

    @Test
    @Timeout(30)
    void pause_survives_a_restart_and_resume_survives_the_next_one() throws Exception {
        try (var backend = new InMemoryLogBackend()) {
            try (var e = engine(backend)) {
                e.newGraph(graph());
                e.pause(List.of("A", "B"));
            }
            try (var e = engine(backend)) {
                e.newGraph(graph());
                assertPaused(e, "A");
                assertPaused(e, "B");
                assertThat(query(e, "G")).isEqualTo("G:1");
                assertThat(inits("A")).as("a paused process does not start").hasValue(1);

                e.resume(List.of("A", "B"));
                assertThat(query(e, "A")).as("warm-loaded after restart + resume").isEqualTo("A:1");
            }
            try (var e = engine(backend)) {
                e.newGraph(graph());
                assertThat(query(e, "A")).isEqualTo("A:1");
                assertThat(query(e, "B")).isEqualTo("B:1");
            }
            assertThat(inits("A")).hasValue(1);
        }
    }

    @Test
    @Timeout(30)
    void staleness_survives_a_restart() throws Exception {
        try (var backend = new InMemoryLogBackend()) {
            try (var e = engine(backend)) {
                e.newGraph(graph());
                e.pause(List.of("A", "B"));
                e.trigger("A", "refresh");
            }
            try (var e = engine(backend)) {
                e.newGraph(graph());
                e.resume(List.of("A", "B"));
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(query(e, "A")).isEqualTo("A:2"));
            }
        }
    }

    @Test
    @Timeout(30)
    void pause_survives_a_snapshot() throws Exception {
        try (var backend = new InMemoryLogBackend()) {
            try (var e = engine(backend)) {
                e.newGraph(graph());
                e.pause(List.of("A", "B"));
                e.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            long pausedMarkers = java.util.Arrays.stream(backend.getBetween(0, backend.length()))
                    .filter(ev -> ev instanceof LogPaused).map(LogEvent::getClass).count();
            assertThat(pausedMarkers).isEqualTo(2);
            try (var e = engine(backend)) {
                e.newGraph(graph());
                assertPaused(e, "A");
                e.resume(List.of("A", "B"));
                assertThat(query(e, "B")).isEqualTo("B:1");
            }
        }
    }

    @Test
    @Timeout(30)
    void restart_pauses_a_new_running_consumer_of_a_paused_process() throws Exception {
        try (var backend = new InMemoryLogBackend()) {
            try (var e = engine(backend)) {
                e.newGraph(graph());
                e.pause(List.of("A", "B"));
            }
            var withConsumer = new GraphBuilder();
            add(withConsumer, "G");
            add(withConsumer, "A");
            add(withConsumer, "B", "A");
            add(withConsumer, "C", "A");
            Graph next = withConsumer.build();
            try (var e = engine(backend)) {
                e.newGraph(next);
                assertPaused(e, "C");
                assertThat(inits("C")).hasValue(0);
                e.resume(List.of("A", "B", "C"));
                assertThat(query(e, "C")).isEqualTo("C:1");
            }
        }
    }

    @Test
    @Timeout(30)
    void a_pause_for_a_dependency_is_told_apart_and_lapses_once_the_dependency_is_resumed() throws Exception {
        try (var backend = new InMemoryLogBackend()) {
            var withoutConsumer = new GraphBuilder();
            add(withoutConsumer, "G");
            add(withoutConsumer, "A");
            try (var e = engine(backend)) {
                e.newGraph(withoutConsumer.build());
                e.pause(List.of("A"));
            }
            try (var e = engine(backend)) {
                e.newGraph(graph());
                assertPaused(e, "B");
                assertThat(e.pausedByDependency()).containsExactly("B");
                e.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
                e.resume(List.of("A"));
                assertPaused(e, "B"); // resuming A does not resume B by itself
            }
            try (var e = engine(backend)) {
                e.newGraph(graph());
                assertThat(query(e, "B")).as("nothing B depends on is paused any more").isEqualTo("B:1");
                assertThat(e.pausedByDependency()).isEmpty();
            }
            try (var e = engine(backend)) {
                e.newGraph(graph());
                assertThat(query(e, "B")).isEqualTo("B:1");
            }
        }
    }

    @Test
    @Timeout(30)
    void pausing_a_process_paused_for_a_dependency_pins_it() throws Exception {
        try (var backend = new InMemoryLogBackend()) {
            var withoutConsumer = new GraphBuilder();
            add(withoutConsumer, "G");
            add(withoutConsumer, "A");
            try (var e = engine(backend)) {
                e.newGraph(withoutConsumer.build());
                e.pause(List.of("A"));
            }
            try (var e = engine(backend)) {
                e.newGraph(graph());
                assertThat(e.pausedByDependency()).containsExactly("B");
                e.pause(List.of("B"));
                assertThat(e.pausedByDependency()).isEmpty();
                e.resume(List.of("A"));
                assertThat(e.resumeUnblocked(List.of("B")))
                        .as("an operator pause is not resumed as a dependency pause").isEmpty();
                assertPaused(e, "B");
            }
            try (var e = engine(backend)) {
                e.newGraph(graph());
                assertThat(query(e, "A")).isEqualTo("A:1");
                assertPaused(e, "B"); // the operator's pause does not lapse
            }
        }
    }
}
