package io.fom;

import io.fom.api.EngineObserver;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Every sent query reports exactly one outcome, including a caller-side timeout
 * and a hung dependency; {@code onQuerySent} runs on the submitting thread.
 */
class ObserverQueryLifecycleTest {

    record Outcome(String process, UUID queryId, String reason) { }

    static final class Recorder implements EngineObserver {
        final Map<UUID, String> sentOnThread = new ConcurrentHashMap<>();
        final Map<UUID, String> sentTo = new ConcurrentHashMap<>();
        final List<Outcome> outcomes = new CopyOnWriteArrayList<>();

        @Override public void onQuerySent(String p, UUID id, Class<?> type, UUID parent) {
            sentOnThread.put(id, Thread.currentThread().getName());
            sentTo.put(id, p);
        }
        @Override public void onQueryCompleted(String p, UUID id, Duration d) {
            outcomes.add(new Outcome(p, id, "ok"));
        }
        @Override public void onQueryFailed(String p, UUID id, String reason, Throwable cause) {
            outcomes.add(new Outcome(p, id, reason));
        }
    }

    /** "ok" answers, "boom" throws, "hang" never completes, "ask-A" queries dependency A. */
    static final class Node implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> switch ((String) q) {
                case "boom" -> CompletableFuture.failedFuture(new IllegalStateException("boom"));
                case "hang" -> new CompletableFuture<>();
                case "ask-A" -> c.query("A", "hang");
                default -> CompletableFuture.completedFuture("ok");
            });
        }
    }

    private static EngineConfig cfg() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofMillis(500),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    private static Engine engine(Recorder recorder) {
        var engine = new Engine(cfg(), new InMemoryLogBackend(), new JavaSerializableSerDe(), recorder);
        engine.newGraph(new GraphBuilder()
                .add("A", Node::new, Node::new)
                .add("B", Node::new, Node::new, "A")
                .build());
        return engine;
    }

    private static List<String> reasonsFor(Recorder r, String process) {
        return r.outcomes.stream().filter(o -> o.process().equals(process)).map(Outcome::reason).toList();
    }

    @Test
    @Timeout(20)
    void success_and_failure_are_reported_once_and_sent_runs_on_the_caller_thread() throws Exception {
        var recorder = new Recorder();
        try (var engine = engine(recorder)) {
            assertThat(engine.queryProcess("A", "ok").toCompletableFuture().get(3, TimeUnit.SECONDS)).isEqualTo("ok");
            assertThatThrownBy(() -> engine.queryProcess("A", "boom").toCompletableFuture().get(3, TimeUnit.SECONDS));

            await().atMost(Duration.ofSeconds(3)).until(() -> recorder.outcomes.size() == 2);
            assertThat(reasonsFor(recorder, "A")).containsExactly("ok", "exception");
            assertThat(recorder.sentOnThread.values()).containsOnly(Thread.currentThread().getName());
        }
    }

    @Test
    @Timeout(20)
    void a_caller_timeout_is_reported_even_though_compute_never_finishes() {
        var recorder = new Recorder();
        try (var engine = engine(recorder)) {
            assertThatThrownBy(() -> engine.queryProcess("A", "hang", Duration.ofMillis(200))
                    .toCompletableFuture().get(3, TimeUnit.SECONDS));
            await().atMost(Duration.ofSeconds(3)).until(() -> reasonsFor(recorder, "A").contains("timeout"));
            assertThat(recorder.outcomes).hasSize(1);
        }
    }

    @Test
    @Timeout(20)
    void a_hung_dependency_query_times_out_with_the_outer_deadline() {
        var recorder = new Recorder();
        try (var engine = engine(recorder)) {
            assertThatThrownBy(() -> engine.queryProcess("B", "ask-A", Duration.ofMillis(300))
                    .toCompletableFuture().get(3, TimeUnit.SECONDS));
            // A's reply is B's compute stage: it times out on the inherited deadline, or is
            // cancelled first because B's own timeout cancels B's compute — whichever fires first.
            await().atMost(Duration.ofSeconds(3)).until(() ->
                    reasonsFor(recorder, "A").stream().anyMatch(r -> r.equals("timeout") || r.equals("cancelled"))
                            && reasonsFor(recorder, "B").contains("timeout"));
            assertThat(recorder.sentTo).hasSize(2);
            assertThat(recorder.outcomes).hasSize(2);
        }
    }

    @Test
    @Timeout(20)
    void a_query_to_an_unknown_or_paused_process_fires_no_hooks() {
        var recorder = new Recorder();
        try (var engine = engine(recorder)) {
            assertThatThrownBy(() -> engine.queryProcess("nope", "ok").toCompletableFuture().get(3, TimeUnit.SECONDS));
            engine.pause(List.of("B"));
            assertThatThrownBy(() -> engine.queryProcess("B", "ok").toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertThat(recorder.sentTo).isEmpty();
            assertThat(recorder.outcomes).isEmpty();
        }
    }
}
