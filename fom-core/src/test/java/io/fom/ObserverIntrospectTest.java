package io.fom;

import io.fom.api.EngineObserver;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ObserverIntrospectTest {

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
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void observer_receives_lifecycle_events_during_normal_run() throws Exception {
        var recorder = new RecordingObserver();
        try (LogBackend backend = new InMemoryLogBackend()) {
            var g = new GraphBuilder()
                    .add("Echo",
                            (SerializableSupplier<ProcessInitializer>) EchoInit::new,
                            (SerializableSupplier<ProcessLoader>) EchoInit::new)
                        .handles(String.class)
                    .build();
            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true, recorder)) {
                engine.newGraph(g);
                engine.query("hi").toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
        }

        // We should have seen at least: state transitions, init started/completed,
        // load started/completed, query sent/completed, sid promotion, cleanup completed.
        assertThat(recorder.events).anySatisfy(e -> assertThat(e).startsWith("transition Echo NotPresent->Initializing"));
        assertThat(recorder.events).anySatisfy(e -> assertThat(e).startsWith("transition Echo Loading->Serving"));
        assertThat(recorder.events).anySatisfy(e -> assertThat(e).startsWith("init-started Echo"));
        assertThat(recorder.events).anySatisfy(e -> assertThat(e).startsWith("init-completed Echo"));
        assertThat(recorder.events).anySatisfy(e -> assertThat(e).startsWith("load-completed Echo"));
        assertThat(recorder.events).anySatisfy(e -> assertThat(e).startsWith("query-completed Echo"));
        assertThat(recorder.events).anySatisfy(e -> assertThat(e).startsWith("sid-promotion Echo prev=null new=Sid"));
        assertThat(recorder.events).anySatisfy(e -> assertThat(e).startsWith("cleanup-completed Echo"));
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void observer_receives_dedup_collapsed_when_triggered_burst() throws Exception {
        var recorder = new RecordingObserver();
        try (LogBackend backend = new InMemoryLogBackend()) {
            var g = new GraphBuilder()
                    .add("Echo",
                            (SerializableSupplier<ProcessInitializer>) EchoInit::new,
                            (SerializableSupplier<ProcessLoader>) EchoInit::new)
                        .handles(String.class)
                    .build();
            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true, recorder)) {
                engine.newGraph(g);
                engine.query("warm").toCompletableFuture().get(2, TimeUnit.SECONDS);
                for (int i = 0; i < 5; i++) {
                    engine.trigger("Echo", "burst-" + i);
                }
                Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                        assertThat(recorder.events).anySatisfy(e -> assertThat(e).startsWith("dedup-collapsed Echo"))
                );
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void introspect_reports_per_process_state_and_mailbox() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            var g = new GraphBuilder()
                    .add("Echo",
                            (SerializableSupplier<ProcessInitializer>) EchoInit::new,
                            (SerializableSupplier<ProcessLoader>) EchoInit::new)
                        .handles(String.class)
                    .build();
            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                EngineReport report = engine.introspect().toCompletableFuture().get(1, TimeUnit.SECONDS);
                assertThat(report.instanceId()).isEqualTo(engine.instanceId());
                assertThat(report.isLeader()).isTrue();
                assertThat(report.graph().nodes()).hasSize(1);
                assertThat(report.graph().nodes().get(0).name()).isEqualTo("Echo");
                assertThat(report.graph().nodes().get(0).state()).isEqualTo("Serving");
                assertThat(report.graph().nodes().get(0).sid()).isNotNull();
                assertThat(report.log().length()).isGreaterThan(0);
            }
        }
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

    static final class RecordingObserver implements EngineObserver {

        final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();

        @Override public void onStateTransition(String p, String from, String to) {
            events.add("transition " + p + " " + from + "->" + to);
        }
        @Override public void onInitStarted(String p, int attempt) {
            events.add("init-started " + p + " attempt=" + attempt);
        }
        @Override public void onInitCompleted(String p, Sid newSid, Duration duration) {
            events.add("init-completed " + p + " sid=" + newSid);
        }
        @Override public void onLoadCompleted(String p, Sid sid, Duration duration) {
            events.add("load-completed " + p + " sid=" + sid);
        }
        @Override public void onQuerySent(String p, UUID id, Class<?> type) {
            events.add("query-sent " + p + " type=" + type.getSimpleName());
        }
        @Override public void onQueryCompleted(String p, UUID id, Duration duration) {
            events.add("query-completed " + p);
        }
        @Override public void onSidPromotion(String p, Sid prev, Sid newSid) {
            events.add("sid-promotion " + p + " prev=" + prev + " new=" + newSid);
        }
        @Override public void onCleanupCompleted(String p, Sid sid, boolean ok, Duration d) {
            events.add("cleanup-completed " + p + " ok=" + ok);
        }
        @Override public void onDedupCollapsed(String p, int count) {
            events.add("dedup-collapsed " + p + " count=" + count);
        }
    }
}
