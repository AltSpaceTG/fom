package io.fom;

import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.QueryableContext;
import io.fom.log.FileLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Offline compaction ({@link LogCompaction#compact}) keeps exactly what a restart needs. */
class LogCompactionTest {

    record Mode(String value) implements Serializable { }

    static final AtomicInteger INITS = new AtomicInteger();
    static final AtomicBoolean FAIL_INIT = new AtomicBoolean();

    /** Persists "value#initCount"; serves what it loaded. */
    static final class ModeInit implements ParamProcessInitializer<Mode>, ParamProcessLoader<Mode> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, Mode mode) {
            if (FAIL_INIT.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("init fails for now"));
            }
            String state = mode.value() + "#" + INITS.incrementAndGet();
            return CompletableFuture.completedFuture(Map.of("state", state.getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, Mode mode) {
            String state = new String(props.get("state"), StandardCharsets.UTF_8);
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(state));
        }
    }

    @BeforeEach
    void reset() {
        INITS.set(0);
        FAIL_INIT.set(false);
    }

    private static EngineConfig cfg() {
        return new EngineConfig(
                Duration.ofMillis(500), Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    /** A dedup window long enough that a trigger is still pending when the engine closes. */
    private static EngineConfig slowDedup() {
        return new EngineConfig(
                Duration.ofMillis(500), Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofMinutes(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    @Timeout(30)
    void a_trigger_not_applied_before_shutdown_is_replayed_once_after_restart(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("fom.log");
        try (var backend = new FileLogBackend(file);
             var engine = new Engine(slowDedup(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph("v1"));
            engine.trigger("A", "refresh");   // still waiting in the dedup window at close
        }
        assertThat(INITS).hasValue(1);

        try (var backend = new FileLogBackend(file);
             var engine = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph("v1"));
            // The replayed re-init briefly rejects queries while A cleans up: keep polling.
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "v1#2".equals(queryA(engine)));
        }
        try (var backend = new FileLogBackend(file);
             var engine = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph("v1"));
            assertThat(queryA(engine)).isEqualTo("v1#2");
        }
        assertThat(INITS).as("replayed exactly once").hasValue(2);
    }

    @Test
    @Timeout(30)
    void a_pending_trigger_survives_offline_compaction(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("fom.log");
        try (var backend = new FileLogBackend(file);
             var engine = new Engine(slowDedup(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph("v1"));
            engine.trigger("A", "refresh");
        }
        try (var backend = new FileLogBackend(file)) {
            LogCompaction.compact(backend);
        }
        try (var backend = new FileLogBackend(file);
             var engine = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph("v1"));
            // The replayed re-init briefly rejects queries while A cleans up: keep polling.
            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().until(() -> "v1#2".equals(queryA(engine)));
        }
    }

    private static Graph graph(String mode) {
        return new GraphBuilder().addWithParam("A", ModeInit::new, ModeInit::new, new Mode(mode)).build();
    }

    private static Object queryA(Engine engine) throws Exception {
        return engine.queryProcess("A", "q").toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(30)
    void offline_compaction_keeps_live_state_for_a_warm_restart(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("fom.log");
        try (var backend = new FileLogBackend(file);
             var engine = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph("v1"));
            engine.pause(java.util.List.of("A"));
            engine.resume(java.util.List.of("A"));
            for (int i = 2; i <= 4; i++) {
                engine.trigger("A", "refresh");
                String expected = "v1#" + i;
                await().atMost(Duration.ofSeconds(5)).until(() -> expected.equals(queryA(engine)));
            }
        }
        int lengthBefore;
        try (var backend = new FileLogBackend(file)) {
            lengthBefore = backend.length();
            LogCompaction.compact(backend);
            assertThat(backend.length()).isLessThan(lengthBefore).isEqualTo(4);
        }
        try (var backend = new FileLogBackend(file);
             var engine = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph("v1"));
            assertThat(queryA(engine)).isEqualTo("v1#4");
        }
        assertThat(INITS).as("the restart after compaction warm-loads").hasValue(4);
    }

    @Test
    @Timeout(30)
    void state_written_under_an_older_definition_is_not_passed_off_as_current(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("fom.log");
        try (var backend = new FileLogBackend(file);
             var engine = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph("v1"));
        }
        // Restart with a changed param whose cold init fails: the log now has
        // the v2 graph after the still-live v1 state.
        FAIL_INIT.set(true);
        try (var backend = new FileLogBackend(file);
             var engine = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            assertThatThrownBy(() -> engine.newGraph(graph("v2"))).isInstanceOf(RuntimeException.class);
        }
        try (var backend = new FileLogBackend(file)) {
            LogCompaction.compact(backend);
        }
        FAIL_INIT.set(false);
        try (var backend = new FileLogBackend(file);
             var engine = new Engine(cfg(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph("v2"));
            assertThat(queryA(engine)).as("v1 state must not be warm-loaded under v2").isEqualTo("v2#2");
        }
    }
}
