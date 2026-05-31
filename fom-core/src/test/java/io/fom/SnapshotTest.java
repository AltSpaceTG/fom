package io.fom;

import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LocalFileLogBackend;
import io.fom.log.LogBackend;
import io.fom.log.LogChangeGraph;
import io.fom.log.LogEvent;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import io.fom.log.LogSnapshot;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Stage 3 integration: {@link Engine#snapshot()} works against both backends,
 * scheduled rotation fires automatically, archived files survive, and warm
 * restart works after a snapshot.
 */
class SnapshotTest {

    private EngineConfig fastConfig(SnapshotPolicy policy) {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofMillis(100), Duration.ofMillis(100),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                policy);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void manual_snapshot_in_memory_collapses_log() throws Exception {
        Counter.reset();
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = chainGraph();
            try (var engine = new Engine(fastConfig(SnapshotPolicy.Disabled.INSTANCE), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                engine.query("ping").toCompletableFuture().get(2, TimeUnit.SECONDS);

                int beforeLen = backend.length();
                assertThat(beforeLen).isGreaterThanOrEqualTo(4); // Leader + ChangeGraph + Initialized + Loaded

                SnapshotResult result = engine.snapshot().toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(result.eventsCopied()).isEqualTo(4); // Leader + Snapshot + ChangeGraph + 1 Initialized
                assertThat(result.checkpointClock()).isEqualTo(beforeLen - 1);
                assertThat(backend.length()).isEqualTo(4);
                assertThat(backend.get(0)).isInstanceOf(LogLeader.class);
                assertThat(backend.get(1)).isInstanceOf(LogSnapshot.class);
                assertThat(backend.get(2)).isInstanceOf(LogChangeGraph.class);
                assertThat(backend.get(3)).isInstanceOf(LogInitialized.class);

                // Engine continues to serve after snapshot.
                Object r = engine.query("pong").toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(r).isEqualTo("echo:pong");
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void local_file_snapshot_creates_archived_and_warm_restart_works(@TempDir Path tmp) throws Exception {
        Counter.reset();
        Path logPath = tmp.resolve("fom.log");

        try (LogBackend backend = new LocalFileLogBackend(logPath)) {
            Graph g = chainGraph();
            try (var engine = new Engine(fastConfig(SnapshotPolicy.Disabled.INSTANCE), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                engine.query("ping").toCompletableFuture().get(2, TimeUnit.SECONDS);
                SnapshotResult result = engine.snapshot().toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(result.archivedLogId()).contains(".archived.");
                assertThat(Files.exists(Path.of(result.archivedLogId()))).isTrue();
                assertThat(backend.length()).isEqualTo(4);
            }
        }

        // Warm restart on the rotated file — Engine should warm-load from the snapshot.
        int initBefore = Counter.INIT.get();
        try (LogBackend reopened = new LocalFileLogBackend(logPath)) {
            try (var engine2 = new Engine(fastConfig(SnapshotPolicy.Disabled.INSTANCE), reopened, new JavaSerializableSerDe(), true)) {
                engine2.newGraph(chainGraph());
                Object r = engine2.query("after-restart").toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(r).isEqualTo("echo:after-restart");
            }
        }
        assertThat(Counter.INIT).hasValue(initBefore); // warm load, no fresh init
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void scheduled_snapshot_fires_and_purges_old_archives(@TempDir Path tmp) throws Exception {
        Counter.reset();
        Path logPath = tmp.resolve("fom.log");
        var policy = new SnapshotPolicy.FixedInterval(Duration.ofMillis(200), 2);

        try (LogBackend backend = new LocalFileLogBackend(logPath)) {
            Graph g = chainGraph();
            try (var engine = new Engine(fastConfig(policy), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                engine.query("ping").toCompletableFuture().get(2, TimeUnit.SECONDS);
                Thread.sleep(900); // expect ~4 firings
            }
        }
        try (Stream<Path> stream = Files.list(tmp)) {
            long archives = stream.filter(p -> p.getFileName().toString().contains(".archived.")).count();
            assertThat(archives).isLessThanOrEqualTo(2); // keepHistory cap
            assertThat(archives).isGreaterThanOrEqualTo(1); // at least one fired
        }
    }

    @Test
    void snapshot_without_graph_fails() throws IOException {
        try (LogBackend backend = new InMemoryLogBackend()) {
            try (var engine = new Engine(fastConfig(SnapshotPolicy.Disabled.INSTANCE), backend, new JavaSerializableSerDe(), true)) {
                var stage = engine.snapshot();
                assertThatThrownBy(() -> stage.toCompletableFuture().get(1, TimeUnit.SECONDS))
                        .hasMessageContaining("No graph installed");
            }
        }
    }

    // ───────────────── fixture ─────────────────

    private static Graph chainGraph() {
        return new GraphBuilder()
                .add("Echo",
                        (SerializableSupplier<ProcessInitializer>) Counter.EchoInit::new,
                        (SerializableSupplier<ProcessLoader>) Counter.EchoInit::new)
                .handles(String.class)
                .build();
    }

    static final class Counter {

        static final AtomicInteger INIT = new AtomicInteger();
        static final AtomicInteger LOAD = new AtomicInteger();

        static void reset() {
            INIT.set(0);
            LOAD.set(0);
        }

        static final class EchoInit implements ProcessInitializer, ProcessLoader {

            @Override
            public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
                INIT.incrementAndGet();
                return CompletableFuture.completedFuture(Map.of("k", new byte[]{1}));
            }

            @Override
            public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
                LOAD.incrementAndGet();
                return CompletableFuture.completedFuture(
                        (c, q) -> CompletableFuture.completedFuture("echo:" + q));
            }
        }
    }
}
