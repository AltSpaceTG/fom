package io.fom.examples;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.SnapshotResult;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.LocalFileLogBackend;
import io.fom.serde.JavaSerializableSerDe;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Snapshots compact the append-only log so it doesn't grow forever. A snapshot
 * rewrites the log down to just the current live state and archives the old one.
 *
 * <p>Here we grow the log with repeated triggers, take a manual snapshot, and
 * show the log shrinking while the archive preserves the old contents.</p>
 *
 * <p>Run: {@code ./gradlew :examples:snapshots}</p>
 */
public final class SnapshotExample {

    public static void main(String[] args) throws Exception {
        Path logFile = Files.createTempFile("fom-snapshot-", ".bin");
        Files.delete(logFile);

        var graph = new GraphBuilder()
                .add("Counter", CounterInit::new, CounterInit::new)
                .build();

        try (var backend = new LocalFileLogBackend(logFile);
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe(), true)) {

            engine.newGraph(graph);

            // Grow the log: every trigger re-inits Counter and appends events.
            for (int i = 0; i < 10; i++) {
                engine.trigger("Counter", new Bump(i));
                Thread.sleep(150); // clear the dedup window so each trigger lands
            }

            int before = engine.introspect().toCompletableFuture().get().log().length();
            System.out.println("Log length before snapshot: " + before + " events");

            // Manual snapshot (the same thing a SnapshotPolicy.FixedInterval would
            // do on a schedule — see EngineConfig / fom-log-maintenance).
            SnapshotResult result = engine.snapshot().toCompletableFuture().get();
            System.out.println("Snapshot written:");
            System.out.println("  new log      = " + result.newLogId());
            System.out.println("  archived log = " + result.archivedLogId());
            System.out.println("  events copied= " + result.eventsCopied());

            int after = engine.introspect().toCompletableFuture().get().log().length();
            System.out.println("Log length after snapshot:  " + after + " events  (compacted)");

            System.out.println("Archive still on disk: " + Files.exists(Path.of(result.archivedLogId())));
        }

        // A fresh engine on the compacted file recovers from the snapshot.
        try (var backend = new LocalFileLogBackend(logFile);
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe(), true)) {
            engine.newGraph(graph);
            int len = engine.introspect().toCompletableFuture().get().log().length();
            System.out.println("Reopened from snapshot, log length = " + len + " (warm restart OK)");
        }

        // Tidy up temp files (incl. the archive).
        Path dir = logFile.getParent();
        try (var s = Files.list(dir)) {
            s.filter(p -> p.getFileName().toString().startsWith(logFile.getFileName().toString()))
                    .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) { } });
        }
    }

    record Bump(int n) implements Serializable { }

    static final class CounterInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            Process live = (c, query) -> CompletableFuture.completedFuture("ok");
            return CompletableFuture.completedFuture(live);
        }
    }
}
