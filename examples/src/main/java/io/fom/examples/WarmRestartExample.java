package io.fom.examples;

import io.fom.Codecs;
import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.Properties;
import io.fom.TypedKey;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.FileLogBackend;
import io.fom.serde.JavaSerializableSerDe;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Idempotent restart: the headline guarantee. The expensive {@code init} runs
 * once; restarting the JVM against the same log recovers the state by running
 * only the cheap {@code load}.
 *
 * <p>We simulate two JVM runs by opening two engines against the same
 * {@link FileLogBackend} file, and count how many times {@code init} ran.</p>
 *
 * <p>Run: {@code ./gradlew :examples:warmRestart}</p>
 */
public final class WarmRestartExample {

    static final AtomicInteger INIT_CALLS = new AtomicInteger();

    private static final TypedKey<Long> BUILT_AT = new TypedKey<>("builtAt", Codecs.longCodec());

    public static void main(String[] args) throws Exception {
        Path logFile = Files.createTempFile("fom-warm-restart-", ".bin");
        Files.delete(logFile);   // the backend wants to create it itself

        var graph = new GraphBuilder()
                .add("Index", IndexInit::new, IndexInit::new)
                .handles(GetBuiltAt.class)
                .build();

        System.out.println("First start (log is empty → cold init):");
        try (var backend = new FileLogBackend(logFile);
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph);
            long builtAt = (Long) engine.query(new GetBuiltAt())
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println("  built at = " + builtAt + ", init calls so far = " + INIT_CALLS.get());
        }

        System.out.println("Second start (same log file → warm load, init skipped):");
        try (var backend = new FileLogBackend(logFile);
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(graph);
            long builtAt = (Long) engine.query(new GetBuiltAt())
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println("  built at = " + builtAt + " (same value — state was recovered, not rebuilt)");
        }

        System.out.println();
        System.out.println("Total init executions across both runs: " + INIT_CALLS.get() + "  (expected: 1)");

        Files.deleteIfExists(logFile);
        Files.deleteIfExists(logFile.resolveSibling(logFile.getFileName() + ".lock"));
    }

    record GetBuiltAt() implements Serializable { }

    static final class IndexInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            INIT_CALLS.incrementAndGet();
            // Stands in for a heavy build. The timestamp lets the second run show it reused this state.
            long builtAt = System.currentTimeMillis();
            Map<String, byte[]> cells = Properties.empty().put(BUILT_AT, builtAt).asRaw();
            return CompletableFuture.completedFuture(cells);
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            long builtAt = Properties.of(properties).get(BUILT_AT);
            Process live = (c, query) -> CompletableFuture.completedFuture(builtAt);
            return CompletableFuture.completedFuture(live);
        }
    }
}
