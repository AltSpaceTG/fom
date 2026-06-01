package io.fom.examples;

import io.fom.Codecs;
import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.Properties;
import io.fom.ScheduledWatcher;
import io.fom.TypedKey;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Two ways to make a process re-initialise, plus the reactive cascade.
 *
 * <ul>
 *   <li><b>Trigger</b> — an explicit push ({@code engine.trigger}).</li>
 *   <li><b>Watcher</b> — a scheduled poll that triggers on change.</li>
 *   <li><b>Cascade</b> — a reactive consumer re-inits when its dependency does.</li>
 * </ul>
 *
 * <p>Run: {@code ./gradlew :examples:triggersWatchers}</p>
 */
public final class TriggersAndWatchersExample {

    /** Stands in for an external source the Catalog is built from. */
    static final AtomicLong EXTERNAL_VERSION = new AtomicLong(1);

    static final AtomicInteger CATALOG_INITS = new AtomicInteger();
    static final AtomicInteger REPORT_INITS = new AtomicInteger();

    private static final TypedKey<Long> VERSION = new TypedKey<>("version", Codecs.longCodec());

    public static void main(String[] args) throws Exception {
        var graph = new GraphBuilder()
                .add("Catalog", CatalogInit::new, CatalogInit::new)
                .add("Report", ReportInit::new, ReportInit::new, "Catalog") // reactive dependency
                .build();

        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe(), true)) {

            engine.newGraph(graph);
            System.out.printf("After cold start:        catalog inits=%d, report inits=%d%n",
                    CATALOG_INITS.get(), REPORT_INITS.get());

            // ── 1. Explicit trigger: re-init Catalog. Report cascades. ──
            EXTERNAL_VERSION.set(2);
            engine.trigger("Catalog", new Refresh("manual"));
            awaitInits(2, 2);
            System.out.printf("After manual trigger:    catalog inits=%d, report inits=%d  (Report cascaded)%n",
                    CATALOG_INITS.get(), REPORT_INITS.get());

            // ── 2. Watcher: poll EXTERNAL_VERSION; trigger Catalog when it grows. ──
            try (var ignored = engine.watch(new ScheduledWatcher<>(
                    "Catalog",
                    Long.class,
                    EXTERNAL_VERSION.get(),       // initial value the first poll compares against
                    Duration.ZERO,                 // initial delay
                    Duration.ofMillis(100),        // poll interval
                    prev -> {                      // check: emit a value to trigger with, or empty
                        long now = EXTERNAL_VERSION.get();
                        return now > prev ? Optional.of(now) : Optional.empty();
                    },
                    null))) {

                // Simulate the external source advancing.
                EXTERNAL_VERSION.set(3);
                awaitInits(3, 3);
            }
            System.out.printf("After watcher fired:     catalog inits=%d, report inits=%d%n",
                    CATALOG_INITS.get(), REPORT_INITS.get());

            long version = (Long) engine.queryProcess("Catalog", new GetVersion())
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println("Catalog now serving version = " + version);
        }
    }

    /** Poll until both counters reach the targets (or time out). */
    private static void awaitInits(int catalog, int report) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (CATALOG_INITS.get() >= catalog && REPORT_INITS.get() >= report) return;
            Thread.sleep(20);
        }
    }

    record Refresh(String reason) implements Serializable { }

    record GetVersion() implements Serializable { }

    static final class CatalogInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            CATALOG_INITS.incrementAndGet();
            long v = EXTERNAL_VERSION.get();
            return CompletableFuture.completedFuture(Properties.empty().put(VERSION, v).asRaw());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            long v = Properties.of(properties).get(VERSION);
            Process live = (c, query) -> CompletableFuture.completedFuture(v);
            return CompletableFuture.completedFuture(live);
        }
    }

    static final class ReportInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            REPORT_INITS.incrementAndGet();
            // A real report would re-read Catalog here via ctx.query("Catalog", ...).
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            Process live = (c, query) -> CompletableFuture.completedFuture("ok");
            return CompletableFuture.completedFuture(live);
        }
    }
}
