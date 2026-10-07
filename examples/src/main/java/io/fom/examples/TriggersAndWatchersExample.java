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
 * Two ways to make a process re-initialise, plus the reactive cascade:
 *
 * <ul>
 *   <li><b>Trigger</b> — an explicit push ({@code engine.trigger}).</li>
 *   <li><b>Watcher</b> — a scheduled poll that triggers on change.</li>
 *   <li><b>Cascade</b> — {@code Summary} re-inits whenever {@code Observations} does.</li>
 * </ul>
 *
 * <p>Run: {@code ./gradlew :examples:triggersWatchers}</p>
 */
public final class TriggersAndWatchersExample {

    /** Stands in for the external feed {@code Observations} is built from. */
    static final AtomicLong FEED_VERSION = new AtomicLong(1);

    static final AtomicInteger OBSERVATION_INITS = new AtomicInteger();
    static final AtomicInteger SUMMARY_INITS = new AtomicInteger();

    private static final TypedKey<Long> VERSION = new TypedKey<>("version", Codecs.longCodec());

    public static void main(String[] args) throws Exception {
        var graph = new GraphBuilder()
                .add("Observations", ObservationsInit::new, ObservationsInit::new)
                .add("Summary", SummaryInit::new, SummaryInit::new, "Observations")
                .build();

        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {

            engine.newGraph(graph);
            printInits("After cold start:    ");

            FEED_VERSION.set(2);
            engine.trigger("Observations", new Refresh("manual"));
            awaitInits(2, 2);
            printInits("After manual trigger:");

            try (var watch = engine.watch(new ScheduledWatcher<>(
                    "Observations",
                    Long.class,
                    FEED_VERSION.get(),        // the first poll compares against this
                    Duration.ZERO,
                    Duration.ofMillis(100),
                    previous -> {
                        long now = FEED_VERSION.get();
                        return now > previous ? Optional.of(now) : Optional.empty();
                    },
                    null))) {
                FEED_VERSION.set(3);
                awaitInits(3, 3);
            }
            printInits("After watcher fired: ");

            long version = (Long) engine.queryProcess("Observations", new GetVersion())
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println("Observations now serving feed version " + version);
        }
    }

    private static void printInits(String label) {
        System.out.printf("%s observation inits=%d, summary inits=%d%n",
                label, OBSERVATION_INITS.get(), SUMMARY_INITS.get());
    }

    private static void awaitInits(int observations, int summary) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (OBSERVATION_INITS.get() >= observations && SUMMARY_INITS.get() >= summary) return;
            Thread.sleep(20);
        }
    }

    record Refresh(String reason) implements Serializable { }

    record GetVersion() implements Serializable { }

    static final class ObservationsInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            OBSERVATION_INITS.incrementAndGet();
            return CompletableFuture.completedFuture(
                    Properties.empty().put(VERSION, FEED_VERSION.get()).asRaw());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            long version = Properties.of(properties).get(VERSION);
            Process live = (c, query) -> CompletableFuture.completedFuture(version);
            return CompletableFuture.completedFuture(live);
        }
    }

    static final class SummaryInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            SUMMARY_INITS.incrementAndGet();
            // A real summary would re-read Observations here via ctx.query("Observations", ...).
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            Process live = (c, query) -> CompletableFuture.completedFuture("ok");
            return CompletableFuture.completedFuture(live);
        }
    }
}
