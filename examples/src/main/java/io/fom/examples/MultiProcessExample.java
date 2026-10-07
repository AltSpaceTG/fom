package io.fom.examples;

import io.fom.Codecs;
import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.ProcessRef;
import io.fom.Properties;
import io.fom.TypedKey;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.api.Routable;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Two processes with a dependency: {@code Forecasts} queries {@code Stations}
 * during its own init and builds a forecast from the station readings.
 *
 * <pre>
 *   Stations  ──&gt;  Forecasts
 * </pre>
 *
 * <p>A node starts only after its dependencies serve, so {@code Stations} is
 * already answering when {@code Forecasts} runs init. Both are addressed through
 * {@link ProcessRef} constants, so renaming a process is a compile error rather
 * than a string hunt; the persisted identity is still {@code REF.name()}.</p>
 *
 * <p>Run: {@code ./gradlew :examples:multiProcess}</p>
 */
public final class MultiProcessExample {

    private static final TypedKey<Long> TEMPERATURE = new TypedKey<>("temperature", Codecs.longCodec());
    private static final TypedKey<String> FORECAST = new TypedKey<>("forecast", Codecs.stringCodec());

    public static void main(String[] args) throws Exception {
        var graph = new GraphBuilder()
                .add(StationsInit.REF, StationsInit::new, StationsInit::new)
                .handles(GetReadings.class)
                .add(ForecastsInit.REF, ForecastsInit::new, ForecastsInit::new, StationsInit.REF)
                .handles(GetForecast.class)
                .build();

        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {

            engine.newGraph(graph);

            long temperature = (Long) engine.query(new GetReadings("ST-1"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println("ST-1 temperature: " + temperature + " °C");

            String forecast = (String) engine.query(new GetForecast("ST-1"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println("Forecast: " + forecast);
        }
    }

    record GetReadings(String stationId) implements Routable, Serializable {
        @Override public String targetProcess() { return StationsInit.REF.name(); }
    }

    record GetForecast(String stationId) implements Routable, Serializable {
        @Override public String targetProcess() { return ForecastsInit.REF.name(); }
    }

    /** Owns the latest station readings. */
    static final class StationsInit implements ProcessInitializer, ProcessLoader {
        static final ProcessRef REF = ProcessRef.of("Stations");

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Properties.empty().put(TEMPERATURE, 14L).asRaw());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            long temperature = Properties.of(properties).get(TEMPERATURE);
            Process live = (c, query) -> CompletableFuture.completedFuture(temperature);
            return CompletableFuture.completedFuture(live);
        }
    }

    /** Builds a forecast from the readings of {@code Stations}. */
    static final class ForecastsInit implements ProcessInitializer, ProcessLoader {
        static final ProcessRef REF = ProcessRef.of("Forecasts");

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            // ctx.query only reaches declared dependencies.
            return ctx.query(StationsInit.REF, new GetReadings("ST-1"))
                    .thenApply(temperature -> {
                        String forecast = "ST-1: around " + temperature + " °C tomorrow";
                        return Properties.empty().put(FORECAST, forecast).asRaw();
                    });
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            String forecast = Properties.of(properties).get(FORECAST);
            Process live = (c, query) -> CompletableFuture.completedFuture(forecast);
            return CompletableFuture.completedFuture(live);
        }
    }
}
