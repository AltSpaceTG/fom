package io.fom.examples;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.api.Routable;
import io.fom.fury.FurySerDe;
import io.fom.log.InMemoryLogBackend;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Same shape as the quickstart, but with {@link FurySerDe} — the compact, fast,
 * schema-evolution-friendly serializer recommended for production. You construct
 * one and pass it in place of {@code JavaSerializableSerDe}; everything else is
 * identical.
 *
 * <p>For untrusted log storage, harden it with {@code new FurySerDe(true)}
 * (class registration on) — see docs/guides/security.md.</p>
 *
 * <p>Run: {@code ./gradlew :examples:furySerde}</p>
 */
public final class FurySerdeExample {

    public static void main(String[] args) throws Exception {
        var graph = new GraphBuilder()
                .add("Echo", EchoInit::new, EchoInit::new)
                .handles(Echo.class)
                .build();

        // The only change from the quickstart: FurySerDe instead of Java serialization.
        var serDe = new FurySerDe();

        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, serDe, true)) {

            engine.newGraph(graph);

            var reply = engine.query(new Echo("encoded with Apache Fury"))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            System.out.println(reply);
        }
    }

    record Echo(String text) implements Routable, Serializable {
        @Override public String targetProcess() { return "Echo"; }
    }

    static final class EchoInit implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            Process live = (c, query) -> CompletableFuture.completedFuture("echo: " + ((Echo) query).text());
            return CompletableFuture.completedFuture(live);
        }
    }
}
