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
import io.fom.api.Routable;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * The smallest useful fom program: one process, one query.
 *
 * <p>A {@code Greeter} process computes a greeting prefix once in {@code init}
 * (the expensive-but-one-time work) and serves greetings from {@code load}
 * (the cheap, every-start reconstruction). We query it by message type.</p>
 *
 * <p>Run: {@code ./gradlew :examples:quickstart}</p>
 */
public final class QuickstartExample {

    private static final TypedKey<String> PREFIX = new TypedKey<>("prefix", Codecs.stringCodec());

    public static void main(String[] args) throws Exception {
        var graph = new GraphBuilder()
                .add("Greeter", GreeterInit::new, GreeterInit::new)
                .handles(GetGreeting.class)
                .build();

        // In-memory log: nothing survives the JVM. Use FurySerDe in production (see FurySerdeExample).
        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe())) {

            engine.newGraph(graph);   // blocks until Greeter serves

            var greeting = (Greeting) engine.query(new GetGreeting("world"))
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);

            System.out.println(greeting.text());   // Hello, world!
        }
    }

    record GetGreeting(String who) implements Routable, Serializable {
        @Override public String targetProcess() {
            return "Greeter";
        }
    }

    record Greeting(String text) implements Serializable { }

    static final class GreeterInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            // The expensive one-time work goes here.
            Map<String, byte[]> cells = Properties.empty()
                    .put(PREFIX, "Hello")
                    .asRaw();
            return CompletableFuture.completedFuture(cells);
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            String prefix = Properties.of(properties).get(PREFIX);
            Process live = (c, query) -> {
                var q = (GetGreeting) query;
                return CompletableFuture.completedFuture(new Greeting(prefix + ", " + q.who() + "!"));
            };
            return CompletableFuture.completedFuture(live);
        }
    }
}
