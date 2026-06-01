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

    // A typed key for the one property cell we persist (type-safe vs raw byte[]).
    private static final TypedKey<String> PREFIX = new TypedKey<>("prefix", Codecs.stringCodec());

    public static void main(String[] args) throws Exception {
        var graph = new GraphBuilder()
                .add("Greeter", GreeterInit::new, GreeterInit::new)
                .handles(GetGreeting.class)   // route GetGreeting → "Greeter"
                .build();

        // InMemoryLogBackend: nothing survives the JVM — perfect for a demo.
        // JavaSerializableSerDe: fine for examples; use FurySerDe in production
        // (see FurySerdeExample). `true` = this instance is the leader at start.
        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe(), true)) {

            engine.newGraph(graph);   // runs init + load, blocks until Serving

            var greeting = (Greeting) engine.query(new GetGreeting("world"))
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);

            System.out.println(greeting.text());   // Hello, world!
        }
    }

    /** Query message. Implements Routable so it carries its own target process. */
    record GetGreeting(String who) implements Routable, Serializable {
        @Override public String targetProcess() {
            return "Greeter";
        }
    }

    record Greeting(String text) implements Serializable { }

    /** Init computes the persisted state once; load rebuilds the live process. */
    static final class GreeterInit implements ProcessInitializer, ProcessLoader {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            System.out.println("[Greeter] init — running the one-time work");
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
