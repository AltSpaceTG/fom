package io.fom.examples;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.EngineReport;
import io.fom.GraphBuilder;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.api.Routable;
import io.fom.log.InMemoryLogBackend;
import io.fom.micrometer.MicrometerEngineObserver;
import io.fom.serde.JavaSerializableSerDe;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Two complementary observability mechanisms:
 *
 * <ul>
 *   <li><b>Push</b> — a {@link MicrometerEngineObserver} records counters/timers
 *       as lifecycle events happen.</li>
 *   <li><b>Pull</b> — {@code engine.introspect()} returns a point-in-time
 *       {@link EngineReport} of every node's state.</li>
 * </ul>
 *
 * <p>Run: {@code ./gradlew :examples:observability}</p>
 */
public final class ObservabilityExample {

    public static void main(String[] args) throws Exception {
        var registry = new SimpleMeterRegistry();

        var graph = new GraphBuilder()
                .add("Echo", EchoInit::new, EchoInit::new)
                .handles(Echo.class)
                .build();

        // The observer is the 5th constructor argument.
        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, new JavaSerializableSerDe(), true,
                     new MicrometerEngineObserver(registry))) {

            engine.newGraph(graph);

            for (int i = 0; i < 25; i++) {
                engine.query(new Echo("msg-" + i)).toCompletableFuture().get(5, TimeUnit.SECONDS);
            }

            // ── Pull: introspect ──
            EngineReport report = engine.introspect().toCompletableFuture().get();
            System.out.println("Engine " + report.instanceId() + "  leader=" + report.isLeader());
            for (var node : report.graph().nodes()) {
                System.out.printf("  node %-6s state=%-8s sid=%s mailbox=%d%n",
                        node.name(), node.state(), node.sid(),
                        report.graph().mailboxSizes().getOrDefault(node.name(), 0));
            }
            System.out.println("  log: " + report.log().length() + " events, leader="
                    + report.log().currentLeader());

            // ── Push: metrics recorded by the observer ──
            System.out.println("Micrometer meters (engine_*):");
            for (Meter meter : registry.getMeters()) {
                String name = meter.getId().getName();
                if (!name.startsWith("engine")) continue;
                for (Measurement m : meter.measure()) {
                    System.out.printf("  %-42s %-12s %.4f%n",
                            name + meter.getId().getTags(), m.getStatistic(), m.getValue());
                }
            }
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
            Process live = (c, query) -> CompletableFuture.completedFuture(((Echo) query).text());
            return CompletableFuture.completedFuture(live);
        }
    }
}
