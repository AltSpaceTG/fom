package io.fom;

import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.log.LogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ProcessRef} is a typed handle whose durable identity is its name.
 * These tests prove the ref overloads build the same graph as the String API
 * (same node names, same routing, queryable by ref) and that a ref-built graph
 * is wire-compatible with a String-addressed one.
 */
class ProcessRefTest {

    private static final ProcessRef A = ProcessRef.of("A");
    private static final ProcessRef B = ProcessRef.of("B");

    private EngineConfig fastConfig() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofMillis(100), Duration.ofMillis(100),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    void ref_name_is_the_durable_identity() {
        assertThat(A.name()).isEqualTo("A");
        assertThat(ProcessRef.of("A")).isEqualTo(A);            // value semantics
        assertThat(A.toString()).isEqualTo("A");
        assertThatThrownBy(() -> ProcessRef.of("")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ref_built_graph_has_same_node_names_and_dependency_as_string_built() {
        Graph viaRef = new GraphBuilder()
                .add(A, AInit::new, AInit::new)
                .add(B, BInit::new, BInit::new, A)              // B depends reactively on A
                .build();
        Graph viaString = new GraphBuilder()
                .add("A", AInit::new, AInit::new)
                .add("B", BInit::new, BInit::new, "A")
                .build();

        assertThat(viaRef.nodes().keySet()).isEqualTo(viaString.nodes().keySet());
        var bRef = viaRef.nodes().get("B");
        var bStr = viaString.nodes().get("B");
        assertThat(bRef.dependencies()).isEqualTo(bStr.dependencies());
        assertThat(bRef.dependencies().get(0)).isInstanceOf(Dependency.Reactive.class);
        assertThat(bRef.dependencies().get(0).name()).isEqualTo("A");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void engine_queries_and_dependency_resolve_via_ref() throws Exception {
        // A built with refs; B (built with refs) queries A by ref during init.
        Graph g = new GraphBuilder()
                .add(A, AInit::new, AInit::new)
                .add(B, BInit::new, BInit::new, A)
                .build();

        try (LogBackend backend = new InMemoryLogBackend();
             var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
            engine.newGraph(g);

            // queryProcess(ProcessRef, …) reaches the node added by ref.
            Object a = engine.queryProcess(A, new Ask()).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(a).isEqualTo("A");

            // B's init did ctx.query(A, …); its served value embeds A's answer.
            Object b = engine.queryProcess(B, new Ask()).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(b).isEqualTo("B-sawdep:A");
        }
    }

    record Ask() implements Serializable { }

    static final class AInit implements ProcessInitializer, ProcessLoader {
        @Override public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }
        @Override public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("A"));
        }
    }

    static final class BInit implements ProcessInitializer, ProcessLoader {
        @Override public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            // Query the dependency by ProcessRef — the typed context overload.
            return ctx.query(A, new Ask()).thenApply(depAnswer ->
                    Map.of("dep", String.valueOf(depAnswer).getBytes()));
        }
        @Override public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> p) {
            String dep = new String(p.get("dep"));
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("B-sawdep:" + dep));
        }
    }
}
