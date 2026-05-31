package io.fom;

import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the graph-validation invariants enforced by {@link Graph}'s
 * compact constructor and the {@link GraphBuilder} ergonomic API.
 */
class GraphTest {

    @Test
    void empty_builder_fails_fast() {
        assertThatThrownBy(() -> new GraphBuilder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void handles_before_any_add_fails() {
        assertThatThrownBy(() -> new GraphBuilder().handles(String.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before any add");
    }

    @Test
    void duplicate_handles_for_same_type_fails() {
        assertThatThrownBy(() -> new GraphBuilder()
                .add("A", noOp(), noOp())
                    .handles(String.class)
                .add("B", noOp(), noOp())
                    .handles(String.class)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already routed");
    }

    @Test
    void duplicate_node_name_fails() {
        assertThatThrownBy(() -> new GraphBuilder()
                .add("A", noOp(), noOp())
                .add("A", noOp(), noOp())
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate process name");
    }

    @Test
    void missing_dependency_fails_in_graph_constructor() {
        assertThatThrownBy(() -> new GraphBuilder()
                .add("A", noOp(), noOp(), "Ghost")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing 'Ghost'");
    }

    @Test
    void cycle_detected() {
        // GraphBuilder cannot natively produce a cycle (each add references earlier names),
        // but we can synthesise one by constructing ProcessNodes directly.
        var a = new ProcessNode("A", java.util.List.of(Dependency.reactive("B")),
                null, noOp(), noOp());
        var b = new ProcessNode("B", java.util.List.of(Dependency.reactive("A")),
                null, noOp(), noOp());
        var nodes = Map.of("A", a, "B", b);
        assertThatThrownBy(() -> new Graph(a, nodes, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cycle detected");
    }

    @Test
    void static_route_to_unknown_process_fails() {
        var a = new ProcessNode("A", java.util.List.of(), null, noOp(), noOp());
        var nodes = Map.of("A", a);
        Map<Class<?>, QueryRoute> routing = Map.of(String.class, new QueryRoute.Static("Ghost"));
        assertThatThrownBy(() -> new Graph(a, nodes, routing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not in nodes");
    }

    @Test
    void top_not_in_nodes_fails() {
        var a = new ProcessNode("A", java.util.List.of(), null, noOp(), noOp());
        var b = new ProcessNode("B", java.util.List.of(), null, noOp(), noOp());
        assertThatThrownBy(() -> new Graph(a, Map.of("B", b), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not present in nodes");
    }

    @Test
    void topological_order_dependencies_first() {
        Graph g = new GraphBuilder()
                .add("A", noOp(), noOp())
                .add("B", noOp(), noOp(), "A")
                .add("C", noOp(), noOp(), "B")
                .build();
        var order = g.topologicalOrder().stream().map(ProcessNode::name).toList();
        assertThat(order.indexOf("A")).isLessThan(order.indexOf("B"));
        assertThat(order.indexOf("B")).isLessThan(order.indexOf("C"));
    }

    @Test
    void diamond_dependencies_allowed() {
        // A → B → D, A → C → D (D depends on B and C; B and C depend on A)
        Graph g = new GraphBuilder()
                .add("A", noOp(), noOp())
                .add("B", noOp(), noOp(), "A")
                .add("C", noOp(), noOp(), "A")
                .add("D", noOp(), noOp(), "B", "C")
                .build();
        var order = g.topologicalOrder().stream().map(ProcessNode::name).toList();
        assertThat(order.indexOf("A")).isLessThan(order.indexOf("B"));
        assertThat(order.indexOf("A")).isLessThan(order.indexOf("C"));
        assertThat(order.indexOf("B")).isLessThan(order.indexOf("D"));
        assertThat(order.indexOf("C")).isLessThan(order.indexOf("D"));
    }

    // ───────────────── helpers ─────────────────

    @SuppressWarnings("unchecked")
    private static <T> SerializableSupplier<T> noOp() {
        return (SerializableSupplier<T>) (SerializableSupplier<NoOp>) NoOp::new;
    }

    static final class NoOp implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }
        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture(q));
        }
    }
}
