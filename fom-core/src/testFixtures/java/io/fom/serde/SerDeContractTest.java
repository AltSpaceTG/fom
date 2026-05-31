package io.fom.serde;

import io.fom.Dependency;
import io.fom.Graph;
import io.fom.GraphBuilder;
import io.fom.ProcessNode;
import io.fom.QueryRoute;
import io.fom.SerializableSupplier;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared invariants every {@link SerDe} implementation must satisfy.
 * Subclasses supply the factory and inherit the test methods unchanged.
 *
 * <p>Lives in {@code testFixtures} so adapter modules (e.g. {@code fom-fury})
 * can run the same round-trip cases against their implementation.</p>
 */
public abstract class SerDeContractTest {

    protected abstract SerDe createSerDe();

    @Test
    void trigger_value_round_trip_for_records() {
        SerDe s = createSerDe();
        var payload = new TriggerPayload("PUB42", 7, List.of("a", "b"));
        byte[] bytes = s.serializeTriggerValue("Foo", payload);
        Object back = s.loadTriggerValue("Foo", bytes);
        assertThat(back).isEqualTo(payload);
    }

    @Test
    void trigger_value_round_trip_for_primitives() {
        SerDe s = createSerDe();
        Object[] cases = {Integer.valueOf(42), Long.valueOf(1L << 40), "hello",
                java.util.List.of(1, 2, 3)};
        for (Object value : cases) {
            byte[] bytes = s.serializeTriggerValue("Foo", value);
            assertThat(s.loadTriggerValue("Foo", bytes)).isEqualTo(value);
        }
    }

    @Test
    void process_properties_round_trip_preserves_bytes() {
        SerDe s = createSerDe();
        var props = new LinkedHashMap<String, byte[]>();
        props.put("uri", "s3://bucket/key".getBytes());
        props.put("version", new byte[]{0, 0, 0, 42});
        props.put("empty", new byte[0]);

        byte[] bytes = s.serializeProcessProperties(props);
        Map<String, byte[]> back = s.loadProcessProperties(bytes);

        assertThat(back).hasSize(3);
        assertThat(back).containsEntry("uri", "s3://bucket/key".getBytes());
        assertThat(back).containsEntry("version", new byte[]{0, 0, 0, 42});
        assertThat(back).containsEntry("empty", new byte[0]);
    }

    @Test
    void graph_round_trip_preserves_nodes_and_routing() {
        SerDe s = createSerDe();
        Graph original = new GraphBuilder()
                .add("A",
                        (SerializableSupplier<ProcessInitializer>) ContractNoOp::new,
                        (SerializableSupplier<ProcessLoader>) ContractNoOp::new)
                    .handles(ContractQuery.class)
                .add("B",
                        (SerializableSupplier<ProcessInitializer>) ContractNoOp::new,
                        (SerializableSupplier<ProcessLoader>) ContractNoOp::new, "A")
                .build();

        byte[] bytes = s.serializeProcessGraph(original);
        Graph back = s.loadProcessGraph(bytes);

        assertThat(back.nodes()).hasSize(2);
        assertThat(back.nodes()).containsKey("A");
        assertThat(back.nodes()).containsKey("B");
        ProcessNode b = back.nodes().get("B");
        assertThat(b.dependencies())
                .extracting(Dependency::name)
                .containsExactly("A");
        assertThat(back.typeRouting()).containsKey(ContractQuery.class);
        assertThat(back.typeRouting().get(ContractQuery.class)).isInstanceOf(QueryRoute.Static.class);
    }

    public record TriggerPayload(String publisherId, int version, List<String> tags) implements Serializable { }

    public record ContractQuery(String value) implements Serializable { }

    public static final class ContractNoOp implements ProcessInitializer, ProcessLoader {

        public ContractNoOp() { }

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
