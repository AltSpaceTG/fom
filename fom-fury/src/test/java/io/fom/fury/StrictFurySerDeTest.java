package io.fom.fury;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.api.Process;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.SerDeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class StrictFurySerDeTest {

    static final class Init implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties) {
            Process p = (c, q) -> CompletableFuture.completedFuture("x");
            return CompletableFuture.completedFuture(p);
        }
    }

    static final class ParamInit implements io.fom.api.ParamProcessInitializer<MyVal>,
            io.fom.api.ParamProcessLoader<MyVal> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, MyVal param) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties, MyVal param) {
            Process p = (c, q) -> CompletableFuture.completedFuture(param.s());
            return CompletableFuture.completedFuture(p);
        }
    }

    record MyVal(String s, List<Integer> xs) implements Serializable { }

    record Unregistered(String s) implements Serializable { }

    @Test
    void strict_engine_needs_only_param_types_registered() throws Exception {
        var serDe = FurySerDe.strict(MyVal.class);
        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, serDe)) {
            var graph = new GraphBuilder()
                    .add("P", Init::new, Init::new)
                    .addWithParam("Q", ParamInit::new, ParamInit::new, new MyVal("q", List.of(1)))
                    .build();
            assertThat(engine.newGraph(graph)).isTrue();
            // Trigger values are not serialized, so their types need no registration.
            assertThat(engine.trigger("P", new Unregistered("a"))).isTrue();
            assertThat(engine.queryProcess("Q", "q").toCompletableFuture().get(10, TimeUnit.SECONDS))
                    .isEqualTo("q");
        }
    }

    @Test
    void strict_param_round_trips() {
        var serDe = FurySerDe.strict(MyVal.class);
        var v = new MyVal("a", List.of(1, 2));
        assertThat(serDe.loadParam("P", serDe.serializeParam("P", v))).isEqualTo(v);
    }

    @Test
    void strict_refuses_unregistered_param_type() {
        var serDe = FurySerDe.strict();
        assertThatThrownBy(() -> serDe.serializeParam("P", new MyVal("p", List.of())))
                .isInstanceOf(SerDeException.class);
        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, serDe)) {
            var graph = new GraphBuilder()
                    .addWithParam("P", ParamInit::new, ParamInit::new, new MyVal("p", List.of()))
                    .build();
            assertThatThrownBy(() -> engine.newGraph(graph)).isInstanceOf(SerDeException.class);
        }
    }

    /** Stands in for a DI container: not Serializable, not registered. */
    static final class Container {
        Init make() {
            return new Init();
        }
    }

    @Test
    void factories_are_not_serialized_so_they_may_capture_anything() throws Exception {
        var container = new Container();
        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(EngineConfig.defaults(), backend, FurySerDe.strict())) {
            var graph = new GraphBuilder().add("P", container::make, container::make).build();
            assertThat(engine.newGraph(graph)).isTrue();
            assertThat(engine.queryProcess("P", "q").toCompletableFuture().get(10, TimeUnit.SECONDS))
                    .isEqualTo("x");
        }
    }
}
