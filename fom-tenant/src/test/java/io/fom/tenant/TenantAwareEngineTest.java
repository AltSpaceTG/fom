package io.fom.tenant;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.Graph;
import io.fom.GraphBuilder;
import io.fom.SerializableSupplier;
import io.fom.SnapshotPolicy;
import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.QueryableContext;
import io.fom.api.Routable;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantAwareEngineTest {

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
    void suffix_resolver_extracts_tenant() {
        var resolver = TenantResolver.suffixAfter("_");
        assertThat(resolver.resolve("Inventory_PUB123"))
                .contains(TenantId.of("PUB123"));
        assertThat(resolver.resolve("global-process")).isEmpty();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void authz_policy_blocks_other_tenants_queries() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = multiTenantGraph();
            try (Engine engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);

                var aware = TenantAwareEngine.builder(engine)
                        .tenantResolver(TenantResolver.suffixAfter("_"))
                        .authzPolicy((caller, tenant) -> caller.tenants().contains(tenant))
                        .build();

                var pub1Caller = TenantCaller.of("alice", TenantId.of("PUB1"));
                var pub2Caller = TenantCaller.of("bob", TenantId.of("PUB2"));

                Object ok = aware.query(pub1Caller, new GetTenantValue("PUB1"))
                        .toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(ok).isEqualTo("hello-PUB1");

                CompletionStage<Object> denied = aware.query(pub2Caller, new GetTenantValue("PUB1"));
                assertThatThrownBy(() -> denied.toCompletableFuture().get(2, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(TenantAccessDeniedException.class);
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void queryProcess_also_enforces_authz() throws Exception {
        try (LogBackend backend = new InMemoryLogBackend()) {
            Graph g = multiTenantGraph();
            try (Engine engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe(), true)) {
                engine.newGraph(g);
                var aware = TenantAwareEngine.builder(engine)
                        .authzPolicy((caller, tenant) -> caller.tenants().contains(tenant))
                        .build();

                var denied = aware.queryProcess(
                        TenantCaller.anonymous(),
                        "Inventory_PUB1",
                        new GetTenantValue("PUB1"));
                assertThatThrownBy(() -> denied.toCompletableFuture().get(2, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(TenantAccessDeniedException.class);
            }
        }
    }

    private static Graph multiTenantGraph() {
        return new GraphBuilder()
                .addWithParam("Inventory_PUB1",
                        (SerializableSupplier<ParamProcessInitializer<PubId>>) TenantInit::new,
                        (SerializableSupplier<ParamProcessLoader<PubId>>) TenantInit::new,
                        new PubId("PUB1"))
                .addWithParam("Inventory_PUB2",
                        (SerializableSupplier<ParamProcessInitializer<PubId>>) TenantInit::new,
                        (SerializableSupplier<ParamProcessLoader<PubId>>) TenantInit::new,
                        new PubId("PUB2"))
                .build();
    }

    record PubId(String id) implements Serializable { }

    record GetTenantValue(String pub) implements Routable, Serializable {
        @Override public String targetProcess() { return "Inventory_" + pub; }
    }

    static final class TenantInit implements ParamProcessInitializer<PubId>, ParamProcessLoader<PubId> {

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, PubId param) {
            return CompletableFuture.completedFuture(Map.of("pub", param.id().getBytes()));
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> properties, PubId param) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("hello-" + param.id()));
        }
    }
}
