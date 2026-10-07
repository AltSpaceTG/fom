package io.fom.tenant;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.Graph;
import io.fom.GraphBuilder;
import io.fom.ProcessRef;
import io.fom.SnapshotPolicy;
import io.fom.api.ParamProcessInitializer;
import io.fom.api.ParamProcessLoader;
import io.fom.api.Process;
import io.fom.api.QueryRejectedException;
import io.fom.api.QueryableContext;
import io.fom.api.Routable;
import io.fom.log.InMemoryLogBackend;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.function.Supplier;

class TenantAwareEngineTest {

    private static final TenantCaller ALICE = TenantCaller.of("alice", TenantId.of("PUB1"));
    private static final TenantCaller BOB = TenantCaller.of("bob", TenantId.of("PUB2"));

    private static EngineConfig fastConfig() {
        return new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofMillis(100),
                Duration.ofMillis(10), Duration.ofMillis(100), 1,
                SnapshotPolicy.Disabled.INSTANCE);
    }

    private static Engine engine(Graph g) {
        var engine = new Engine(fastConfig(), new InMemoryLogBackend(), new JavaSerializableSerDe());
        engine.newGraph(g);
        return engine;
    }

    private static TenantAwareEngine.Builder ownTenantsOnly(Engine engine) {
        return TenantAwareEngine.builder(engine)
                .authzPolicy((caller, tenant) -> caller.tenants().contains(tenant));
    }

    private static Object get(CompletionStage<Object> stage) throws Exception {
        return stage.toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    private static void assertDenied(CompletionStage<Object> stage) {
        assertThatThrownBy(() -> get(stage))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TenantAccessDeniedException.class);
    }

    // ───────────────── resolver ─────────────────

    @Test
    void strict_suffix_resolver_never_guesses() {
        var resolver = TenantResolver.suffixAfter("_");
        assertThat(resolver.resolve("Inventory_PUB123")).contains(TenantId.of("PUB123"));
        assertThat(resolver.resolve("global-process")).isEmpty();
        assertThat(resolver.resolve("Inventory_ACME_EU")).isEmpty();
        assertThat(resolver.resolve("_PUB1")).isEmpty();
        assertThat(resolver.resolve("Inventory_")).isEmpty();
        assertThat(resolver.resolve("Inventory__PUB1")).isEmpty();
    }

    @Test
    void registry_and_regex_resolvers() {
        var registry = TenantResolver.registry(Map.of("Inventory_ACME_EU", TenantId.of("ACME_EU")));
        assertThat(registry.resolve("Inventory_ACME_EU")).contains(TenantId.of("ACME_EU"));
        assertThat(registry.resolve("Other")).isEmpty();

        var regex = TenantResolver.regex(Pattern.compile("Inventory@(.+)"));
        assertThat(regex.resolve("Inventory@ACME_EU")).contains(TenantId.of("ACME_EU"));
        assertThat(regex.resolve("Inventory_ACME_EU")).isEmpty();
    }

    // ───────────────── access decisions ─────────────────

    @Test
    @Timeout(10)
    void callers_reach_only_their_own_tenants() throws Exception {
        try (var engine = engine(tenantGraph("Inventory_PUB1", "Inventory_PUB2"))) {
            var aware = ownTenantsOnly(engine).build();

            assertThat(get(aware.query(ALICE, new GetInventory("Inventory_PUB1")))).isEqualTo("hello-Inventory_PUB1");
            assertDenied(aware.query(BOB, new GetInventory("Inventory_PUB1")));
            assertDenied(aware.queryProcess(TenantCaller.anonymous(), "Inventory_PUB1", "q"));
            assertThatThrownBy(() -> aware.trigger(BOB, "Inventory_PUB1", "x"))
                    .isInstanceOf(TenantAccessDeniedException.class);
        }
    }

    @Test
    @Timeout(10)
    void default_policy_denies_everyone() throws Exception {
        try (var engine = engine(tenantGraph("Inventory_PUB1"))) {
            var aware = TenantAwareEngine.builder(engine).build();
            assertDenied(aware.queryProcess(ALICE, "Inventory_PUB1", "q"));
        }
    }

    @Test
    @Timeout(10)
    void ambiguous_name_is_denied_instead_of_mapped_to_another_tenant() throws Exception {
        try (var engine = engine(tenantGraph("Inventory_ACME_EU"))) {
            var aware = ownTenantsOnly(engine).build();
            // The old lastIndexOf resolver handed this process to tenant "EU".
            assertDenied(aware.queryProcess(TenantCaller.of("eu", TenantId.of("EU")), "Inventory_ACME_EU", "q"));
            assertDenied(aware.queryProcess(TenantCaller.of("acme", TenantId.of("ACME_EU")), "Inventory_ACME_EU", "q"));

            var explicit = ownTenantsOnly(engine)
                    .tenantResolver(TenantResolver.registry(Map.of("Inventory_ACME_EU", TenantId.of("ACME_EU"))))
                    .build();
            assertThat(get(explicit.queryProcess(TenantCaller.of("acme", TenantId.of("ACME_EU")),
                    "Inventory_ACME_EU", "q"))).isEqualTo("hello-Inventory_ACME_EU");
        }
    }

    @Test
    @Timeout(10)
    void process_without_a_tenant_is_denied_unless_declared_global() throws Exception {
        try (var engine = engine(tenantGraph("Rates", "Inventory_PUB1"))) {
            assertDenied(ownTenantsOnly(engine).build().queryProcess(TenantCaller.anonymous(), "Rates", "q"));

            var withGlobal = ownTenantsOnly(engine).globalProcesses("Rates").build();
            assertThat(get(withGlobal.queryProcess(TenantCaller.anonymous(), "Rates", "q"))).isEqualTo("hello-Rates");
            assertDenied(withGlobal.queryProcess(TenantCaller.anonymous(), "Inventory_PUB1", "q"));
        }
    }

    @Test
    @Timeout(10)
    void triggering_a_global_process_needs_the_global_trigger_policy() {
        try (var engine = engine(tenantGraph("Rates", "Inventory_PUB1"))) {
            var aware = ownTenantsOnly(engine).globalProcesses("Rates").build();
            assertThatThrownBy(() -> aware.trigger(TenantCaller.anonymous(), "Rates", "x"))
                    .isInstanceOf(TenantAccessDeniedException.class);
            assertThatThrownBy(() -> aware.trigger(ALICE, "Rates", "x"))
                    .isInstanceOf(TenantAccessDeniedException.class);

            var admin = ownTenantsOnly(engine).globalProcesses("Rates")
                    .globalTriggerPolicy(caller -> caller == ALICE).build();
            assertThat(admin.trigger(ALICE, "Rates", "x")).isTrue();
            assertThatThrownBy(() -> admin.trigger(BOB, "Rates", "x"))
                    .isInstanceOf(TenantAccessDeniedException.class);
        }
    }

    @Test
    @Timeout(10)
    void routable_target_is_read_once_so_it_cannot_switch_after_the_check() throws Exception {
        try (var engine = engine(tenantGraph("Inventory_PUB1", "Inventory_PUB2"))) {
            var aware = ownTenantsOnly(engine).build();
            assertThat(get(aware.query(ALICE, new ShapeShifter()))).isEqualTo("hello-Inventory_PUB1");
        }
    }

    @Test
    @Timeout(10)
    void non_routable_query_is_rejected() {
        try (var engine = engine(tenantGraph("Inventory_PUB1"))) {
            assertDenied(ownTenantsOnly(engine).build().query(ALICE, "plain message"));
        }
    }

    // ───────────────── timeout and ProcessRef overloads ─────────────────

    @Test
    @Timeout(10)
    void timeout_and_process_ref_overloads_are_authorized_like_the_plain_calls() throws Exception {
        try (var engine = engine(tenantGraph("Inventory_PUB1", "Inventory_PUB2"))) {
            var aware = ownTenantsOnly(engine).build();
            var timeout = Duration.ofSeconds(3);
            var pub1 = ProcessRef.of("Inventory_PUB1");

            assertThat(get(aware.query(ALICE, new GetInventory("Inventory_PUB1"), timeout)))
                    .isEqualTo("hello-Inventory_PUB1");
            assertThat(get(aware.queryProcess(ALICE, "Inventory_PUB1", "q", timeout)))
                    .isEqualTo("hello-Inventory_PUB1");
            assertThat(get(aware.queryProcess(ALICE, pub1, "q"))).isEqualTo("hello-Inventory_PUB1");
            assertThat(get(aware.queryProcess(ALICE, pub1, "q", timeout))).isEqualTo("hello-Inventory_PUB1");
            assertThat(aware.trigger(ALICE, pub1, "x")).isTrue();

            assertDenied(aware.query(BOB, new GetInventory("Inventory_PUB1"), timeout));
            assertDenied(aware.queryProcess(BOB, "Inventory_PUB1", "q", timeout));
            assertDenied(aware.queryProcess(BOB, pub1, "q"));
            assertDenied(aware.queryProcess(BOB, pub1, "q", timeout));
            assertThatThrownBy(() -> aware.trigger(BOB, pub1, "x"))
                    .isInstanceOf(TenantAccessDeniedException.class);

            // a non-Routable message is refused by the timeout overload too
            assertDenied(aware.query(ALICE, "plain message", timeout));
        }
    }

    @Test
    @Timeout(10)
    void a_per_query_timeout_is_honoured() {
        Graph slow = new GraphBuilder()
                .add("Slow_PUB1", SlowProcess::new, SlowProcess::new)
                .build();
        try (var engine = engine(slow)) {
            var aware = ownTenantsOnly(engine).build();
            assertThatThrownBy(() -> get(aware.queryProcess(ALICE, "Slow_PUB1", "q", Duration.ofMillis(50))))
                    .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
        }
    }

    @Test
    @Timeout(10)
    void a_failing_authz_policy_fails_the_stage_for_the_overloads_too() {
        try (var engine = engine(tenantGraph("Inventory_PUB1"))) {
            var aware = TenantAwareEngine.builder(engine)
                    .authzPolicy((caller, tenant) -> { throw new IllegalStateException("authz service down"); })
                    .build();
            assertThatThrownBy(() -> get(aware.queryProcess(ALICE, ProcessRef.of("Inventory_PUB1"), "q")))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> get(aware.query(ALICE, new GetInventory("Inventory_PUB1"),
                    Duration.ofSeconds(1)))).hasRootCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    @Timeout(10)
    void triggering_a_global_process_by_ref_still_needs_the_global_trigger_policy() {
        try (var engine = engine(tenantGraph("Rates", "Inventory_PUB1"))) {
            var aware = ownTenantsOnly(engine).globalProcesses("Rates").build();
            assertThatThrownBy(() -> aware.trigger(ALICE, ProcessRef.of("Rates"), "x"))
                    .isInstanceOf(TenantAccessDeniedException.class);
        }
    }

    // ───────────────── processesOf ─────────────────

    @Test
    @Timeout(10)
    void processes_of_lists_only_the_callers_own_tenant_and_changes_nothing() throws Exception {
        try (var engine = engine(tenantGraph("Rates", "Inventory_PUB1", "Orders_PUB1", "Inventory_PUB2"))) {
            var aware = ownTenantsOnly(engine).globalProcesses("Rates").build();

            assertThat(aware.processesOf(ALICE, TenantId.of("PUB1")))
                    .containsExactlyInAnyOrder("Inventory_PUB1", "Orders_PUB1");
            assertThat(aware.processesOf(BOB, TenantId.of("PUB2"))).containsExactly("Inventory_PUB2");

            // the graph is untouched and the processes still serve
            assertThat(engine.currentGraph().nodes())
                    .containsOnlyKeys("Rates", "Inventory_PUB1", "Orders_PUB1", "Inventory_PUB2");
            assertThat(get(aware.queryProcess(ALICE, "Inventory_PUB1", "q"))).isEqualTo("hello-Inventory_PUB1");
        }
    }

    @Test
    @Timeout(10)
    void a_batch_trigger_authorises_every_name_before_recording_any() throws Exception {
        try (var engine = engine(tenantGraph("Rates", "Inventory_PUB1", "Orders_PUB1", "Inventory_PUB2"))) {
            var aware = ownTenantsOnly(engine).globalProcesses("Rates").build();

            assertThat(aware.trigger(ALICE, java.util.Map.of(
                    "Inventory_PUB1", "v2", "Orders_PUB1", "v2"))).isTrue();

            // one foreign name is enough to refuse the whole batch, and nothing is recorded
            int triggersBefore = triggerRecords(engine);
            assertThatThrownBy(() -> aware.trigger(ALICE, java.util.Map.of(
                    "Inventory_PUB1", "v3", "Inventory_PUB2", "v3")))
                    .isInstanceOf(TenantAccessDeniedException.class);
            assertThat(triggerRecords(engine)).as("a denied batch writes no LogTrigger at all")
                    .isEqualTo(triggersBefore);
            // so is a global one, without globalTriggerPolicy
            assertThatThrownBy(() -> aware.trigger(ALICE, java.util.Map.of(
                    "Inventory_PUB1", "v3", "Rates", "v3")))
                    .isInstanceOf(TenantAccessDeniedException.class)
                    .hasMessageContaining("global process 'Rates'");
        }
    }

    private static int triggerRecords(Engine engine) throws Exception {
        return engine.introspect().toCompletableFuture().get()
                .log().eventCounts().getOrDefault("LogTrigger", 0);
    }

    /** Engine.query fails the stage for a throwing route resolution; the wrapper must match it. */
    @Test
    @Timeout(10)
    void a_routable_that_throws_or_names_nothing_fails_the_stage() {
        try (var engine = engine(tenantGraph("Inventory_PUB1"))) {
            var aware = ownTenantsOnly(engine).build();

            assertThatThrownBy(() -> get(aware.query(ALICE, new ThrowingRoutable())))
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("routing blew up");
            assertThatThrownBy(() -> get(aware.query(ALICE, new NamelessRoutable())))
                    .hasRootCauseInstanceOf(io.fom.api.QueryException.class)
                    .hasMessageContaining("returned no target process");
        }
    }

    record ThrowingRoutable() implements io.fom.api.Routable, java.io.Serializable {
        @Override
        public String targetProcess() {
            throw new IllegalStateException("routing blew up");
        }
    }

    record NamelessRoutable() implements io.fom.api.Routable, java.io.Serializable {
        @Override
        public String targetProcess() {
            return "";
        }
    }

    /** A closed engine must not tell an unauthorised caller more than an open one does. */
    @Test
    @Timeout(10)
    void a_closed_engine_still_denies_an_unauthorised_caller_first() {
        var engine = engine(tenantGraph("Inventory_PUB1", "Inventory_PUB2"));
        var aware = ownTenantsOnly(engine).build();
        engine.close();

        assertThatThrownBy(() -> aware.processesOf(BOB, TenantId.of("PUB1")))
                .isInstanceOf(TenantAccessDeniedException.class);
        assertThatThrownBy(() -> aware.removeTenant(BOB, TenantId.of("PUB1")))
                .isInstanceOf(TenantAccessDeniedException.class);
        // an authorised caller sees the engine's own refusal
        assertThatThrownBy(() -> aware.processesOf(ALICE, TenantId.of("PUB1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is closed");
    }

    @Test
    @Timeout(10)
    void processes_of_cannot_probe_another_tenant() {
        try (var engine = engine(tenantGraph("Inventory_PUB1", "Inventory_PUB2"))) {
            var aware = ownTenantsOnly(engine).build();
            assertThatThrownBy(() -> aware.processesOf(BOB, TenantId.of("PUB1")))
                    .isInstanceOf(TenantAccessDeniedException.class)
                    .hasMessageContaining("cannot manage tenant");
            assertThatThrownBy(() -> aware.processesOf(TenantCaller.anonymous(), TenantId.of("PUB1")))
                    .isInstanceOf(TenantAccessDeniedException.class);
            // the default policy denies everyone
            assertThatThrownBy(() -> TenantAwareEngine.builder(engine).build()
                    .processesOf(ALICE, TenantId.of("PUB1")))
                    .isInstanceOf(TenantAccessDeniedException.class);
        }
    }

    @Test
    @Timeout(10)
    void processes_of_is_an_unmodifiable_empty_set_for_a_tenant_without_processes() {
        try (var engine = engine(tenantGraph("Inventory_PUB1"))) {
            var aware = ownTenantsOnly(engine).build();
            var names = aware.processesOf(BOB, TenantId.of("PUB2"));
            assertThat(names).isEmpty();
            assertThatThrownBy(() -> names.add("x")).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    // ───────────────── tenant lifecycle ─────────────────

    @Test
    @Timeout(30)
    void pause_resume_and_remove_a_tenant() throws Exception {
        try (var engine = engine(tenantGraph("Rates", "Inventory_PUB1", "Orders_PUB1", "Inventory_PUB2"))) {
            var aware = ownTenantsOnly(engine).globalProcesses("Rates").build();

            assertThat(aware.pauseTenant(ALICE, TenantId.of("PUB1")))
                    .containsExactlyInAnyOrder("Inventory_PUB1", "Orders_PUB1");
            assertThatThrownBy(() -> get(aware.queryProcess(ALICE, "Inventory_PUB1", "q")))
                    .hasCauseInstanceOf(QueryRejectedException.class);
            assertThat(get(aware.queryProcess(BOB, "Inventory_PUB2", "q"))).isEqualTo("hello-Inventory_PUB2");
            assertThat(get(aware.queryProcess(BOB, "Rates", "q"))).isEqualTo("hello-Rates");

            aware.resumeTenant(ALICE, TenantId.of("PUB1"));
            assertThat(get(aware.queryProcess(ALICE, "Inventory_PUB1", "q"))).isEqualTo("hello-Inventory_PUB1");

            assertThat(aware.removeTenant(ALICE, TenantId.of("PUB1")))
                    .containsExactlyInAnyOrder("Inventory_PUB1", "Orders_PUB1");
            assertThat(engine.currentGraph().nodes()).containsOnlyKeys("Rates", "Inventory_PUB2");
        }
    }

    @Test
    @Timeout(30)
    void removing_several_tenants_at_once_writes_one_graph_change() {
        try (var backend = new InMemoryLogBackend();
             var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe())) {
            engine.newGraph(tenantGraph("Rates", "Inventory_PUB1", "Orders_PUB1", "Inventory_PUB2",
                    "Inventory_PUB3", "Inventory_PUB4"));
            var admin = TenantCaller.of("admin", TenantId.of("PUB1"), TenantId.of("PUB2"), TenantId.of("PUB3"));
            var aware = ownTenantsOnly(engine).globalProcesses("Rates").build();
            int graphsBefore = graphChanges(backend);

            assertThat(aware.removeTenants(admin, java.util.List.of(
                    TenantId.of("PUB1"), TenantId.of("PUB2"), TenantId.of("PUB3"))))
                    .containsExactlyInAnyOrder("Inventory_PUB1", "Orders_PUB1", "Inventory_PUB2", "Inventory_PUB3");
            assertThat(engine.currentGraph().nodes()).containsOnlyKeys("Rates", "Inventory_PUB4");
            assertThat(graphChanges(backend) - graphsBefore).as("LogChangeGraph events written").isEqualTo(1);
            // Tenants without processes (any more) are a no-op.
            assertThat(aware.removeTenants(admin, java.util.List.of(TenantId.of("PUB1")))).isEmpty();
            assertThat(graphChanges(backend) - graphsBefore).isEqualTo(1);
        }
    }

    @Test
    @Timeout(10)
    void removing_several_tenants_is_all_or_nothing_on_authorisation() {
        try (var engine = engine(tenantGraph("Inventory_PUB1", "Inventory_PUB2"))) {
            var aware = ownTenantsOnly(engine).build();
            assertThatThrownBy(() -> aware.removeTenants(ALICE, java.util.List.of(TenantId.of("PUB1"), TenantId.of("PUB2"))))
                    .isInstanceOf(TenantAccessDeniedException.class)
                    .hasMessageContaining("PUB2");
            assertThat(engine.currentGraph().nodes()).containsOnlyKeys("Inventory_PUB1", "Inventory_PUB2");
        }
    }

    @Test
    @Timeout(30)
    void removing_several_tenants_resolves_each_node_once() {
        int tenants = 20;
        var names = new java.util.ArrayList<String>();
        var ids = new java.util.ArrayList<TenantId>();
        for (int t = 0; t < tenants; t++) {
            ids.add(TenantId.of("T" + t));
            names.add("Inventory_T" + t);
            names.add("Orders_T" + t);
        }
        names.add("Rates");
        try (var engine = engine(tenantGraph(names.toArray(String[]::new)))) {
            var calls = new AtomicInteger();
            TenantResolver suffix = TenantResolver.suffixAfter("_");
            var aware = TenantAwareEngine.builder(engine)
                    .tenantResolver(name -> {
                        calls.incrementAndGet();
                        return suffix.resolve(name);
                    })
                    .authzPolicy((caller, tenant) -> true)
                    .globalProcesses("Rates")
                    .build();
            calls.set(0);

            assertThat(aware.removeTenants(TenantCaller.of("admin"), ids)).hasSize(2 * tenants);
            // Linear: one resolver call per non-global node, not one per node per tenant.
            assertThat(calls.get()).as("resolver calls").isEqualTo(2 * tenants);
            assertThat(engine.currentGraph().nodes()).containsOnlyKeys("Rates");
        }
    }

    private static int graphChanges(InMemoryLogBackend backend) {
        int n = 0;
        for (int i = 0; i < backend.length(); i++) {
            if (backend.get(i) instanceof io.fom.log.LogChangeGraph) n++;
        }
        return n;
    }

    @Test
    @Timeout(10)
    void tenant_lifecycle_calls_are_authorized() {
        try (var engine = engine(tenantGraph("Inventory_PUB1"))) {
            var aware = ownTenantsOnly(engine).build();
            assertThatThrownBy(() -> aware.pauseTenant(BOB, TenantId.of("PUB1")))
                    .isInstanceOf(TenantAccessDeniedException.class);
            assertThatThrownBy(() -> aware.removeTenant(BOB, TenantId.of("PUB1")))
                    .isInstanceOf(TenantAccessDeniedException.class);
            assertThat(engine.currentGraph().nodes()).containsKey("Inventory_PUB1");
        }
    }

    @Test
    @Timeout(20)
    void resuming_a_tenant_also_resumes_globals_that_were_paused_waiting_for_it() throws Exception {
        Graph tenantAndAudit = new GraphBuilder()
                .addWithParam("Orders_PUB1", (Supplier<ParamProcessInitializer<ProcName>>) Hello::new,
                        (Supplier<ParamProcessLoader<ProcName>>) Hello::new, new ProcName("Orders_PUB1"))
                .addWithParam("Audit", (Supplier<ParamProcessInitializer<ProcName>>) Hello::new,
                        (Supplier<ParamProcessLoader<ProcName>>) Hello::new, new ProcName("Audit"), "Orders_PUB1")
                .build();
        Graph full = new GraphBuilder()
                .addWithParam("Orders_PUB1", (Supplier<ParamProcessInitializer<ProcName>>) Hello::new,
                        (Supplier<ParamProcessLoader<ProcName>>) Hello::new, new ProcName("Orders_PUB1"))
                .addWithParam("Audit", (Supplier<ParamProcessInitializer<ProcName>>) Hello::new,
                        (Supplier<ParamProcessLoader<ProcName>>) Hello::new, new ProcName("Audit"), "Orders_PUB1")
                .addWithParam("Dashboard", (Supplier<ParamProcessInitializer<ProcName>>) Hello::new,
                        (Supplier<ParamProcessLoader<ProcName>>) Hello::new, new ProcName("Dashboard"), "Orders_PUB1")
                .addWithParam("Report", (Supplier<ParamProcessInitializer<ProcName>>) Hello::new,
                        (Supplier<ParamProcessLoader<ProcName>>) Hello::new, new ProcName("Report"), "Dashboard")
                .build();
        try (var backend = new InMemoryLogBackend()) {
            try (var first = new Engine(fastConfig(), backend, new JavaSerializableSerDe())) {
                first.newGraph(tenantAndAudit);
                // The operator pauses the tenant and, deliberately, the global Audit.
                first.pause(java.util.List.of("Orders_PUB1", "Audit"));
            }
            try (var engine = new Engine(fastConfig(), backend, new JavaSerializableSerDe())) {
                // Restarted with new globals on top of the paused tenant: the engine pauses them too.
                engine.newGraph(full);
                assertThat(engine.pausedByDependency()).containsExactlyInAnyOrder("Dashboard", "Report");

                // A tenant admin without rights on global processes resumes only the tenant.
                var tenantOnly = ownTenantsOnly(engine).globalProcesses("Dashboard", "Report", "Audit").build();
                tenantOnly.resumeTenant(ALICE, TenantId.of("PUB1"));
                assertThat(get(tenantOnly.queryProcess(ALICE, "Orders_PUB1", "q"))).isEqualTo("hello-Orders_PUB1");
                assertThatThrownBy(() -> get(tenantOnly.queryProcess(BOB, "Dashboard", "q")))
                        .hasCauseInstanceOf(QueryRejectedException.class);

                // An operator allowed on globals brings back the chain waiting for the tenant,
                // but not the global that was paused on purpose.
                var operator = ownTenantsOnly(engine).globalProcesses("Dashboard", "Report", "Audit")
                        .globalTriggerPolicy(caller -> caller == ALICE).build();
                operator.resumeTenant(ALICE, TenantId.of("PUB1"));
                assertThat(get(operator.queryProcess(BOB, "Dashboard", "q"))).isEqualTo("hello-Dashboard");
                assertThat(get(operator.queryProcess(BOB, "Report", "q"))).isEqualTo("hello-Report");
                assertThatThrownBy(() -> get(operator.queryProcess(BOB, "Audit", "q")))
                        .hasCauseInstanceOf(QueryRejectedException.class);
            }
        }
    }

    @Test
    @Timeout(20)
    void concurrent_removals_of_the_same_tenant_do_not_fail() throws Exception {
        try (var engine = engine(tenantGraph("Inventory_PUB1", "Orders_PUB1", "Inventory_PUB2"))) {
            var aware = ownTenantsOnly(engine).build();
            var start = new java.util.concurrent.CountDownLatch(1);
            var removals = new java.util.ArrayList<CompletableFuture<java.util.Set<String>>>();
            for (int i = 0; i < 4; i++) {
                removals.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                    return aware.removeTenant(ALICE, TenantId.of("PUB1"));
                }));
            }
            start.countDown();
            var removed = new java.util.HashSet<String>();
            for (var removal : removals) removed.addAll(removal.get(10, TimeUnit.SECONDS));
            assertThat(removed).containsExactlyInAnyOrder("Inventory_PUB1", "Orders_PUB1");
            assertThat(engine.currentGraph().nodes()).containsOnlyKeys("Inventory_PUB2");
        }
    }

    @Test
    @Timeout(10)
    void a_resolver_returning_null_is_reported_clearly() {
        try (var engine = engine(tenantGraph("Inventory_PUB1"))) {
            var aware = ownTenantsOnly(engine).tenantResolver(name -> null).build();
            assertThatThrownBy(() -> aware.removeTenant(ALICE, TenantId.of("PUB1")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("returned null for process 'Inventory_PUB1'");
            assertThatThrownBy(() -> get(aware.queryProcess(ALICE, "Inventory_PUB1", "q")))
                    .hasRootCauseMessage("TenantResolver returned null for process 'Inventory_PUB1'; "
                            + "it must return Optional.empty() for a name that does not resolve");
        }
    }

    @Test
    @Timeout(10)
    void a_failing_authz_policy_fails_the_returned_stage_instead_of_throwing() {
        try (var engine = engine(tenantGraph("Inventory_PUB1"))) {
            var aware = TenantAwareEngine.builder(engine)
                    .authzPolicy((caller, tenant) -> { throw new IllegalStateException("authz service down"); })
                    .build();
            var stage = aware.queryProcess(ALICE, "Inventory_PUB1", "q");
            assertThatThrownBy(() -> get(stage)).hasRootCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void repeated_tenant_ids_are_collapsed() {
        assertThat(TenantCaller.of("x", TenantId.of("A"), TenantId.of("A"), TenantId.of("B")).tenants()).hasSize(2);
    }

    // ───────────────── fixtures ─────────────────

    private static Graph tenantGraph(String... names) {
        var builder = new GraphBuilder();
        for (String name : names) {
            builder.addWithParam(name,
                    (Supplier<ParamProcessInitializer<ProcName>>) Hello::new,
                    (Supplier<ParamProcessLoader<ProcName>>) Hello::new,
                    new ProcName(name));
        }
        return builder.build();
    }

    record ProcName(String value) implements Serializable { }

    record GetInventory(String process) implements Routable, Serializable {
        @Override public String targetProcess() { return process; }
    }

    /** Names PUB1 on its first targetProcess() call and PUB2 afterwards. */
    static final class ShapeShifter implements Routable, Serializable {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public String targetProcess() {
            return calls.getAndIncrement() == 0 ? "Inventory_PUB1" : "Inventory_PUB2";
        }
    }

    /** Never answers, so a per-query timeout can fire. */
    static final class SlowProcess implements io.fom.api.ProcessInitializer, io.fom.api.ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> new CompletableFuture<>());
        }
    }

    static final class Hello implements ParamProcessInitializer<ProcName>, ParamProcessLoader<ProcName> {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx, ProcName name) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<Process> load(QueryableContext ctx, Map<String, byte[]> props, ProcName name) {
            return CompletableFuture.completedFuture(
                    (c, q) -> CompletableFuture.completedFuture("hello-" + name.value()));
        }
    }

    @Test
    @Timeout(10)
    void processes_of_fails_on_a_closed_engine_like_every_other_call() {
        var engine = engine(tenantGraph("Inventory_PUB1"));
        var aware = ownTenantsOnly(engine).build();
        engine.close();

        assertThatThrownBy(() -> aware.processesOf(ALICE, TenantId.of("PUB1")))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> aware.pauseTenant(ALICE, TenantId.of("PUB1")))
                .isInstanceOf(IllegalStateException.class);
    }
}
