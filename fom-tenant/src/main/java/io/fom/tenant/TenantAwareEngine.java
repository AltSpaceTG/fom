package io.fom.tenant;

import io.fom.Engine;
import io.fom.api.Routable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiPredicate;

/**
 * Wrapper over {@link Engine} that enforces a per-tenant authorization
 * policy on every {@code query} / {@code trigger} / lifecycle call.
 *
 * <p>The wrapped {@link Engine} stays the source of truth — this class adds
 * a defence-in-depth check on top. A {@link TenantResolver} maps a process
 * name to its {@link TenantId}; the {@code authzPolicy} decides whether a
 * given caller may touch a given tenant.</p>
 *
 * <p><strong>Fail-closed:</strong> the default {@code authzPolicy} denies
 * everything, so a wrapper built without an explicit policy protects all
 * tenants until one is configured. {@link #query(TenantCaller, Object)} only
 * authorizes messages that implement {@link Routable} (whose target process is
 * known up front); a non-{@code Routable} message would be type-routed by the
 * engine to a process this wrapper cannot identify in advance, so it is
 * rejected — use {@link #queryProcess(TenantCaller, String, Object)} for
 * explicit addressing.</p>
 *
 * <p>Processes whose names do not resolve to a tenant (e.g. global support
 * processes) bypass the authz check.</p>
 */
public final class TenantAwareEngine {

    private final Engine delegate;
    private final TenantResolver resolver;
    private final BiPredicate<TenantCaller, TenantId> authzPolicy;

    private TenantAwareEngine(Engine delegate,
                              TenantResolver resolver,
                              BiPredicate<TenantCaller, TenantId> authzPolicy) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.authzPolicy = Objects.requireNonNull(authzPolicy, "authzPolicy");
    }

    public Engine delegate() {
        return delegate;
    }

    /**
     * Dispatch {@code msg} after checking {@code caller} may touch the resolved
     * tenant. Only {@link Routable} messages are accepted: the target process
     * (and therefore the tenant) must be known before dispatch so it can be
     * authorized. A non-{@code Routable} message — which the engine would
     * type-route internally — is rejected with {@link TenantAccessDeniedException}
     * to avoid a fail-open bypass.
     */
    public CompletionStage<Object> query(TenantCaller caller, Object msg) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(msg, "msg");
        if (msg instanceof Routable r && r.targetProcess() != null) {
            CompletionStage<Object> denied = checkAccess(caller, r.targetProcess());
            if (denied != null) return denied;
            return delegate.query(msg);
        }
        return deny("Cannot authorize a non-Routable query through TenantAwareEngine; "
                + "implement Routable or use queryProcess(caller, name, msg)");
    }

    /** Explicit addressing — also checked against authz. */
    public CompletionStage<Object> queryProcess(TenantCaller caller, String processName, Object msg) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(msg, "msg");
        CompletionStage<Object> denied = checkAccess(caller, processName);
        if (denied != null) return denied;
        return delegate.queryProcess(processName, msg);
    }

    /** Trigger after authz check. */
    public boolean trigger(TenantCaller caller, String processName, Serializable value) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(processName, "processName");
        Optional<TenantId> tenant = resolver.resolve(processName);
        if (tenant.isPresent() && !authzPolicy.test(caller, tenant.get())) {
            throw new TenantAccessDeniedException(
                    "Caller " + caller + " cannot trigger tenant " + tenant.get());
        }
        return delegate.trigger(processName, value);
    }

    /**
     * Shutdown every process belonging to {@code tenantId}, in arbitrary order,
     * after checking {@code caller} is authorized to manage that tenant.
     * Discovered by enumerating live processes and filtering by the resolver.
     *
     * @throws TenantAccessDeniedException if {@code caller} may not manage {@code tenantId}
     */
    public CompletionStage<Void> shutdownTenant(TenantCaller caller, TenantId tenantId, Duration perProcessTimeout) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(perProcessTimeout, "perProcessTimeout");
        if (!authzPolicy.test(caller, tenantId)) {
            var failed = new CompletableFuture<Void>();
            failed.completeExceptionally(new TenantAccessDeniedException(
                    "Caller " + caller + " cannot manage tenant " + tenantId));
            return failed;
        }
        return delegate.introspect().thenCompose(report -> {
            List<CompletionStage<Object>> futures = new ArrayList<>();
            for (var node : report.graph().nodes()) {
                if (resolver.resolve(node.name())
                        .map(t -> t.equals(tenantId))
                        .orElse(false)) {
                    futures.add(delegate.queryProcess(node.name(),
                            new ShutdownSignal(tenantId), perProcessTimeout)
                            .exceptionally(e -> null));
                }
            }
            return CompletableFuture.allOf(
                    futures.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new));
        });
    }

    private CompletionStage<Object> checkAccess(TenantCaller caller, String processName) {
        Optional<TenantId> tenant = resolver.resolve(processName);
        if (tenant.isPresent() && !authzPolicy.test(caller, tenant.get())) {
            return deny("Caller " + caller + " cannot access tenant " + tenant.get());
        }
        return null;
    }

    private static CompletionStage<Object> deny(String message) {
        var failed = new CompletableFuture<Object>();
        failed.completeExceptionally(new TenantAccessDeniedException(message));
        return failed;
    }

    public static Builder builder(Engine engine) {
        return new Builder(engine);
    }

    /** Synthetic marker sent by {@link #shutdownTenant} — implementers ignore or treat as no-op. */
    public record ShutdownSignal(TenantId tenant) implements Serializable { }

    /** Builder for {@link TenantAwareEngine}. */
    public static final class Builder {

        private final Engine engine;
        private TenantResolver resolver = TenantResolver.suffixAfter("_");
        // Fail-closed default: deny everything until an explicit policy is supplied.
        private BiPredicate<TenantCaller, TenantId> authzPolicy = (caller, tenant) -> false;

        Builder(Engine engine) {
            this.engine = Objects.requireNonNull(engine, "engine");
        }

        public Builder tenantResolver(TenantResolver resolver) {
            this.resolver = Objects.requireNonNull(resolver, "resolver");
            return this;
        }

        public Builder authzPolicy(BiPredicate<TenantCaller, TenantId> policy) {
            this.authzPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        public TenantAwareEngine build() {
            return new TenantAwareEngine(engine, resolver, authzPolicy);
        }
    }
}
