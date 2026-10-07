package io.fom.tenant;

import io.fom.Engine;
import io.fom.ProcessRef;
import io.fom.api.QueryException;
import io.fom.api.Routable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Wrapper over {@link Engine} that enforces a per-tenant authorization policy
 * on queries, triggers and tenant lifecycle calls.
 *
 * <p>It is <strong>fail-closed</strong> throughout. Every access to a process
 * is decided like this:</p>
 * <ol>
 *   <li>the process is declared global ({@link Builder#globalProcesses}) →
 *       queries are allowed for every caller; a trigger (which re-initialises
 *       the shared process and cascades into every tenant) only for callers
 *       accepted by {@link Builder#globalTriggerPolicy} — nobody by default;</li>
 *   <li>the {@link TenantResolver} maps the name to a tenant → allowed only if
 *       {@code authzPolicy} accepts the caller for that tenant (the default
 *       policy denies everyone);</li>
 *   <li>otherwise (the name belongs to no tenant, or is ambiguous) →
 *       <strong>denied</strong>.</li>
 * </ol>
 *
 * <p>{@link #query(TenantCaller, Object)} accepts only {@link Routable}
 * messages, reads the target once, authorizes it and dispatches to exactly that
 * process, so a message cannot name one process for the check and another for
 * dispatch.</p>
 *
 * <p>The wrapped engine is intentionally not exposed: code that only holds a
 * {@code TenantAwareEngine} cannot bypass the checks.</p>
 */
public final class TenantAwareEngine {

    private final Engine engine;
    private final TenantResolver resolver;
    private final BiPredicate<TenantCaller, TenantId> authzPolicy;
    private final Set<String> globalProcesses;
    private final Predicate<TenantCaller> globalTriggerPolicy;

    private TenantAwareEngine(Engine engine,
                              TenantResolver resolver,
                              BiPredicate<TenantCaller, TenantId> authzPolicy,
                              Set<String> globalProcesses,
                              Predicate<TenantCaller> globalTriggerPolicy) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.authzPolicy = Objects.requireNonNull(authzPolicy, "authzPolicy");
        this.globalProcesses = Set.copyOf(globalProcesses);
        this.globalTriggerPolicy = Objects.requireNonNull(globalTriggerPolicy, "globalTriggerPolicy");
    }

    /**
     * Dispatch a {@link Routable} message after authorizing its target. A
     * non-{@code Routable} message would be type-routed inside the engine to a
     * process this wrapper cannot identify in advance, so it is rejected — use
     * {@link #queryProcess(TenantCaller, String, Object)} for explicit addressing.
     */
    public CompletionStage<Object> query(TenantCaller caller, Object msg) {
        return query0(caller, msg, null);
    }

    /**
     * {@link #query(TenantCaller, Object)} with a per-query timeout, as
     * {@code Engine.query(msg, timeout)}.
     */
    public CompletionStage<Object> query(TenantCaller caller, Object msg, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return query0(caller, msg, timeout);
    }

    private CompletionStage<Object> query0(TenantCaller caller, Object msg, Duration timeout) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(msg, "msg");
        if (!(msg instanceof Routable routable)) {
            return denied("Cannot authorize a non-Routable query; use queryProcess(caller, name, msg)");
        }
        String target;
        try {
            target = routable.targetProcess(); // read once: the checked name is the dispatched name
        } catch (RuntimeException e) {
            return failedStage(e); // as Engine.query does
        }
        if (target == null || target.isEmpty()) {
            // A routing bug, not a denial: report it as Engine.query would.
            return failedStage(new QueryException(
                    "Routable " + msg.getClass().getName() + " returned no target process"));
        }
        return authorizedQuery(caller, target, msg, timeout);
    }

    /** Explicit addressing, after authorization. */
    public CompletionStage<Object> queryProcess(TenantCaller caller, String processName, Object msg) {
        return queryProcess0(caller, processName, msg, null);
    }

    /**
     * {@link #queryProcess(TenantCaller, String, Object)} with a per-query
     * timeout, as {@code Engine.queryProcess(name, msg, timeout)}.
     */
    public CompletionStage<Object> queryProcess(TenantCaller caller, String processName, Object msg, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return queryProcess0(caller, processName, msg, timeout);
    }

    private CompletionStage<Object> queryProcess0(TenantCaller caller, String processName,
                                                  Object msg, Duration timeout) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(processName, "processName");
        Objects.requireNonNull(msg, "msg");
        return authorizedQuery(caller, processName, msg, timeout);
    }

    /** {@link #queryProcess(TenantCaller, String, Object)} keyed by a typed {@link ProcessRef}. */
    public CompletionStage<Object> queryProcess(TenantCaller caller, ProcessRef ref, Object msg) {
        Objects.requireNonNull(ref, "ref");
        return queryProcess0(caller, ref.name(), msg, null);
    }

    /** {@link #queryProcess(TenantCaller, String, Object, Duration)} keyed by a typed {@link ProcessRef}. */
    public CompletionStage<Object> queryProcess(TenantCaller caller, ProcessRef ref, Object msg, Duration timeout) {
        Objects.requireNonNull(ref, "ref");
        return queryProcess(caller, ref.name(), msg, timeout);
    }

    /**
     * Authorize {@code processName} for {@code caller} and dispatch, or fail the
     * returned stage. {@code timeout} {@code null} means the engine's configured
     * query timeout.
     */
    private CompletionStage<Object> authorizedQuery(TenantCaller caller, String processName,
                                                    Object msg, Duration timeout) {
        Optional<String> denial;
        try {
            denial = denialReason(caller, processName);
        } catch (RuntimeException e) { // a failing policy or resolver still fails closed
            return failedStage(e);
        }
        if (denial.isPresent()) return denied(denial.get());
        return timeout == null
                ? engine.queryProcess(processName, msg)
                : engine.queryProcess(processName, msg, timeout);
    }

    /**
     * Trigger a re-init after authorization. A global process may only be
     * triggered by callers accepted by {@link Builder#globalTriggerPolicy}.
     *
     * @throws TenantAccessDeniedException if access is denied
     */
    public boolean trigger(TenantCaller caller, String processName, Serializable value) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(processName, "processName");
        authorizeTrigger(caller, processName);
        return engine.trigger(processName, value);
    }

    /** {@link #trigger(TenantCaller, String, Serializable)} keyed by a typed {@link ProcessRef}. */
    public boolean trigger(TenantCaller caller, ProcessRef ref, Serializable value) {
        Objects.requireNonNull(ref, "ref");
        return trigger(caller, ref.name(), value);
    }

    /**
     * Trigger several processes as one durable record — the tenant-aware form of
     * {@link Engine#trigger(Map)}. Every name is authorized first; if any one is denied,
     * nothing is triggered.
     *
     * @throws TenantAccessDeniedException if access to any of the names is denied
     * @see Engine#trigger(Map)
     */
    public boolean trigger(TenantCaller caller, Map<String, Serializable> nameToValue) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(nameToValue, "nameToValue");
        // Check and trigger the same copy, so the caller's map cannot change in between.
        // Insertion order is kept: the durable LogTrigger lists names in it.
        var batch = new LinkedHashMap<String, Serializable>();
        nameToValue.forEach((name, value) -> batch.put(Objects.requireNonNull(name, "process name"),
                Objects.requireNonNull(value, "trigger value for '" + name + "'")));
        for (String processName : batch.keySet()) {
            authorizeTrigger(caller, processName);
        }
        return engine.trigger(batch);
    }

    /**
     * The names of {@code tenant}'s processes in the current graph — a read-only
     * view of what {@link #pauseTenant}, {@link #resumeTenant} and
     * {@link #removeTenant} would act on. Global processes
     * ({@link Builder#globalProcesses}) are never included, and neither are
     * names the {@link TenantResolver} maps to another tenant or to no tenant
     * at all, so a caller cannot use this to probe tenants it may not manage.
     *
     * @return an immutable set of the tenant's process names; empty if it has none
     * @throws TenantAccessDeniedException if {@code caller} may not manage {@code tenant}
     */
    public Set<String> processesOf(TenantCaller caller, TenantId tenant) {
        return authorizedProcessesOf(caller, tenant);
    }

    /**
     * Remove every process of {@code tenant} from the running graph for good:
     * their state is retired, so adding the tenant back later cold-inits.
     * Global processes are never removed.
     *
     * @return the names of the removed processes (empty if the tenant has none)
     * @throws TenantAccessDeniedException if {@code caller} may not manage {@code tenant}
     * @throws IllegalArgumentException if a process outside the tenant depends on one of them
     * @see Engine#remove
     */
    public Set<String> removeTenant(TenantCaller caller, TenantId tenant) {
        return onTenantProcesses(caller, tenant, engine::remove);
    }

    /**
     * Remove every process of all of {@code tenants} in <em>one</em> graph change. Each removal
     * writes the remaining graph to the log, so this beats {@code N} calls to
     * {@link #removeTenant}. If {@code caller} may not manage any one of the tenants, nothing is
     * removed. Global processes are never removed.
     *
     * @return the names of the removed processes of all the tenants (empty if they have none)
     * @throws TenantAccessDeniedException if {@code caller} may not manage one of {@code tenants}
     * @throws IllegalArgumentException if a process outside these tenants depends on one of them
     * @see Engine#remove
     */
    public Set<String> removeTenants(TenantCaller caller, Collection<TenantId> tenants) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(tenants, "tenants");
        List<TenantId> requested = List.copyOf(tenants); // rejects nulls, keeps the caller's order
        for (TenantId tenant : requested) authorize(caller, tenant); // every check before any change
        Set<TenantId> all = Set.copyOf(requested);
        Set<String> names = processesOf(all);
        while (!names.isEmpty()) {
            try {
                engine.remove(names);
                return names;
            } catch (IllegalArgumentException e) {
                // Retry as onTenantProcesses does.
                Set<String> current = processesOf(all);
                if (current.equals(names)) throw e;
                names = current;
            }
        }
        return names;
    }

    /**
     * Stop every process of {@code tenant} but keep their state, so
     * {@link #resumeTenant} warm-loads them. Queries to them are rejected while
     * paused. The pause is persisted in the log: after a restart they stay paused until resumed.
     *
     * @return the names of the paused processes (empty if the tenant has none)
     * @throws TenantAccessDeniedException if {@code caller} may not manage {@code tenant}
     * @throws IllegalArgumentException if a running process outside the tenant depends on one of them
     * @see Engine#pause
     */
    public Set<String> pauseTenant(TenantCaller caller, TenantId tenant) {
        return onTenantProcesses(caller, tenant, engine::pause);
    }

    /**
     * Restart the paused processes of {@code tenant}, warm-loading their state. If
     * {@code caller} passes {@link Builder#globalTriggerPolicy}, globals the engine left
     * paused only because they depend on these processes
     * ({@link Engine#pausedByDependency()}) are resumed too, down the chain. A global an
     * operator paused stays paused.
     *
     * @return the names of the tenant's processes (those not paused are left untouched)
     * @throws TenantAccessDeniedException if {@code caller} may not manage {@code tenant}
     * @throws RuntimeException if a process fails to reach {@code Serving}, possibly after the
     *         tenant's own processes are resumed; what did not start stays paused, and calling
     *         again retries it
     * @see Engine#resume
     */
    public Set<String> resumeTenant(TenantCaller caller, TenantId tenant) {
        Set<String> names = onTenantProcesses(caller, tenant, engine::resume);
        resumeGlobalsWaitingFor(caller, names);
        return names;
    }

    /**
     * Apply {@code action} to the tenant's processes. A refusal caused by a concurrent graph
     * change (say, a {@link #removeTenant} in between) is retried on the processes the tenant
     * has now; a refusal that persists is rethrown.
     */
    private Set<String> onTenantProcesses(TenantCaller caller, TenantId tenant, Consumer<Set<String>> action) {
        Set<String> names = authorizedProcessesOf(caller, tenant);
        while (!names.isEmpty()) {
            try {
                action.accept(names);
                return names;
            } catch (IllegalArgumentException e) {
                Set<String> current = authorizedProcessesOf(caller, tenant);
                if (current.equals(names)) throw e;
                names = current;
            }
        }
        return names;
    }

    private Set<String> authorizedProcessesOf(TenantCaller caller, TenantId tenant) {
        authorize(caller, tenant);
        return processesOf(Set.of(tenant));
    }

    private void authorize(TenantCaller caller, TenantId tenant) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(tenant, "tenant");
        // Before any engine-state check: a denied caller must not learn whether the engine is closed.
        if (!authzPolicy.test(caller, tenant)) {
            throw new TenantAccessDeniedException(
                    "Caller '" + caller.identity() + "' cannot manage tenant " + tenant);
        }
    }

    /** The non-global processes of the current graph that resolve to one of {@code tenants}. */
    private Set<String> processesOf(Set<TenantId> tenants) {
        if (engine.isClosed()) { // fail like every other call on a closed engine
            throw new IllegalStateException("Engine " + engine.instanceId() + " is closed");
        }
        Set<String> names = new TreeSet<>();
        for (String name : engine.currentGraph().nodes().keySet()) {
            if (!globalProcesses.contains(name) && resolve(name).filter(tenants::contains).isPresent()) {
                names.add(name);
            }
        }
        return Set.copyOf(names);
    }

    /** A trigger of a global process also needs {@link Builder#globalTriggerPolicy}. */
    private void authorizeTrigger(TenantCaller caller, String processName) {
        if (globalProcesses.contains(processName) && !globalTriggerPolicy.test(caller)) {
            throw new TenantAccessDeniedException("Caller '" + caller.identity()
                    + "' cannot trigger global process '" + processName + "'");
        }
        Optional<String> denial = denialReason(caller, processName);
        if (denial.isPresent()) throw new TenantAccessDeniedException(denial.get());
    }

    /** Empty if {@code caller} may access {@code processName}; otherwise why not. */
    private Optional<String> denialReason(TenantCaller caller, String processName) {
        if (globalProcesses.contains(processName)) {
            return Optional.empty();
        }
        Optional<TenantId> tenant = resolve(processName);
        if (tenant.isEmpty()) {
            return Optional.of("Process '" + processName
                    + "' does not resolve to a tenant and is not declared global");
        }
        if (!authzPolicy.test(caller, tenant.get())) {
            return Optional.of("Caller '" + caller.identity() + "' cannot access tenant " + tenant.get());
        }
        return Optional.empty();
    }

    private Optional<TenantId> resolve(String processName) {
        Optional<TenantId> tenant = resolver.resolve(processName);
        if (tenant == null) {
            throw new IllegalStateException("TenantResolver returned null for process '" + processName
                    + "'; it must return Optional.empty() for a name that does not resolve");
        }
        return tenant;
    }

    private static CompletionStage<Object> denied(String message) {
        return failedStage(new TenantAccessDeniedException(message));
    }

    private static CompletionStage<Object> failedStage(Throwable cause) {
        var failed = new CompletableFuture<Object>();
        failed.completeExceptionally(cause);
        return failed;
    }

    /**
     * Resume globals the engine paused only because they depend on {@code tenantProcesses},
     * then the globals waiting on those, and so on. Only for a caller that passes
     * {@link Builder#globalTriggerPolicy}.
     */
    private void resumeGlobalsWaitingFor(TenantCaller caller, Set<String> tenantProcesses) {
        if (tenantProcesses.isEmpty() || globalProcesses.isEmpty() || !globalTriggerPolicy.test(caller)) return;
        var graph = engine.currentGraph();
        Set<String> released = new HashSet<>(tenantProcesses);
        while (true) {
            Set<String> candidates = new TreeSet<>();
            for (String global : globalProcesses) {
                var node = graph.nodes().get(global);
                if (node != null && node.dependencies().stream().anyMatch(d -> released.contains(d.name()))) {
                    candidates.add(global);
                }
            }
            candidates.removeAll(released);
            if (candidates.isEmpty()) return;
            // The engine decides under its lock, so a global an operator just paused stays paused.
            Set<String> resumed = engine.resumeUnblocked(candidates);
            if (resumed.isEmpty()) return;
            released.addAll(resumed);
        }
    }

    public static Builder builder(Engine engine) {
        return new Builder(engine);
    }

    /** Builder for {@link TenantAwareEngine}. */
    public static final class Builder {

        private final Engine engine;
        private TenantResolver resolver = TenantResolver.suffixAfter("_");
        private BiPredicate<TenantCaller, TenantId> authzPolicy = (caller, tenant) -> false;
        private Set<String> globalProcesses = Set.of();
        private Predicate<TenantCaller> globalTriggerPolicy = caller -> false;

        Builder(Engine engine) {
            this.engine = Objects.requireNonNull(engine, "engine");
        }

        /** How process names map to tenants. Default: strict {@code suffixAfter("_")}. */
        public Builder tenantResolver(TenantResolver resolver) {
            this.resolver = Objects.requireNonNull(resolver, "resolver");
            return this;
        }

        /** Who may act on which tenant. Default: nobody (fail-closed). */
        public Builder authzPolicy(BiPredicate<TenantCaller, TenantId> policy) {
            this.authzPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        /**
         * Processes that belong to no tenant and every caller may query (shared reference
         * data such as Units). Replaces any earlier set. A name listed here is global even
         * if the resolver maps it to a tenant.
         */
        public Builder globalProcesses(Collection<String> names) {
            Objects.requireNonNull(names, "names");
            this.globalProcesses = Set.copyOf(names);
            return this;
        }

        /** Varargs form of {@link #globalProcesses(Collection)}. */
        public Builder globalProcesses(String... names) {
            return globalProcesses(List.of(names));
        }

        /**
         * Who may trigger a re-init of a global process. It cascades into every tenant,
         * so this is an admin operation. Default: nobody.
         */
        public Builder globalTriggerPolicy(Predicate<TenantCaller> policy) {
            this.globalTriggerPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        public TenantAwareEngine build() {
            return new TenantAwareEngine(engine, resolver, authzPolicy, globalProcesses, globalTriggerPolicy);
        }
    }
}
