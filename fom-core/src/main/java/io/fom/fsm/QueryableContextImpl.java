package io.fom.fsm;

import io.fom.Sid;
import io.fom.api.Deadline;
import io.fom.api.QueryableContext;
import io.fom.api.QueryException;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Internal implementation of {@link QueryableContext}. Cross-process
 * {@code query(name, msg)} is delegated to a {@link ProcessRouter} — for
 * single-process callers a router that rejects every name (matching the empty
 * dependency list) is sufficient.
 */
final class QueryableContextImpl implements QueryableContext {

    private final Sid sid;
    private final Executor executor;
    private final List<String> dependencies;
    private final Deadline deadline;
    private final ProcessRouter router;
    private final long deadlineEpochMillis;

    private QueryableContextImpl(Sid sid,
                                 Executor executor,
                                 List<String> dependencies,
                                 Deadline deadline,
                                 long deadlineEpochMillis,
                                 ProcessRouter router) {
        this.sid = Objects.requireNonNull(sid, "sid");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.dependencies = List.copyOf(Objects.requireNonNull(dependencies, "dependencies"));
        this.deadline = deadline;
        this.deadlineEpochMillis = deadlineEpochMillis;
        this.router = Objects.requireNonNull(router, "router");
    }

    @Override
    public Sid sid() {
        return sid;
    }

    @Override
    public Executor executor() {
        return executor;
    }

    @Override
    public List<String> dependencies() {
        return dependencies;
    }

    @Override
    public Optional<Deadline> currentQueryDeadline() {
        return Optional.ofNullable(deadline);
    }

    @Override
    public CompletionStage<Object> query(String dependencyName, Object query) {
        Objects.requireNonNull(dependencyName, "dependencyName");
        Objects.requireNonNull(query, "query");
        if (!dependencies.contains(dependencyName)) {
            throw new QueryException("No such dependency: " + dependencyName);
        }
        return router.route(dependencyName, query, deadlineEpochMillis);
    }

    static QueryableContextImpl forPhase(Sid sid,
                                         Executor executor,
                                         List<String> dependencies,
                                         ProcessRouter router) {
        return new QueryableContextImpl(sid, executor, dependencies, null, 0L, router);
    }

    static QueryableContextImpl forQuery(Sid sid,
                                         Executor executor,
                                         List<String> dependencies,
                                         long deadlineEpochMillis,
                                         ProcessRouter router) {
        Deadline d = deadlineEpochMillis > 0
                ? new Deadline(Instant.ofEpochMilli(deadlineEpochMillis))
                : null;
        return new QueryableContextImpl(sid, executor, dependencies, d, deadlineEpochMillis, router);
    }
}
