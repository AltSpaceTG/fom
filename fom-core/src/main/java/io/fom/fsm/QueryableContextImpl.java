package io.fom.fsm;

import io.fom.Sid;
import io.fom.api.Deadline;
import io.fom.api.QueryableContext;
import io.fom.api.UndeclaredDependencyException;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/** {@link QueryableContext} whose {@code query(name, msg)} goes through a {@link ProcessRouter}. */
final class QueryableContextImpl implements QueryableContext {

    private final Sid sid;
    private final Executor executor;
    private final List<String> dependencies;
    private final Deadline deadline;
    private final ProcessRouter router;
    private final long deadlineEpochMillis;
    /** The query being computed, or {@code null} during init/load. */
    private final UUID queryId;

    private QueryableContextImpl(Sid sid,
                                 Executor executor,
                                 List<String> dependencies,
                                 Deadline deadline,
                                 long deadlineEpochMillis,
                                 ProcessRouter router,
                                 UUID queryId) {
        this.sid = Objects.requireNonNull(sid, "sid");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.dependencies = List.copyOf(Objects.requireNonNull(dependencies, "dependencies"));
        this.deadline = deadline;
        this.deadlineEpochMillis = deadlineEpochMillis;
        this.router = Objects.requireNonNull(router, "router");
        this.queryId = queryId;
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
            throw new UndeclaredDependencyException(sid.processName(),
                    "No such dependency: " + dependencyName + "; declared: " + dependencies);
        }
        CompletionStage<Object> reply = router.route(dependencyName, query, deadlineEpochMillis, queryId);
        if (deadlineEpochMillis > 0) {
            // Inherit the calling query's deadline, so a hung dependency cannot hold the reply forever.
            long remaining = Math.max(0, deadlineEpochMillis - System.currentTimeMillis());
            CompletableFuture<Object> pending = reply.toCompletableFuture();
            QueryDeadlines.failAfter(pending, remaining, "Query to '" + dependencyName
                    + "' did not complete within the deadline inherited from its calling query");
            return pending;
        }
        return reply;
    }

    static QueryableContextImpl forPhase(Sid sid,
                                         Executor executor,
                                         List<String> dependencies,
                                         ProcessRouter router) {
        return new QueryableContextImpl(sid, executor, dependencies, null, 0L, router, null);
    }

    static QueryableContextImpl forQuery(Sid sid,
                                         Executor executor,
                                         List<String> dependencies,
                                         long deadlineEpochMillis,
                                         ProcessRouter router,
                                         UUID queryId) {
        Deadline d = deadlineEpochMillis > 0
                ? new Deadline(Instant.ofEpochMilli(deadlineEpochMillis))
                : null;
        return new QueryableContextImpl(sid, executor, dependencies, d, deadlineEpochMillis, router, queryId);
    }
}
