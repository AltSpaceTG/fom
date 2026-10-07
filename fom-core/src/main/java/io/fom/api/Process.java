package io.fom.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A loaded process, answering queries.
 *
 * <p>Must tolerate interruption: time out blocking I/O, close resources, roll back on
 * failure, and check {@link QueryableContext#currentQueryDeadline()} in long loops.</p>
 */
@FunctionalInterface
public interface Process {

    /** Answer {@code query}. Calls on one instance may run concurrently. */
    CompletionStage<?> compute(QueryableContext ctx, Object query);

    /**
     * Called when this state is retired: replaced, removed, cancelled, or the engine closes.
     * Bound by the engine's cleanup budget, not by query timeouts. Release every external
     * resource here (open transactions, connections, temp files).
     */
    default CompletionStage<Void> cleanUp(ProcessContext ctx) {
        return CompletableFuture.completedFuture(null);
    }
}
