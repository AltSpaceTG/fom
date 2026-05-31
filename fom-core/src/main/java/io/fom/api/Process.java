package io.fom.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A live serving instance of a process node.
 *
 * <p>Implementations must be interrupt-safe:
 * use driver-level timeouts for blocking I/O, try-with-resources for external
 * resources, full rollback on exception, and cooperative deadline checks for
 * long loops via {@link QueryableContext#currentQueryDeadline()}.</p>
 */
@FunctionalInterface
public interface Process {

    /**
     * Compute the answer for {@code query}. Multiple {@code compute} calls may
     * execute concurrently for a single {@code Process} instance.
     */
    CompletionStage<?> compute(QueryableContext ctx, Object query);

    /**
     * Invoked when this {@code Sid} transitions to {@code Dead}, on
     * {@code engine.close()} or {@code engine.cancelInit(sid)}. NOT cancelled
     * by query timeouts — has its own {@code cleanup.timeout} (default 30s,
     * configurable per-process).
     *
     * <p>MUST release every external resource: abort multipart uploads, roll
     * back open transactions, close DB connections, delete temp files.</p>
     */
    default CompletionStage<Void> cleanUp(ProcessContext ctx) {
        return CompletableFuture.completedFuture(null);
    }
}
