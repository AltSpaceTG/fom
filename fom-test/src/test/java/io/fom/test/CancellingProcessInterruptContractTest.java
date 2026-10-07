package io.fom.test;

import io.fom.api.Deadline;
import io.fom.api.Process;

import java.util.concurrent.CompletableFuture;

/**
 * Self-test: a process that honours an expired deadline by cancelling its
 * returned future (so {@code get()} throws {@code CancellationException}
 * directly, not wrapped in {@code ExecutionException}) satisfies the contract.
 */
class CancellingProcessInterruptContractTest extends InterruptContractTest {

    @Override
    protected Process newProcess() {
        return (ctx, q) -> {
            var f = new CompletableFuture<Object>();
            if (ctx.currentQueryDeadline().map(Deadline::isExpired).orElse(false)) {
                f.cancel(true);
            }
            return f;
        };
    }

    @Override
    protected Object cancellableQuery() {
        return "q";
    }
}
