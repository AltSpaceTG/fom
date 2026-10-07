package io.fom.fsm;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One timer for every query deadline. The timer thread only starts a virtual thread to fail the
 * reply, since completing it runs the caller's continuations. It is not the common
 * {@code ForkJoinPool}, which user code may block and so delay every timeout.
 */
public final class QueryDeadlines {

    private static final ScheduledThreadPoolExecutor TIMER = timer();

    private QueryDeadlines() {
    }

    private static ScheduledThreadPoolExecutor timer() {
        var executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "fom-query-deadlines");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true); // answered queries leave no entry behind
        return executor;
    }

    /**
     * Fail {@code reply} with a {@link TimeoutException} carrying {@code message} once
     * {@code delayMillis} has passed, unless it settles first.
     */
    public static void failAfter(CompletableFuture<Object> reply, long delayMillis, String message) {
        if (reply.isDone()) return;
        if (delayMillis <= 0) {
            reply.completeExceptionally(new TimeoutException(message));
            return;
        }
        try {
            ScheduledFuture<?> deadline = TIMER.schedule(
                    () -> Thread.ofVirtual().name("fom-query-deadline").start(
                            () -> reply.completeExceptionally(new TimeoutException(message))),
                    delayMillis, TimeUnit.MILLISECONDS);
            reply.whenComplete((r, e) -> deadline.cancel(false));
        } catch (RejectedExecutionException shuttingDown) {
            reply.completeExceptionally(new TimeoutException(message));
        }
    }
}
