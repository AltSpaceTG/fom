package io.fom.test;

import io.fom.Sid;
import io.fom.api.Deadline;
import io.fom.api.Process;
import io.fom.api.QueryableContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract for a {@link Process} that checks its query deadline cooperatively: given a
 * {@link Deadline} that has already expired, it must give up within
 * {@link #graceMillis()}.
 *
 * <pre>
 * class MyProcessInterruptContractTest extends InterruptContractTest {
 *     {@literal @}Override protected Process newProcess() { return new MyProcess(...); }
 *     {@literal @}Override protected Object cancellableQuery() { return new LongQuery(); }
 * }
 * </pre>
 */
public abstract class InterruptContractTest {

    /** Build a fresh process instance per test. */
    protected abstract Process newProcess();

    /** A query the process runs as a long operation that checks its deadline. */
    protected abstract Object cancellableQuery();

    /** Maximum time the process is allowed to keep running past an expired deadline. */
    protected long graceMillis() {
        return 200;
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void honours_expired_deadline() throws Exception {
        Process process = newProcess();
        Executor executor = Executors.newVirtualThreadPerTaskExecutor();
        var ctx = new ExpiredCtx(new Sid("test", 0), executor);

        Instant start = Instant.now();
        CompletionStage<?> stage = process.compute(ctx, cancellableQuery());
        try {
            stage.toCompletableFuture().get(graceMillis() + 1000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | CancellationException honoured) {
            // failing or cancelling both honour the deadline
        }
        Duration elapsed = Duration.between(start, Instant.now());
        assertThat(elapsed.toMillis())
                .as("Process should respect expired deadline within %dms grace", graceMillis())
                .isLessThan(graceMillis() + 500);
    }

    /** A {@link QueryableContext} whose deadline has already passed. */
    private static final class ExpiredCtx implements QueryableContext {

        private final Sid sid;
        private final Executor executor;
        private final Deadline expired;

        ExpiredCtx(Sid sid, Executor executor) {
            this.sid = sid;
            this.executor = executor;
            this.expired = new Deadline(Instant.now().minusSeconds(60));
        }

        @Override public Sid sid() { return sid; }
        @Override public Executor executor() { return executor; }
        @Override public List<String> dependencies() { return List.of(); }
        @Override public Optional<Deadline> currentQueryDeadline() { return Optional.of(expired); }
        @Override public CompletionStage<Object> query(String dep, Object q) {
            var failed = new CompletableFuture<Object>();
            failed.completeExceptionally(new UnsupportedOperationException("no deps in contract test"));
            return failed;
        }
    }

    /** A cooperative process that loops until its deadline expires; used by the self-test. */
    public static final class CooperativeBusyLoop implements Process {

        public final AtomicBoolean cancelled = new AtomicBoolean();

        @Override
        public CompletionStage<?> compute(QueryableContext ctx, Object query) {
            return CompletableFuture.supplyAsync(() -> {
                while (true) {
                    if (ctx.currentQueryDeadline().map(Deadline::isExpired).orElse(false)) {
                        cancelled.set(true);
                        throw new CancellationException("deadline expired");
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        cancelled.set(true);
                        throw new CancellationException("interrupted");
                    }
                }
            }, ctx.executor());
        }
    }
}
