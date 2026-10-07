package io.fom.config;

import io.fom.SnapshotContext;
import io.fom.SnapshotResult;
import io.fom.log.LogBackend;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CronSnapshotPolicyTest {

    @Test
    void a_unix_cron_expression_is_rejected_with_a_usable_message() {
        assertThatThrownBy(() -> new CronSnapshotPolicy("0 */5 * * *", 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Quartz cron expression '0 */5 * * *'")
                .hasMessageContaining("5-field Unix cron")
                .hasMessageContaining("'0 0 */5 * * ?'");
    }

    /** A numeric weekday is not carried over: Quartz counts from Sunday = 1, Unix from Sunday = 0. */
    @Test
    void a_numeric_weekday_is_not_suggested_verbatim() {
        String message = org.assertj.core.api.Assertions.catchThrowable(
                () -> new CronSnapshotPolicy("30 2 * * 1", 3)).getMessage();
        assertThat(message).doesNotContain("e.g.").contains("Sunday = 1");
    }

    /** The suggested Quartz form must itself be accepted — '0 0 * /5 * * *' is not. */
    @Test
    void the_suggested_quartz_form_is_valid() {
        for (String unix : new String[]{"0 */5 * * *", "30 2 * * MON", "0 0 1 * *", "*/15 * * * *"}) {
            String message = org.assertj.core.api.Assertions.catchThrowable(
                    () -> new CronSnapshotPolicy(unix, 3)).getMessage();
            var suggested = java.util.regex.Pattern.compile("e\\.g\\. '([^']+)'").matcher(message);
            assertThat(suggested.find()).as("a suggestion for %s in: %s", unix, message).isTrue();
            assertThat(new CronSnapshotPolicy(suggested.group(1), 3).expression())
                    .as("the suggestion for %s parses", unix).isEqualTo(suggested.group(1));
        }
    }

    @Test
    void a_cron_with_the_wrong_field_count_says_how_many_it_found() {
        assertThatThrownBy(() -> new CronSnapshotPolicy("0 0", 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("6 fields")
                .hasMessageContaining("this one has 2");
    }

    /** Regression: cron used to become a FixedInterval of "time until next fire", measured once at parse. */
    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void fires_on_cron_boundaries_and_rearms_after_each_fire() throws Exception {
        try (Ctx ctx = new Ctx()) {
            AutoCloseable handle = new CronSnapshotPolicy("* * * * * ?", 3, ZoneOffset.UTC).activate(ctx);
            try {
                awaitTrue(() -> ctx.fireTimes.size() >= 3, Duration.ofSeconds(6));
            } finally {
                handle.close();
            }
            List<Long> times = List.copyOf(ctx.fireTimes);
            for (long t : times) {
                assertThat(t % 1000).as("fires on a whole-second boundary").isLessThan(250);
            }
            for (int i = 1; i < times.size(); i++) {
                assertThat(times.get(i) - times.get(i - 1)).as("one fire per cron slot").isBetween(700L, 1300L);
            }
            awaitTrue(() -> ctx.purgeCalls.get() >= 3, Duration.ofSeconds(2));
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void keep_all_default_never_purges() throws Exception {
        assertThat(new CronSnapshotPolicy("0 0 3 * * ?").keepHistory()).isEqualTo(io.fom.SnapshotPolicy.KEEP_ALL);
        try (Ctx ctx = new Ctx()) {
            AutoCloseable handle = new CronSnapshotPolicy("* * * * * ?", ZoneOffset.UTC).activate(ctx);
            try {
                awaitTrue(() -> ctx.fireTimes.size() >= 2, Duration.ofSeconds(6));
                Thread.sleep(200); // let the last snapshot's completion run
            } finally {
                handle.close();
            }
            assertThat(ctx.purgeCalls).hasValue(0);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void slow_snapshot_does_not_block_scheduler_and_close_cancels() throws Exception {
        try (Ctx ctx = new Ctx()) {
            var slow = new CompletableFuture<SnapshotResult>();
            ctx.nextResult.set(slow);
            AutoCloseable handle = new CronSnapshotPolicy("* * * * * ?", 3, ZoneOffset.UTC).activate(ctx);

            awaitTrue(() -> ctx.fireTimes.size() == 1, Duration.ofSeconds(3));
            // The scheduler stays responsive while the snapshot runs.
            ctx.scheduler.submit(() -> { }).get(300, TimeUnit.MILLISECONDS);
            // Slots that fire while the snapshot is still in flight are skipped.
            Thread.sleep(1_500);
            assertThat(ctx.fireTimes).hasSize(1);

            slow.complete(new SnapshotResult("new", "old", 0, 0));
            awaitTrue(() -> ctx.fireTimes.size() == 2, Duration.ofSeconds(3));

            handle.close();
            int after = ctx.fireTimes.size();
            Thread.sleep(1_300);
            assertThat(ctx.fireTimes).as("close() cancels the pending fire").hasSize(after);
        }
    }

    @Test
    void validates_arguments_and_equality() {
        assertThatThrownBy(() -> new CronSnapshotPolicy("not a cron", 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CronSnapshotPolicy("0 0 3 * * ?", 0)).isInstanceOf(IllegalArgumentException.class);
        assertThat(new CronSnapshotPolicy("0 0 3 * * ?", 7, ZoneOffset.UTC))
                .isEqualTo(new CronSnapshotPolicy("0 0 3 * * ?", 7, ZoneOffset.UTC));
    }

    private static void awaitTrue(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not met within " + timeout);
            Thread.sleep(10);
        }
    }

    private static final class Ctx implements SnapshotContext, AutoCloseable {
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "fake-fom-scheduler");
            t.setDaemon(true);
            return t;
        });
        final List<Long> fireTimes = new CopyOnWriteArrayList<>();
        final AtomicInteger purgeCalls = new AtomicInteger();
        final AtomicReference<CompletableFuture<SnapshotResult>> nextResult = new AtomicReference<>();

        @Override
        public CompletionStage<SnapshotResult> snapshot() {
            fireTimes.add(System.currentTimeMillis());
            CompletableFuture<SnapshotResult> pending = nextResult.getAndSet(null);
            return pending != null ? pending
                    : CompletableFuture.completedFuture(new SnapshotResult("new", "old", 0, 0));
        }

        @Override public ScheduledExecutorService scheduler() { return scheduler; }
        @Override public LogBackend logBackend() { throw new UnsupportedOperationException(); }
        @Override public void purgeArchives(int keepHistory) { purgeCalls.incrementAndGet(); }
        @Override public void close() { scheduler.shutdownNow(); }
    }
}
