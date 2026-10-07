package io.fom.config;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import io.fom.SnapshotContext;
import io.fom.SnapshotPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Snapshot rotation driven by a Quartz 6-field cron expression
 * ({@code "<sec> <min> <hour> <day-of-month> <month> <day-of-week>"}).
 *
 * <p>Each fire starts {@code Engine.snapshot()} asynchronously and, once it completes, purges
 * archives down to {@code keepHistory} (never with {@link SnapshotPolicy#KEEP_ALL}, the default
 * of the constructors without it). A fire that comes while the previous snapshot still runs is
 * skipped. Closing the handle returned by {@link #activate} cancels the pending fire.</p>
 *
 * @param expression  Quartz cron expression; validated on construction
 * @param keepHistory archives retained after each snapshot: {@link SnapshotPolicy#KEEP_ALL}, or {@code >= 1}
 * @param zone        time zone the cron is evaluated in
 */
public record CronSnapshotPolicy(String expression, int keepHistory, ZoneId zone) implements SnapshotPolicy {

    private static final Logger log = LoggerFactory.getLogger(CronSnapshotPolicy.class);

    static final CronParser QUARTZ = new CronParser(
            CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    /** A task that wakes up more than this long before its target re-arms instead of firing. */
    private static final long EARLY_WAKEUP_TOLERANCE_MS = 10;

    public CronSnapshotPolicy {
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(zone, "zone");
        if (keepHistory < 1) {
            throw new IllegalArgumentException("keepHistory must be >= 1 (or SnapshotPolicy.KEEP_ALL), was "
                    + keepHistory);
        }
        ExecutionTime exec = executionTime(expression);
        if (exec.nextExecution(ZonedDateTime.now(zone)).isEmpty()) {
            throw new IllegalArgumentException("Cron has no future execution: " + expression);
        }
    }

    /** Cron evaluated in the JVM's default time zone. */
    public CronSnapshotPolicy(String expression, int keepHistory) {
        this(expression, keepHistory, ZoneId.systemDefault());
    }

    /** Cron evaluated in {@code zone}; every archive is kept ({@link SnapshotPolicy#KEEP_ALL}). */
    public CronSnapshotPolicy(String expression, ZoneId zone) {
        this(expression, KEEP_ALL, zone);
    }

    /** Cron evaluated in the JVM's default time zone; every archive is kept ({@link SnapshotPolicy#KEEP_ALL}). */
    public CronSnapshotPolicy(String expression) {
        this(expression, KEEP_ALL, ZoneId.systemDefault());
    }

    /**
     * Hint for a 5-field Unix cron, with a Quartz form only if that form parses. Quartz forbids
     * {@code *} in both day fields, so one of them becomes {@code ?}.
     */
    private static String quartzSuggestion(String unix) {
        String[] f = unix.split("\\s+");
        String dayOfMonth = f[2];
        String dayOfWeek = f[4];
        if (!"?".equals(dayOfMonth) && !"?".equals(dayOfWeek)) {
            if ("*".equals(dayOfWeek)) {
                dayOfWeek = "?";
            } else if ("*".equals(dayOfMonth)) {
                dayOfMonth = "?";
            }
        }
        // Numeric weekdays differ between the dialects (Unix Sunday = 0, Quartz Sunday = 1).
        if (dayOfWeek.matches(".*\\d.*")) {
            return ", so prepend a seconds field, use '?' in either day-of-month or day-of-week,"
                    + " and note that Quartz numbers weekdays from Sunday = 1 (Unix: Sunday = 0) —"
                    + " names such as MON-FRI mean the same in both";
        }
        String candidate = "0 " + f[0] + " " + f[1] + " " + dayOfMonth + " " + f[3] + " " + dayOfWeek;
        try {
            QUARTZ.parse(candidate);
            return ", so prepend a seconds field and use '?' for one day field (e.g. '" + candidate + "')";
        } catch (IllegalArgumentException stillInvalid) {
            return ", so prepend a seconds field and use '?' in either day-of-month or day-of-week";
        }
    }

    private static ExecutionTime executionTime(String expression) {
        try {
            return ExecutionTime.forCron(QUARTZ.parse(expression));
        } catch (IllegalArgumentException e) {
            // cron-utils' own message does not say which dialect is expected.
            int fields = expression.trim().isEmpty() ? 0 : expression.trim().split("\\s+").length;
            String hint = switch (fields) {
                case 5 -> " — this looks like a 5-field Unix cron; Quartz starts with seconds"
                        + quartzSuggestion(expression.trim());
                case 6, 7 -> "";
                default -> " — a Quartz cron has 6 fields (seconds minutes hours day-of-month month"
                        + " day-of-week) plus an optional year, this one has " + fields;
            };
            throw new IllegalArgumentException("Invalid Quartz cron expression '" + expression + "'"
                    + hint + ": " + e.getMessage(), e);
        }
    }

    @Override
    public AutoCloseable activate(SnapshotContext context) {
        Objects.requireNonNull(context, "context");
        Schedule schedule = new Schedule(context, executionTime(expression));
        schedule.arm(null);
        return schedule::close;
    }

    /** Per-activation state: the pending one-shot and the in-flight flag. */
    private final class Schedule {
        private final SnapshotContext context;
        private final ExecutionTime exec;
        private final AtomicBoolean inFlight = new AtomicBoolean();
        private boolean closed;
        private ScheduledFuture<?> pending;

        Schedule(SnapshotContext context, ExecutionTime exec) {
            this.context = context;
            this.exec = exec;
        }

        /**
         * Schedule the next fire strictly after {@code previousTarget} or now, whichever is
         * later, so an early wake-up never fires one slot twice.
         */
        synchronized void arm(ZonedDateTime previousTarget) {
            if (closed) return;
            ZonedDateTime now = ZonedDateTime.now(zone);
            ZonedDateTime base = previousTarget != null && previousTarget.isAfter(now) ? previousTarget : now;
            // cron-utils keeps the input's sub-second part (12:00:00.300 -> 12:00:01.300).
            Optional<ZonedDateTime> next = exec.nextExecution(base.truncatedTo(ChronoUnit.SECONDS));
            if (next.isEmpty()) {
                log.warn("CronSnapshotPolicy '{}' has no further executions; rotation stops", expression);
                return;
            }
            ZonedDateTime target = next.get();
            long delayMs = Math.max(0L, Duration.between(now, target).toMillis());
            pending = context.scheduler().schedule(() -> fire(target), delayMs, TimeUnit.MILLISECONDS);
        }

        private void fire(ZonedDateTime target) {
            try {
                long early = Duration.between(ZonedDateTime.now(zone), target).toMillis();
                if (early > EARLY_WAKEUP_TOLERANCE_MS) {
                    // The wall clock drifted from the scheduler's over a long delay. Slots are
                    // whole seconds, so "strictly after target - 1s" re-arms for the same slot.
                    rearmFor(target.minusSeconds(1));
                    return;
                }
                rearmFor(target);
                if (isClosed() || !inFlight.compareAndSet(false, true)) return;
                try {
                    context.snapshot().whenComplete(this::afterSnapshot);
                } catch (Throwable t) {
                    inFlight.set(false);
                    throw t;
                }
            } catch (Throwable t) {
                log.warn("CronSnapshotPolicy fire failed: {}", t.toString());
            }
        }

        private void afterSnapshot(Object result, Throwable err) {
            try {
                if (err != null) {
                    log.warn("CronSnapshotPolicy snapshot failed: {}", err.toString());
                } else if (!isClosed() && keepHistory != KEEP_ALL) {
                    context.purgeArchives(keepHistory);
                }
            } catch (Throwable t) {
                log.warn("CronSnapshotPolicy archive purge failed: {}", t.toString());
            } finally {
                inFlight.set(false);
            }
        }

        private void rearmFor(ZonedDateTime previousTarget) {
            try {
                arm(previousTarget);
            } catch (RuntimeException e) {
                // e.g. RejectedExecutionException after the engine's scheduler shut down
                log.debug("CronSnapshotPolicy could not re-arm: {}", e.toString());
            }
        }

        private synchronized boolean isClosed() {
            return closed;
        }

        synchronized void close() {
            closed = true;
            if (pending != null) pending.cancel(false);
        }
    }
}
