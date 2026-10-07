package io.fom.otel;

import io.fom.Sid;
import io.fom.api.EngineObserver;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

/**
 * OpenTelemetry {@link EngineObserver}: one span per init, load and query, tagged with
 * {@code process.name} and the {@code Sid} clock.
 *
 * <p>A failed init or load span starts when the attempt started, not at the failure.</p>
 *
 * <p>A re-init that keeps the old version serving gets a {@code fom.reinit} span from its start to
 * the new version's promotion, with {@code sid.serving.clock} and {@code sid.new.clock}. If it gives
 * up, the span ends with status {@code ERROR} and the exception, and has no {@code sid.new.clock}.
 * A re-init cut short by a shutdown or removal emits no span.</p>
 *
 * <p>A query span starts on the thread that submits the query, so it is a child of the caller's
 * current span. A query a process sends through {@code ctx.query} is a child of the query it
 * serves, even if sent after that query has settled: ended query spans stay available as parents
 * for {@link #ENDED_PARENT_RETENTION}, at most {@link #ENDED_PARENT_CAP} of them. Every query span
 * ends when its reply settles. Spans inside your own {@code Process} code are yours to create.</p>
 */
public final class OtelEngineObserver implements EngineObserver {

    /** How long an ended query span can still parent a late dependency query. */
    static final Duration ENDED_PARENT_RETENTION = Duration.ofMinutes(5);

    /** Hard cap on remembered ended query spans; the oldest go first. */
    static final int ENDED_PARENT_CAP = 10_000;

    private final Tracer tracer;

    /** Open query spans by query id. */
    private final ConcurrentMap<UUID, Span> querySpans = new ConcurrentHashMap<>();

    /** Ended query spans in end order, kept as parents for late children; guarded by its own monitor. */
    private final LinkedHashMap<UUID, Ended> endedQuerySpans = new LinkedHashMap<>();

    private record Ended(SpanContext context, long at) { }

    private final long endedRetentionNanos;
    private final int endedCap;
    private final LongSupplier nanoClock;

    /** Start of the in-flight init attempt, by process name. */
    private final ConcurrentMap<String, Instant> initStarts = new ConcurrentHashMap<>();

    /** Start of the in-flight load attempt, by process name. */
    private final ConcurrentMap<String, Instant> loadStarts = new ConcurrentHashMap<>();

    /** The running keep-old re-init, by process name. */
    private final ConcurrentMap<String, Reinit> reinits = new ConcurrentHashMap<>();

    private record Reinit(Instant start, Sid serving) { }

    public OtelEngineObserver(OpenTelemetry otel) {
        this(otel.getTracer("io.fom"));
    }

    public OtelEngineObserver(Tracer tracer) {
        this(tracer, ENDED_PARENT_RETENTION, ENDED_PARENT_CAP, System::nanoTime);
    }

    /** For tests: a custom retention, cap and clock for ended query spans. */
    OtelEngineObserver(Tracer tracer, Duration endedRetention, int endedCap, LongSupplier nanoClock) {
        this.tracer = Objects.requireNonNull(tracer, "tracer");
        this.endedRetentionNanos = Objects.requireNonNull(endedRetention, "endedRetention").toNanos();
        if (endedCap < 1) throw new IllegalArgumentException("endedCap must be positive: " + endedCap);
        this.endedCap = endedCap;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    /** Ended query spans still remembered as parents (for tests). */
    int rememberedEndedQueries() {
        synchronized (endedQuerySpans) {
            return endedQuerySpans.size();
        }
    }

    /** In-flight init, load and re-init start entries (for tests). */
    int inFlightStarts() {
        return initStarts.size() + loadStarts.size() + reinits.size();
    }

    @Override
    public void onInitStarted(String processName, int attempt) {
        initStarts.put(processName, Instant.now());
    }

    @Override
    public void onInitCompleted(String processName, Sid newSid, Duration duration) {
        initStarts.remove(processName);
        emitCompleted("fom.init", processName, newSid, duration);
    }

    @Override
    public void onInitFailed(String processName, int attempt, Throwable cause) {
        SpanBuilder builder = lifecycleSpan("fom.init", processName)
                .setAttribute("init.attempt", attempt);
        emitFailed(builder, initStarts.remove(processName), cause);
    }

    @Override
    public void onLoadStarted(String processName, Sid sid, int attempt) {
        loadStarts.put(processName, Instant.now());
    }

    @Override
    public void onLoadCompleted(String processName, Sid sid, Duration duration) {
        loadStarts.remove(processName);
        emitCompleted("fom.load", processName, sid, duration);
    }

    @Override
    public void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) {
        SpanBuilder builder = lifecycleSpan("fom.load", processName)
                .setAttribute("load.attempt", attempt);
        if (sid != null) {
            builder.setAttribute("sid.clock", sid.clock());
        }
        emitFailed(builder, loadStarts.remove(processName), cause);
    }

    /**
     * Forgets the process's in-flight start without emitting a span. An attempt cut short by
     * removal is already reported through {@code onInitFailed}/{@code onLoadFailed}; inventing a
     * span here would duplicate that or report an abandoned attempt as a failure.
     */
    @Override
    public void onProcessRemoved(String processName) {
        initStarts.remove(processName);
        loadStarts.remove(processName);
        reinits.remove(processName);
    }

    @Override
    public void onReinitStarted(String processName, Sid servingSid) {
        reinits.put(processName, new Reinit(Instant.now(), servingSid));
    }

    @Override
    public void onSidPromotion(String processName, Sid previousSid, Sid newSid) {
        Reinit reinit = reinits.remove(processName);
        if (reinit == null) return;
        reinitSpan(processName, reinit)
                .setAttribute("sid.new.clock", newSid.clock())
                .startSpan()
                .end();
    }

    @Override
    public void onReinitFailed(String processName, Sid keptSid, Throwable cause) {
        Reinit reinit = reinits.remove(processName);
        if (reinit == null) reinit = new Reinit(null, keptSid);
        emitFailed(reinitSpan(processName, reinit), reinit.start(), cause);
    }

    /** A shutdown cancels a running re-init without a terminal callback; forget it. */
    @Override
    public void onStateTransition(String processName, String fromState, String toState) {
        if ("CleaningUp".equals(toState) || "Dead".equals(toState)) reinits.remove(processName);
    }

    private SpanBuilder reinitSpan(String processName, Reinit reinit) {
        SpanBuilder builder = lifecycleSpan("fom.reinit", processName);
        if (reinit.start() != null) builder.setStartTimestamp(reinit.start());
        if (reinit.serving() != null) builder.setAttribute("sid.serving.clock", reinit.serving().clock());
        return builder;
    }

    @Override
    public void onQuerySent(String processName, UUID queryId, Class<?> messageType, UUID parentQueryId) {
        SpanBuilder builder = tracer.spanBuilder("fom.query")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("process.name", processName)
                .setAttribute("query.id", queryId.toString())
                .setAttribute("query.type", messageType.getName());
        // Compute workers carry no OpenTelemetry context, so a dependency query is parented
        // explicitly to the query it serves.
        Span parent = parentQueryId == null ? null : parentSpan(parentQueryId);
        if (parent != null) {
            builder.setParent(Context.current().with(parent));
        }
        querySpans.put(queryId, builder.startSpan());
    }

    @Override
    public void onQueryCompleted(String processName, UUID queryId, Duration duration) {
        Span span = settleQuery(queryId);
        if (span != null) {
            span.end();
        }
    }

    @Override
    public void onQueryFailed(String processName, UUID queryId, String reason, Throwable cause) {
        Span span = settleQuery(queryId);
        if (span != null) {
            span.setStatus(StatusCode.ERROR, reason);
            if (cause != null) {
                span.recordException(cause);
            }
            span.end();
        }
    }

    /**
     * Moves the query's span from open to ended and returns it for the caller to end, or
     * {@code null} if unknown. It is remembered before {@code end()}, which runs the span
     * processors (slow exporters too), so a child sent meanwhile still finds its parent.
     */
    private Span settleQuery(UUID queryId) {
        Span span = querySpans.get(queryId);
        if (span != null) {
            rememberEnded(queryId, span);
            querySpans.remove(queryId);
        }
        return span;
    }

    private Span parentSpan(UUID parentQueryId) {
        Span open = querySpans.get(parentQueryId);
        if (open != null) return open;
        synchronized (endedQuerySpans) {
            Ended ended = endedQuerySpans.get(parentQueryId);
            if (ended == null || nanoClock.getAsLong() - ended.at() > endedRetentionNanos) return null;
            return Span.wrap(ended.context());
        }
    }

    /** Remembers an ended query span, forgetting expired and over-cap older ones. */
    private void rememberEnded(UUID queryId, Span span) {
        long now = nanoClock.getAsLong();
        synchronized (endedQuerySpans) {
            endedQuerySpans.put(queryId, new Ended(span.getSpanContext(), now));
            Iterator<Map.Entry<UUID, Ended>> it = endedQuerySpans.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<UUID, Ended> oldest = it.next();
                if (endedQuerySpans.size() <= endedCap && now - oldest.getValue().at() <= endedRetentionNanos) break;
                it.remove();
            }
        }
    }

    private SpanBuilder lifecycleSpan(String spanName, String processName) {
        return tracer.spanBuilder(spanName)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("process.name", processName);
    }

    /** Init and load spans are built after the fact, back-dated by the attempt's duration. */
    private void emitCompleted(String spanName, String processName, Sid sid, Duration duration) {
        lifecycleSpan(spanName, processName)
                .setStartTimestamp(Instant.now().minus(duration))
                .setAttribute("sid.clock", sid.clock())
                .startSpan()
                .end();
    }

    /** Without a recorded start the span starts at the failure. */
    private static void emitFailed(SpanBuilder builder, Instant start, Throwable cause) {
        if (start != null) {
            builder.setStartTimestamp(start);
        }
        Span span = builder.startSpan();
        if (cause != null) {
            span.setStatus(StatusCode.ERROR, cause.toString());
            span.recordException(cause);
        } else {
            span.setStatus(StatusCode.ERROR);
        }
        span.end();
    }
}
