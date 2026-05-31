package io.fom.otel;

import io.fom.Sid;
import io.fom.api.EngineObserver;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * OpenTelemetry-based {@link EngineObserver}. Emits one span per init/load and
 * per top-level query, tagged with {@code process.name} and the {@code Sid}
 * clock.
 *
 * <p>For full call-tree tracing (a query span fanning out into dependency
 * sub-spans), the runtime would have to thread the parent context into each
 * V-thread; this observer covers the boundary events the engine itself fires.
 * Deeper instrumentation lives in user {@code Process} implementations.</p>
 */
public final class OtelEngineObserver implements EngineObserver {

    private final Tracer tracer;

    /** queryId → (span, startInstant) — kept until the query completes. */
    private final ConcurrentMap<UUID, Span> querySpans = new ConcurrentHashMap<>();

    public OtelEngineObserver(OpenTelemetry otel) {
        this(otel.getTracer("io.fom"));
    }

    public OtelEngineObserver(Tracer tracer) {
        this.tracer = Objects.requireNonNull(tracer, "tracer");
    }

    @Override
    public void onInitCompleted(String processName, Sid newSid, Duration duration) {
        Span span = tracer.spanBuilder("fom.init")
                .setSpanKind(SpanKind.INTERNAL)
                .setStartTimestamp(Instant.now().minus(duration))
                .setAttribute("process.name", processName)
                .setAttribute("sid.clock", newSid.clock())
                .startSpan();
        span.end();
    }

    @Override
    public void onInitFailed(String processName, int attempt, Throwable cause) {
        Span span = tracer.spanBuilder("fom.init")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("process.name", processName)
                .setAttribute("init.attempt", attempt)
                .startSpan();
        span.setStatus(StatusCode.ERROR, cause.toString());
        span.recordException(cause);
        span.end();
    }

    @Override
    public void onLoadCompleted(String processName, Sid sid, Duration duration) {
        Span span = tracer.spanBuilder("fom.load")
                .setSpanKind(SpanKind.INTERNAL)
                .setStartTimestamp(Instant.now().minus(duration))
                .setAttribute("process.name", processName)
                .setAttribute("sid.clock", sid.clock())
                .startSpan();
        span.end();
    }

    @Override
    public void onQuerySent(String processName, UUID queryId, Class<?> messageType) {
        Span span = tracer.spanBuilder("fom.query")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("process.name", processName)
                .setAttribute("query.id", queryId.toString())
                .setAttribute("query.type", messageType.getName())
                .startSpan();
        querySpans.put(queryId, span);
    }

    @Override
    public void onQueryCompleted(String processName, UUID queryId, Duration duration) {
        Span span = querySpans.remove(queryId);
        if (span != null) {
            span.end();
        }
    }

    @Override
    public void onQueryFailed(String processName, UUID queryId, String reason, Throwable cause) {
        Span span = querySpans.remove(queryId);
        if (span != null) {
            span.setStatus(StatusCode.ERROR, reason);
            if (cause != null) {
                span.recordException(cause);
            }
            span.end();
        }
    }
}
