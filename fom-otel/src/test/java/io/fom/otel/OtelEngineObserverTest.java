package io.fom.otel;

import io.fom.Engine;
import io.fom.EngineConfig;
import io.fom.GraphBuilder;
import io.fom.Sid;
import io.fom.SnapshotPolicy;
import io.fom.api.ProcessInitializer;
import io.fom.api.ProcessLoader;
import io.fom.api.QueryableContext;
import io.fom.log.InMemoryLogBackend;
import io.fom.serde.JavaSerializableSerDe;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OtelEngineObserverTest {

    @Test
    void emits_spans_for_init_load_and_query() {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        var tracer = provider.tracerBuilder("test").build();
        var observer = new OtelEngineObserver(tracer);

        var sid = new Sid("Echo", 3);
        observer.onInitCompleted("Echo", sid, Duration.ofMillis(15));
        observer.onLoadCompleted("Echo", sid, Duration.ofMillis(2));

        var queryId = UUID.randomUUID();
        observer.onQuerySent("Echo", queryId, String.class, null);
        observer.onQueryCompleted("Echo", queryId, Duration.ofMillis(1));

        provider.forceFlush().join(2, java.util.concurrent.TimeUnit.SECONDS);
        var spans = exporter.getFinishedSpanItems();
        assertThat(spans).extracting(io.opentelemetry.sdk.trace.data.SpanData::getName)
                .contains("fom.init", "fom.load", "fom.query");
    }

    @Test
    void removal_mid_attempt_drops_the_start_entries_and_emits_no_span() {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        var observer = new OtelEngineObserver(provider.tracerBuilder("test").build());
        // Forwarded through the composite, as an engine with metrics + tracing wires it.
        var composite = io.fom.api.EngineObserver.composite(io.fom.api.EngineObserver.NOOP, observer);

        // Tenant-shaped names removed mid-init / mid-load, with no terminal callback ever arriving.
        for (int i = 0; i < 1_000; i++) {
            String name = "tenant-" + i;
            if (i % 2 == 0) {
                composite.onInitStarted(name, 1);
            } else {
                composite.onLoadStarted(name, new Sid(name, 1), 1);
            }
            composite.onProcessRemoved(name);
        }

        assertThat(observer.inFlightStarts()).isZero();
        provider.forceFlush().join(2, TimeUnit.SECONDS);
        assertThat(exporter.getFinishedSpanItems()).isEmpty();

        // A load failure the engine reports before the removal keeps its real start.
        composite.onLoadStarted("P", new Sid("P", 1), 1);
        composite.onLoadFailed("P", new Sid("P", 1), 1, new IllegalStateException("removed"));
        composite.onProcessRemoved("P");
        provider.forceFlush().join(2, TimeUnit.SECONDS);
        assertThat(exporter.getFinishedSpanItems()).singleElement()
                .satisfies(s -> assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR));
        assertThat(observer.inFlightStarts()).isZero();
    }

    static final class Node implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) ->
                    "hang".equals(q) ? new CompletableFuture<>() : CompletableFuture.completedFuture("ok"));
        }
    }

    private static Engine engine(OtelEngineObserver observer) {
        var cfg = new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofMillis(500),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        var engine = new Engine(cfg, new InMemoryLogBackend(), new JavaSerializableSerDe(), observer);
        engine.newGraph(new GraphBuilder().add("Echo", Node::new, Node::new).build());
        return engine;
    }

    @Test
    void query_span_is_a_child_of_the_callers_current_span() throws Exception {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        var tracer = provider.tracerBuilder("test").build();
        Span request = tracer.spanBuilder("http.request").startSpan();
        try (var engine = engine(new OtelEngineObserver(tracer))) {
            Scope scope = request.makeCurrent();
            try {
                engine.queryProcess("Echo", "q").toCompletableFuture().get(5, TimeUnit.SECONDS);
            } finally {
                scope.close();
            }
        } finally {
            request.end();
        }
        provider.forceFlush().join(2, TimeUnit.SECONDS);
        assertThat(exporter.getFinishedSpanItems()).filteredOn(s -> s.getName().equals("fom.query"))
                .singleElement()
                .satisfies(s -> {
                    assertThat(s.getParentSpanId()).isEqualTo(request.getSpanContext().getSpanId());
                    assertThat(s.getTraceId()).isEqualTo(request.getSpanContext().getTraceId());
                });
        provider.close();
    }

    static final class Front implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> c.query("Echo", q));
        }
    }

    @Test
    void a_dependency_query_span_is_a_child_of_the_query_it_serves() throws Exception {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        var tracer = provider.tracerBuilder("test").build();
        var cfg = new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofMillis(500),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        Span request = tracer.spanBuilder("http.request").startSpan();
        try (var engine = new Engine(cfg, new InMemoryLogBackend(), new JavaSerializableSerDe(),
                new OtelEngineObserver(tracer))) {
            engine.newGraph(new GraphBuilder()
                    .add("Echo", Node::new, Node::new)
                    .add("Front", Front::new, Front::new, "Echo")
                    .build());
            Scope scope = request.makeCurrent();
            try {
                assertThat(engine.queryProcess("Front", "q").toCompletableFuture().get(5, TimeUnit.SECONDS))
                        .isEqualTo("ok");
            } finally {
                scope.close();
            }
        } finally {
            request.end();
        }
        provider.forceFlush().join(2, TimeUnit.SECONDS);
        var spans = exporter.getFinishedSpanItems();
        var processName = io.opentelemetry.api.common.AttributeKey.stringKey("process.name");
        var front = spans.stream().filter(s -> s.getName().equals("fom.query"))
                .filter(s -> "Front".equals(s.getAttributes().get(processName))).findFirst().orElseThrow();
        var echo = spans.stream().filter(s -> s.getName().equals("fom.query"))
                .filter(s -> "Echo".equals(s.getAttributes().get(processName))).findFirst().orElseThrow();
        assertThat(front.getParentSpanId()).isEqualTo(request.getSpanContext().getSpanId());
        assertThat(echo.getParentSpanId()).isEqualTo(front.getSpanId());
        assertThat(echo.getTraceId()).isEqualTo(request.getSpanContext().getTraceId());
        provider.close();
    }

    /** Answers at once, then asks Echo from an async continuation — after its own query settled. */
    static final class LateFront implements ProcessInitializer, ProcessLoader {
        static final CompletableFuture<Object> lateReply = new CompletableFuture<>();

        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> {
                CompletableFuture.runAsync(() -> { }, CompletableFuture.delayedExecutor(200, TimeUnit.MILLISECONDS))
                        .thenCompose(v -> c.query("Echo", q))
                        .whenComplete((r, e) -> {
                            if (e != null) lateReply.completeExceptionally(e);
                            else lateReply.complete(r);
                        });
                return CompletableFuture.completedFuture("ack");
            });
        }
    }

    @Test
    void a_dependency_query_sent_after_its_parent_settled_is_still_its_child() throws Exception {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        var tracer = provider.tracerBuilder("test").build();
        var cfg = new EngineConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofMillis(500),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        try (var engine = new Engine(cfg, new InMemoryLogBackend(), new JavaSerializableSerDe(),
                new OtelEngineObserver(tracer))) {
            engine.newGraph(new GraphBuilder()
                    .add("Echo", Node::new, Node::new)
                    .add("Front", LateFront::new, LateFront::new, "Echo")
                    .build());
            assertThat(engine.queryProcess("Front", "q").toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isEqualTo("ack");
            assertThat(LateFront.lateReply.get(5, TimeUnit.SECONDS)).isEqualTo("ok");
        }
        provider.forceFlush().join(2, TimeUnit.SECONDS);
        var spans = exporter.getFinishedSpanItems();
        var processName = io.opentelemetry.api.common.AttributeKey.stringKey("process.name");
        var front = spans.stream().filter(s -> s.getName().equals("fom.query"))
                .filter(s -> "Front".equals(s.getAttributes().get(processName))).findFirst().orElseThrow();
        var echo = spans.stream().filter(s -> s.getName().equals("fom.query"))
                .filter(s -> "Echo".equals(s.getAttributes().get(processName))).findFirst().orElseThrow();
        assertThat(echo.getStartEpochNanos()).isGreaterThan(front.getEndEpochNanos());
        assertThat(echo.getParentSpanId()).isEqualTo(front.getSpanId());
        assertThat(echo.getTraceId()).isEqualTo(front.getTraceId());
        provider.close();
    }

    @Test
    void ended_parents_are_bounded_by_count_and_age() {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        var now = new java.util.concurrent.atomic.AtomicLong();
        var observer = new OtelEngineObserver(provider.tracerBuilder("test").build(), Duration.ofMinutes(5), 3, now::get);

        UUID[] ids = new UUID[5];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = UUID.randomUUID();
            observer.onQuerySent("P", ids[i], String.class, null);
            if (i % 2 == 0) observer.onQueryCompleted("P", ids[i], Duration.ofMillis(1));
            else observer.onQueryFailed("P", ids[i], "exception", new IllegalStateException());
        }
        assertThat(observer.rememberedEndedQueries()).isEqualTo(3);

        // ids[0] was evicted by the cap: a late child starts a new trace; ids[4] still parents.
        UUID orphan = UUID.randomUUID();
        UUID child = UUID.randomUUID();
        observer.onQuerySent("Q", orphan, String.class, ids[0]);
        observer.onQuerySent("Q", child, String.class, ids[4]);
        // Past the retention: forgotten even without new removals.
        now.addAndGet(Duration.ofMinutes(6).toNanos());
        UUID tooLate = UUID.randomUUID();
        observer.onQuerySent("Q", tooLate, String.class, ids[4]);
        observer.onQueryCompleted("Q", orphan, Duration.ofMillis(1));
        observer.onQueryCompleted("Q", child, Duration.ofMillis(1));
        observer.onQueryCompleted("Q", tooLate, Duration.ofMillis(1));
        // Each end sweeps expired entries: only the three just-ended ones remain.
        assertThat(observer.rememberedEndedQueries()).isEqualTo(3);

        var queryId = io.opentelemetry.api.common.AttributeKey.stringKey("query.id");
        var spans = exporter.getFinishedSpanItems();
        java.util.function.Function<UUID, io.opentelemetry.sdk.trace.data.SpanData> span = id -> spans.stream()
                .filter(s -> id.toString().equals(s.getAttributes().get(queryId))).findFirst().orElseThrow();
        assertThat(span.apply(child).getParentSpanId()).isEqualTo(span.apply(ids[4]).getSpanId());
        assertThat(span.apply(orphan).getParentSpanContext().isValid()).isFalse();
        assertThat(span.apply(tooLate).getParentSpanContext().isValid()).isFalse();
        provider.close();
    }

    @Test
    void query_span_ends_when_the_caller_times_out() throws Exception {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        try (var engine = engine(new OtelEngineObserver(provider.tracerBuilder("test").build()))) {
            var reply = engine.queryProcess("Echo", "hang", Duration.ofMillis(100)).toCompletableFuture();
            assertThatThrownBy(() -> reply.get(3, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (exporter.getFinishedSpanItems().stream().noneMatch(s -> s.getName().equals("fom.query"))
                    && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
        }
        assertThat(exporter.getFinishedSpanItems()).filteredOn(s -> s.getName().equals("fom.query"))
                .singleElement()
                .satisfies(s -> {
                    assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
                    assertThat(s.getStatus().getDescription()).isEqualTo("timeout");
                });
        provider.close();
    }

    @Test
    void failure_spans_start_at_the_matching_started_callback() throws Exception {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        var observer = new OtelEngineObserver(provider.tracerBuilder("test").build());

        observer.onInitStarted("P", 1);
        Thread.sleep(60);
        observer.onInitFailed("P", 1, new RuntimeException("init boom"));

        var sid = new Sid("P", 2);
        observer.onLoadStarted("P", sid, 1);
        Thread.sleep(60);
        observer.onLoadFailed("P", sid, 1, new RuntimeException("load boom"));

        provider.forceFlush().join(2, java.util.concurrent.TimeUnit.SECONDS);
        var spans = exporter.getFinishedSpanItems();
        for (String name : new String[]{"fom.init", "fom.load"}) {
            assertThat(spans).filteredOn(s -> s.getName().equals(name)).singleElement().satisfies(s -> {
                assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
                assertThat(s.getEndEpochNanos() - s.getStartEpochNanos())
                        .as("%s failure span duration", name)
                        .isGreaterThanOrEqualTo(Duration.ofMillis(50).toNanos());
            });
        }
        provider.close();
    }

    @Test
    void a_reinit_span_runs_from_start_to_promotion() throws Exception {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        var observer = new OtelEngineObserver(provider.tracerBuilder("test").build());

        observer.onSidPromotion("P", null, new Sid("P", 1)); // a cold start: no re-init span
        observer.onReinitStarted("P", new Sid("P", 1));
        Thread.sleep(60);
        observer.onSidPromotion("P", new Sid("P", 1), new Sid("P", 4));

        provider.forceFlush().join(2, TimeUnit.SECONDS);
        assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(s -> {
            assertThat(s.getName()).isEqualTo("fom.reinit");
            assertThat(s.getStatus().getStatusCode()).isNotEqualTo(StatusCode.ERROR);
            assertThat(s.getAttributes().asMap())
                    .containsEntry(io.opentelemetry.api.common.AttributeKey.stringKey("process.name"), "P")
                    .containsEntry(io.opentelemetry.api.common.AttributeKey.longKey("sid.serving.clock"), 1L)
                    .containsEntry(io.opentelemetry.api.common.AttributeKey.longKey("sid.new.clock"), 4L);
            assertThat(s.getEndEpochNanos() - s.getStartEpochNanos())
                    .isGreaterThanOrEqualTo(Duration.ofMillis(50).toNanos());
        });
        assertThat(observer.inFlightStarts()).isZero();
        provider.close();
    }

    @Test
    void a_failed_reinit_span_is_an_error_with_the_exception() throws Exception {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        var observer = new OtelEngineObserver(provider.tracerBuilder("test").build());

        observer.onReinitStarted("P", new Sid("P", 2));
        Thread.sleep(60);
        observer.onReinitFailed("P", new Sid("P", 2), new IllegalStateException("upstream down"));
        // The old version still serves: a later promotion without a new start emits nothing more.
        observer.onSidPromotion("P", new Sid("P", 2), new Sid("P", 3));

        provider.forceFlush().join(2, TimeUnit.SECONDS);
        assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(s -> {
            assertThat(s.getName()).isEqualTo("fom.reinit");
            assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
            assertThat(s.getEvents()).anySatisfy(e -> assertThat(e.getName()).isEqualTo("exception"));
            assertThat(s.getAttributes().get(io.opentelemetry.api.common.AttributeKey.longKey("sid.serving.clock")))
                    .isEqualTo(2L);
            assertThat(s.getAttributes().get(io.opentelemetry.api.common.AttributeKey.longKey("sid.new.clock")))
                    .isNull();
            assertThat(s.getEndEpochNanos() - s.getStartEpochNanos())
                    .isGreaterThanOrEqualTo(Duration.ofMillis(50).toNanos());
        });
        provider.close();
    }

    @Test
    void a_reinit_cut_short_by_removal_or_shutdown_emits_no_span() {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        var observer = new OtelEngineObserver(provider.tracerBuilder("test").build());

        observer.onReinitStarted("Removed", new Sid("Removed", 1));
        observer.onProcessRemoved("Removed");

        observer.onReinitStarted("Paused", new Sid("Paused", 1));
        observer.onStateTransition("Paused", "Serving", "CleaningUp");
        observer.onSidPromotion("Paused", new Sid("Paused", 1), new Sid("Paused", 2)); // after the resume

        assertThat(observer.inFlightStarts()).isZero();
        provider.forceFlush().join(2, TimeUnit.SECONDS);
        assertThat(exporter.getFinishedSpanItems()).isEmpty();
        provider.close();
    }

    static final java.util.concurrent.atomic.AtomicBoolean initFails = new java.util.concurrent.atomic.AtomicBoolean();

    /** Init fails while {@link #initFails} is set. */
    static final class FailsOnce implements ProcessInitializer, ProcessLoader {
        @Override
        public CompletionStage<Map<String, byte[]>> init(QueryableContext ctx) {
            return initFails.get()
                    ? CompletableFuture.failedFuture(new IllegalStateException("upstream down"))
                    : CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletionStage<io.fom.api.Process> load(QueryableContext ctx, Map<String, byte[]> props) {
            return CompletableFuture.completedFuture((c, q) -> CompletableFuture.completedFuture("ok"));
        }
    }

    @Test
    void an_engine_reinit_emits_an_error_span_then_an_ok_span() throws Exception {
        var exporter = InMemorySpanExporter.create();
        var provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
        var observer = new OtelEngineObserver(provider.tracerBuilder("test").build());
        initFails.set(false);
        var cfg = new EngineConfig(
                Duration.ofMillis(300), Duration.ofSeconds(5), Duration.ofMillis(500),
                Duration.ofSeconds(5), Duration.ofMillis(10),
                Duration.ofMillis(10), Duration.ofMillis(50), 1,
                SnapshotPolicy.Disabled.INSTANCE).withReinitRetryBackoff(Duration.ZERO, null);
        try (var engine = new Engine(cfg, new InMemoryLogBackend(), new JavaSerializableSerDe(), observer)) {
            engine.newGraph(new GraphBuilder().add("P", FailsOnce::new, FailsOnce::new).build());
            initFails.set(true);
            engine.trigger("P", "refresh");
            awaitReinitSpans(provider, exporter, 1);
            initFails.set(false);
            engine.trigger("P", "refresh again");
            awaitReinitSpans(provider, exporter, 2);
        }
        var spans = exporter.getFinishedSpanItems().stream().filter(s -> s.getName().equals("fom.reinit")).toList();
        assertThat(spans.get(0).getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(spans.get(1).getStatus().getStatusCode()).isNotEqualTo(StatusCode.ERROR);
        long serving = spans.get(1).getAttributes().get(io.opentelemetry.api.common.AttributeKey.longKey("sid.serving.clock"));
        long fresh = spans.get(1).getAttributes().get(io.opentelemetry.api.common.AttributeKey.longKey("sid.new.clock"));
        assertThat(fresh).isGreaterThan(serving);
        provider.close();
    }

    private static void awaitReinitSpans(SdkTracerProvider provider, InMemorySpanExporter exporter, int n)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (exporter.getFinishedSpanItems().stream().filter(s -> s.getName().equals("fom.reinit")).count() < n) {
            if (System.nanoTime() > deadline) throw new AssertionError("no fom.reinit span #" + n);
            provider.forceFlush().join(1, TimeUnit.SECONDS);
            Thread.sleep(10);
        }
    }
}
