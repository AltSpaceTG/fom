package io.fom.otel;

import io.fom.Sid;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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
        observer.onQuerySent("Echo", queryId, String.class);
        observer.onQueryCompleted("Echo", queryId, Duration.ofMillis(1));

        provider.forceFlush().join(2, java.util.concurrent.TimeUnit.SECONDS);
        var spans = exporter.getFinishedSpanItems();
        assertThat(spans).extracting(io.opentelemetry.sdk.trace.data.SpanData::getName)
                .contains("fom.init", "fom.load", "fom.query");
    }
}
