package io.fom.micrometer;

import io.fom.Sid;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerEngineObserverTest {

    @Test
    void records_init_load_query_and_dedup_metrics() {
        var registry = new SimpleMeterRegistry();
        var observer = new MicrometerEngineObserver(registry);
        var sid = new Sid("Echo", 1);

        observer.onInitCompleted("Echo", sid, Duration.ofMillis(20));
        observer.onLoadCompleted("Echo", sid, Duration.ofMillis(5));
        observer.onQueryCompleted("Echo", UUID.randomUUID(), Duration.ofMillis(2));
        observer.onComputeDuration("Echo", Duration.ofMillis(1));
        observer.onDedupCollapsed("Echo", 4);
        observer.onQueryFailed("Echo", UUID.randomUUID(), "timeout", new RuntimeException("boom"));
        observer.onCleanupCompleted("Echo", sid, true, Duration.ofMillis(3));

        assertThat(registry.find("engine_process_init_duration_seconds").tag("name", "Echo").timer().count())
                .isEqualTo(1);
        assertThat(registry.find("engine_process_load_duration_seconds").tag("name", "Echo").timer().count())
                .isEqualTo(1);
        assertThat(registry.find("engine_query_duration_seconds").tag("name", "Echo").timer().count())
                .isEqualTo(1);
        assertThat(registry.find("engine_dedup_collapsed_total").tag("name", "Echo").counter().count())
                .isEqualTo(4.0);
        assertThat(registry.find("engine_query_failures_total")
                .tag("name", "Echo").tag("reason", "timeout").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("engine_process_cleanup_duration_seconds").tag("name", "Echo").timer().count())
                .isEqualTo(1);
    }
}
