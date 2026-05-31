package io.fom.micrometer;

import io.fom.Sid;
import io.fom.api.EngineObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Records fom runtime metrics into a {@link MeterRegistry}. Metric names
 * follow §6.8 of the spec.
 *
 * <p>Counters: {@code engine_query_failures_total{name, reason}},
 * {@code engine_query_cancellations_total{name}},
 * {@code engine_dedup_collapsed_total{name}}.</p>
 *
 * <p>Timers: {@code engine_process_init_duration_seconds{name}},
 * {@code engine_process_load_duration_seconds{name}},
 * {@code engine_query_duration_seconds{name}},
 * {@code engine_process_cleanup_duration_seconds{name}},
 * {@code engine_process_compute_duration_seconds{name}}.</p>
 *
 * <p>Mailbox size and FSM state gauges are typically registered by the
 * engine container against the same registry, not from this observer
 * (which sees events but not steady-state).</p>
 */
public final class MicrometerEngineObserver implements EngineObserver {

    private final MeterRegistry registry;

    public MicrometerEngineObserver(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    @Override
    public void onInitCompleted(String processName, Sid newSid, Duration duration) {
        timer("engine_process_init_duration_seconds", processName).record(duration);
    }

    @Override
    public void onInitFailed(String processName, int attempt, Throwable cause) {
        counter("engine_query_failures_total", processName, "reason", "init-failed").increment();
    }

    @Override
    public void onLoadCompleted(String processName, Sid sid, Duration duration) {
        timer("engine_process_load_duration_seconds", processName).record(duration);
    }

    @Override
    public void onLoadFailed(String processName, Sid sid, int attempt, Throwable cause) {
        counter("engine_query_failures_total", processName, "reason", "load-failed").increment();
    }

    @Override
    public void onQueryCompleted(String processName, UUID queryId, Duration duration) {
        timer("engine_query_duration_seconds", processName).record(duration);
    }

    @Override
    public void onQueryFailed(String processName, UUID queryId, String reason, Throwable cause) {
        counter("engine_query_failures_total", processName, "reason", reason).increment();
        if ("cancelled".equals(reason)) {
            counter("engine_query_cancellations_total", processName).increment();
        }
    }

    @Override
    public void onComputeDuration(String processName, Duration duration) {
        timer("engine_process_compute_duration_seconds", processName).record(duration);
    }

    @Override
    public void onDedupCollapsed(String processName, int collapsedCount) {
        counter("engine_dedup_collapsed_total", processName).increment(collapsedCount);
    }

    @Override
    public void onCleanupCompleted(String processName, Sid sid, boolean ok, Duration duration) {
        timer("engine_process_cleanup_duration_seconds", processName).record(duration);
        if (!ok) {
            counter("engine_query_failures_total", processName, "reason", "cleanup-failed").increment();
        }
    }

    private Timer timer(String name, String processName) {
        return Timer.builder(name).tag("name", processName).register(registry);
    }

    private Counter counter(String name, String processName) {
        return Counter.builder(name).tag("name", processName).register(registry);
    }

    private Counter counter(String name, String processName, String tagKey, String tagValue) {
        return Counter.builder(name).tag("name", processName).tag(tagKey, tagValue).register(registry);
    }
}
