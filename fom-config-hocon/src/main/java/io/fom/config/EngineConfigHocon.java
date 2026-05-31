package io.fom.config;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.fom.EngineConfig;
import io.fom.SnapshotPolicy;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * Parses {@link EngineConfig} from a Typesafe {@link Config}. Layout follows
 * §6.9 of the spec; cron syntax is Quartz 6-field
 * ({@code "<sec> <min> <hour> <day-of-month> <month> <day-of-week>"}).
 *
 * <p>Reads from {@code engine.system.*} and {@code engine.graph.default.*}.
 * Per-process overrides remain a future addition (Engine doesn't yet apply them).</p>
 *
 * <p>Cron values translate into {@link SnapshotPolicy.FixedInterval} by
 * computing the next-fire {@link Duration} from "now" — sufficient for the
 * Stage 3 fixed-interval scheduler. Real cron-driven scheduling (proper
 * "next fire time" recomputation) lives here too if needed.</p>
 */
public final class EngineConfigHocon {

    private static final CronParser QUARTZ = new CronParser(
            CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    private EngineConfigHocon() {
    }

    public static EngineConfig parse(Config raw) {
        Objects.requireNonNull(raw, "raw");
        Config cfg = raw.withFallback(defaults());
        Config defaults = cfg.getConfig("engine.graph.default");
        Config system = cfg.getConfig("engine.system");

        SnapshotPolicy snapshot = parseSnapshotPolicy(system);

        return new EngineConfig(
                defaults.getDuration("init.timeout"),
                defaults.getDuration("load.timeout"),
                defaults.getDuration("cleanup.timeout"),
                defaults.getDuration("compute.timeout"),
                defaults.getDuration("cancel-init.timeout"),
                system.getDuration("query-timeout"),
                system.getDuration("log.look-up-interval"),
                system.getDuration("dedup-window"),
                defaults.getDuration("init.min-backoff"),
                defaults.getDuration("init.max-backoff"),
                defaults.getInt("load.max-retries"),
                snapshot);
    }

    public static EngineConfig parse() {
        return parse(ConfigFactory.load());
    }

    private static SnapshotPolicy parseSnapshotPolicy(Config system) {
        if (!system.hasPath("log.rotate")) return SnapshotPolicy.Disabled.INSTANCE;
        Config rotate = system.getConfig("log.rotate");
        String cron = rotate.hasPath("cron") ? rotate.getString("cron") : "never";
        if (cron.equalsIgnoreCase("never") || cron.isBlank()) {
            return SnapshotPolicy.Disabled.INSTANCE;
        }
        int keep = rotate.hasPath("keep-history") ? rotate.getInt("keep-history") : 7;
        Duration interval = nextFireInterval(cron);
        return new SnapshotPolicy.FixedInterval(interval, keep);
    }

    /** Quartz cron → next-fire {@link Duration} from {@code ZonedDateTime.now()}. */
    public static Duration nextFireInterval(String quartzCron) {
        Objects.requireNonNull(quartzCron, "quartzCron");
        Cron cron = QUARTZ.parse(quartzCron);
        ExecutionTime exec = ExecutionTime.forCron(cron);
        ZonedDateTime now = ZonedDateTime.now();
        Optional<Duration> next = exec.timeToNextExecution(now);
        return next.orElseThrow(() ->
                new IllegalArgumentException("Cron has no future execution: " + quartzCron));
    }

    private static Config defaults() {
        // Matches §6.9 baseline; user config can override any path.
        return ConfigFactory.parseString("""
                engine {
                  graph.default {
                    init {
                      timeout = 30s
                      min-backoff = 50ms
                      max-backoff = 5m
                    }
                    load {
                      timeout = 30s
                      max-retries = 1
                    }
                    cleanup.timeout = 30s
                    compute.timeout = 60m
                    cancel-init.timeout = 60s
                  }
                  system {
                    query-timeout = 10s
                    log {
                      look-up-interval = 100ms
                    }
                    dedup-window = 100ms
                  }
                }
                """);
    }
}
