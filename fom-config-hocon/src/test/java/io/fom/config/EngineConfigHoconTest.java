package io.fom.config;

import com.typesafe.config.ConfigFactory;
import io.fom.EngineConfig;
import io.fom.ReinitStrategy;
import io.fom.SnapshotPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EngineConfigHoconTest {

    @Test
    void parses_defaults_when_empty_config() {
        EngineConfig cfg = EngineConfigHocon.parse(ConfigFactory.empty());
        assertThat(cfg.initTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(cfg.queryTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(cfg.dedupWindow()).isEqualTo(Duration.ofMillis(100));
        assertThat(cfg.snapshotPolicy()).isEqualTo(SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    void overrides_init_timeout() {
        var cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.init.timeout = 6h
                """));
        assertThat(cfg.initTimeout()).isEqualTo(Duration.ofHours(6));
    }

    @Test
    void rotate_cron_never_is_disabled() {
        var cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate.cron = never
                engine.system.log.rotate.keep-history = 3
                """));
        assertThat(cfg.snapshotPolicy()).isEqualTo(SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    void rotate_cron_quartz_translates_to_cron_policy() {
        var cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate.cron = "0 0 3 * * ?"
                engine.system.log.rotate.keep-history = 7
                """));
        assertThat(cfg.snapshotPolicy()).isInstanceOf(CronSnapshotPolicy.class);
        var cron = (CronSnapshotPolicy) cfg.snapshotPolicy();
        assertThat(cron.keepHistory()).isEqualTo(7);
        assertThat(cron.expression()).isEqualTo("0 0 3 * * ?");
    }

    @Test
    void rotate_without_keep_history_keeps_all_archives() {
        var cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate.cron = "0 0 3 * * ?"
                """));
        var cron = (CronSnapshotPolicy) cfg.snapshotPolicy();
        assertThat(cron.keepHistory()).isEqualTo(SnapshotPolicy.KEEP_ALL);
    }

    @Test
    void rotate_keep_history_all_keeps_all_archives() {
        var cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate { cron = "0 0 3 * * ?", keep-history = all }
                """));
        assertThat(((CronSnapshotPolicy) cfg.snapshotPolicy()).keepHistory()).isEqualTo(SnapshotPolicy.KEEP_ALL);
    }

    @Test
    void invalid_cron_throws() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate.cron = "this is not a cron"
                """))).isInstanceOf(RuntimeException.class);
    }

    @Test
    void an_invalid_value_names_the_full_hocon_key() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.init.min-backoff = 0ms
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("engine.graph.default.init.min-backoff: ")
                .hasMessageNotContaining("backoffMin");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.init.min-backoff = 10s
                engine.graph.default.init.max-backoff = 1s
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("engine.graph.default.init.max-backoff: ")
                .hasMessageContaining("engine.graph.default.init.min-backoff")
                .hasMessageNotContaining("backoffMax");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.load.max-retries = 0
                """)))
                .hasMessageStartingWith("engine.graph.default.load.max-retries: ");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.query-timeout = -1s
                """)))
                .hasMessageStartingWith("engine.system.query-timeout: ");
    }

    @Test
    void a_malformed_value_names_the_full_hocon_key_and_keeps_the_cause() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.init.timeout = soon
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.graph.default.init.timeout: ")
                .hasCauseInstanceOf(com.typesafe.config.ConfigException.class);
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.load.max-retries = many
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.graph.default.load.max-retries: ");
    }

    @Test
    void rotate_errors_name_the_full_hocon_key() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate.cron = "this is not a cron"
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("engine.system.log.rotate.cron: ")
                .hasCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate.cron = "0 0 3 * * ?"
                engine.system.log.rotate.keep-history = 0
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("engine.system.log.rotate.keep-history: ")
                .hasMessageNotContaining("keepHistory");
    }

    @Test
    void scalar_rotate_is_rejected_not_silently_disabled() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate = "0 0 3 * * ?"
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.system.log.rotate: must be an object");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate = never
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.system.log.rotate: ");
    }

    @Test
    void a_scalar_namespace_is_reported_against_its_own_key() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system = "fast"
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.system: must be an object, was string");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default = 5
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.graph.default: must be an object, was number 5");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph = [1, 2]
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.graph: must be an object, was list");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.init = 30s
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.graph.default.init: must be an object, was string");
    }

    @Test
    void rotate_with_misspelled_cron_key_names_the_unknown_key() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate { corn = "0 0 3 * * ?", keep-history = 3 }
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.system.log.rotate.corn: unknown key 'corn'");
    }

    @Test
    void rotate_without_cron_is_rejected() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate { keep-history = 3 }
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.system.log.rotate.cron: missing");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate {}
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.system.log.rotate.cron: missing");
    }

    @Test
    void explicit_blank_cron_still_disables() {
        assertThat(EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate.cron = ""
                """)).snapshotPolicy()).isSameAs(io.fom.SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    void misspelled_parent_of_rotate_is_rejected_not_silently_disabled() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.logs.rotate { cron = "0 0 3 * * ?", keep-history = 3 }
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.system.logs: unknown key 'logs' in engine.system")
                .hasMessageContaining("allowed: dedup-window, log, query-timeout");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotation.cron = "0 0 3 * * ?"
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.system.log.rotation: unknown key 'rotation' in engine.system.log")
                .hasMessageContaining("allowed: rotate");
    }

    @Test
    void misspelled_timeout_key_is_rejected_not_silently_defaulted() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.query-timout = 1s
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.system.query-timout: unknown key 'query-timout'")
                .hasMessageContaining("allowed: dedup-window, log, query-timeout");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.init.timout = 5m
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.graph.default.init.timout: unknown key 'timout' in engine.graph.default.init")
                .hasMessageContaining("allowed: max-backoff, min-backoff, timeout");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.intit.timeout = 5m
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.graph.default.intit: unknown key 'intit'")
                .hasMessageContaining("allowed: cleanup, init, load");
    }

    @Test
    void a_valid_full_config_parses_and_foreign_keys_are_left_alone() {
        EngineConfig cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine {
                  graph.default {
                    init    { timeout = 1m, min-backoff = 10ms, max-backoff = 1m }
                    load    { timeout = 2m, max-retries = 3 }
                    cleanup.timeout = 3m
                  }
                  graph.my-own-setting = 42
                  my-app-key = "untouched"
                  system {
                    query-timeout = 4s
                    dedup-window = 50ms
                    log.rotate { cron = "0 0 */6 * * ?", keep-history = 5 }
                  }
                }
                myapp.system.query-timout = "not ours"
                """));
        assertThat(cfg.initTimeout()).isEqualTo(Duration.ofMinutes(1));
        assertThat(cfg.loadTimeout()).isEqualTo(Duration.ofMinutes(2));
        assertThat(cfg.cleanupTimeout()).isEqualTo(Duration.ofMinutes(3));
        assertThat(cfg.queryTimeout()).isEqualTo(Duration.ofSeconds(4));
        assertThat(cfg.dedupWindow()).isEqualTo(Duration.ofMillis(50));
        assertThat(cfg.backoffMin()).isEqualTo(Duration.ofMillis(10));
        assertThat(cfg.backoffMax()).isEqualTo(Duration.ofMinutes(1));
        assertThat(cfg.maxLoadRetries()).isEqualTo(3);
        assertThat(((CronSnapshotPolicy) cfg.snapshotPolicy()).keepHistory()).isEqualTo(5);
    }

    @Test
    void reinit_keys_default_to_keep_old_and_a_derived_retry_backoff() {
        EngineConfig cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.init.timeout = 2m
                """));
        assertThat(cfg.reinitStrategy()).isEqualTo(ReinitStrategy.KEEP_OLD);
        assertThat(cfg.reinitRetryBackoffMin()).as("max(init timeout, 30s)").isEqualTo(Duration.ofMinutes(2));
        assertThat(cfg.reinitRetryBackoffMax()).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void reinit_keys_are_read() {
        EngineConfig cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.reinit {
                  strategy = release-first
                  retry { min-backoff = 5s, max-backoff = 1m }
                }
                """));
        assertThat(cfg.reinitStrategy()).isEqualTo(ReinitStrategy.RELEASE_FIRST);
        assertThat(cfg.reinitRetryBackoffMin()).isEqualTo(Duration.ofSeconds(5));
        assertThat(cfg.reinitRetryBackoffMax()).isEqualTo(Duration.ofMinutes(1));

        EngineConfig off = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.reinit.retry.min-backoff = 0s
                """));
        assertThat(off.reinitRetryEnabled()).isFalse();
    }

    @Test
    void reinit_keys_are_validated_and_strict() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.reinit.strategy = " Sometimes"
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("engine.graph.default.reinit.strategy: must be keep-old or release-first, was  Sometimes");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.reinit.retry { min-backoff = 1m, max-backoff = 1s }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("engine.graph.default.reinit.retry.max-backoff: must be >= "
                        + "engine.graph.default.reinit.retry.min-backoff (PT1M), was PT1S");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.reinit.retry.min-backof = 1s
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageStartingWith("engine.graph.default.reinit.retry.min-backof: unknown key")
                .hasMessageContaining("allowed: max-backoff, min-backoff");
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.reinit.stratgy = keep-old
                """)))
                .isInstanceOf(com.typesafe.config.ConfigException.class)
                .hasMessageContaining("allowed: retry, strategy");
    }
}
