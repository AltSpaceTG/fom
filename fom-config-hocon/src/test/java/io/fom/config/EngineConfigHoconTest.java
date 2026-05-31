package io.fom.config;

import com.typesafe.config.ConfigFactory;
import io.fom.EngineConfig;
import io.fom.SnapshotPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EngineConfigHoconTest {

    @Test
    void parses_defaults_when_empty_config() {
        EngineConfig cfg = EngineConfigHocon.parse(ConfigFactory.empty());
        assertThat(cfg.defaultInitTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(cfg.queryTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(cfg.dedupWindow()).isEqualTo(Duration.ofMillis(100));
        assertThat(cfg.snapshotPolicy()).isEqualTo(SnapshotPolicy.Disabled.INSTANCE);
    }

    @Test
    void overrides_init_timeout() {
        var cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.graph.default.init.timeout = 6h
                """));
        assertThat(cfg.defaultInitTimeout()).isEqualTo(Duration.ofHours(6));
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
    void rotate_cron_quartz_translates_to_fixed_interval() {
        var cfg = EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate.cron = "0 0 3 * * ?"
                engine.system.log.rotate.keep-history = 7
                """));
        assertThat(cfg.snapshotPolicy()).isInstanceOf(SnapshotPolicy.FixedInterval.class);
        var fi = (SnapshotPolicy.FixedInterval) cfg.snapshotPolicy();
        assertThat(fi.keepHistory()).isEqualTo(7);
        assertThat(fi.interval()).isPositive();
    }

    @Test
    void invalid_cron_throws() {
        assertThatThrownBy(() -> EngineConfigHocon.parse(ConfigFactory.parseString("""
                engine.system.log.rotate.cron = "this is not a cron"
                """))).isInstanceOf(RuntimeException.class);
    }
}
