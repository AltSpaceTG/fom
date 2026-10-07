package io.fom;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EngineConfigReinitTest {

    @Test
    void defaults_keep_the_old_version_and_retry_after_the_init_budget_or_30s() {
        var cfg = EngineConfig.defaults();
        assertThat(cfg.reinitStrategy()).isEqualTo(ReinitStrategy.KEEP_OLD);
        assertThat(cfg.reinitRetryBackoffMin()).isEqualTo(Duration.ofSeconds(30));
        assertThat(cfg.reinitRetryBackoffMax()).isEqualTo(Duration.ofMinutes(10));
        assertThat(cfg.reinitRetryEnabled()).isTrue();

        var longInit = cfg.withInitTimeout(Duration.ofMinutes(2));
        assertThat(longInit.reinitRetryBackoffMin()).as("a derived delay follows the init budget")
                .isEqualTo(Duration.ofMinutes(2));
        var pinned = cfg.withReinitRetryBackoff(Duration.ofSeconds(5), Duration.ofSeconds(50))
                .withInitTimeout(Duration.ofMinutes(2));
        assertThat(pinned.reinitRetryBackoffMin()).as("an explicit one stays").isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void the_nine_value_constructor_takes_the_defaults_and_withers_keep_the_new_fields() {
        var cfg = new EngineConfig(Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1),
                Duration.ofSeconds(1), Duration.ofMillis(10), Duration.ofMillis(10), Duration.ofMillis(20), 1,
                SnapshotPolicy.Disabled.INSTANCE);
        assertThat(cfg.reinitStrategy()).isEqualTo(ReinitStrategy.KEEP_OLD);
        assertThat(cfg.reinitRetryBackoffMin()).isEqualTo(Duration.ofSeconds(30));

        var custom = cfg.withReinitStrategy(ReinitStrategy.RELEASE_FIRST)
                .withReinitRetryBackoff(Duration.ZERO, null)
                .withQueryTimeout(Duration.ofSeconds(3))
                .withDedupWindow(Duration.ofMillis(5))
                .withSnapshotPolicy(SnapshotPolicy.Disabled.INSTANCE);
        assertThat(custom.reinitStrategy()).isEqualTo(ReinitStrategy.RELEASE_FIRST);
        assertThat(custom.reinitRetryEnabled()).isFalse();
    }

    @Test
    void invalid_retry_bounds_are_refused() {
        var cfg = EngineConfig.defaults();
        assertThatThrownBy(() -> cfg.withReinitRetryBackoff(Duration.ofSeconds(-1), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cfg.withReinitRetryBackoff(Duration.ofSeconds(10), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static EngineConfig withRetry(Duration min, Duration max) {
        var d = EngineConfig.defaults();
        return new EngineConfig(d.initTimeout(), d.loadTimeout(), d.cleanupTimeout(), d.queryTimeout(),
                d.dedupWindow(), d.backoffMin(), d.backoffMax(), d.maxLoadRetries(), d.snapshotPolicy(),
                min, max, null);
    }

    @Test
    void an_explicit_max_survives_a_new_init_budget_when_the_min_is_derived() {
        var cfg = withRetry(null, Duration.ofMinutes(20)).withInitTimeout(Duration.ofMinutes(1));
        assertThat(cfg.reinitRetryBackoffMin()).isEqualTo(Duration.ofMinutes(1));
        assertThat(cfg.reinitRetryBackoffMax()).isEqualTo(Duration.ofMinutes(20));
    }

    @Test
    void an_explicit_min_equal_to_the_derived_value_stays_explicit() {
        var cfg = withRetry(Duration.ofSeconds(30), null).withInitTimeout(Duration.ofMinutes(2));
        assertThat(cfg.reinitRetryBackoffMin()).isEqualTo(Duration.ofSeconds(30));
        assertThat(cfg.reinitRetryBackoffMax()).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void derived_bounds_follow_the_init_budget() {
        var cfg = EngineConfig.defaults().withInitTimeout(Duration.ofMinutes(20));
        assertThat(cfg.reinitRetryBackoffMin()).isEqualTo(Duration.ofMinutes(20));
        assertThat(cfg.reinitRetryBackoffMax()).isEqualTo(Duration.ofMinutes(20));
        var back = cfg.withInitTimeout(Duration.ofSeconds(5));
        assertThat(back.reinitRetryBackoffMin()).isEqualTo(Duration.ofSeconds(30));
        assertThat(back.reinitRetryBackoffMax()).isEqualTo(Duration.ofMinutes(10));
        var capped = withRetry(null, Duration.ofMinutes(1)).withInitTimeout(Duration.ofMinutes(5));
        assertThat(capped.reinitRetryBackoffMin()).as("a derived min stays within an explicit max")
                .isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void an_explicit_zero_max_is_refused_and_null_means_derived() {
        var cfg = EngineConfig.defaults();
        assertThatThrownBy(() -> cfg.withReinitRetryBackoff(Duration.ZERO, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("reinitRetryBackoffMax");
        var off = cfg.withReinitRetryBackoff(Duration.ZERO, null);
        assertThat(off.reinitRetryEnabled()).isFalse();
        var derivedMin = cfg.withReinitRetryBackoff(null, Duration.ofMinutes(1));
        assertThat(derivedMin.reinitRetryBackoffMin()).isEqualTo(Duration.ofSeconds(30));
        assertThat(derivedMin.reinitRetryBackoffMax()).isEqualTo(Duration.ofMinutes(1));
        var bothDerived = cfg.withReinitRetryBackoff(Duration.ofSeconds(5), Duration.ofSeconds(50))
                .withReinitRetryBackoff(null, null).withInitTimeout(Duration.ofMinutes(2));
        assertThat(bothDerived.reinitRetryBackoffMin()).isEqualTo(Duration.ofMinutes(2));
    }
}
