package io.fom;

import io.fom.log.FileLogBackend;
import io.fom.log.LogBackend;
import io.fom.serde.JavaSerializableSerDe;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Archive retention: by default no policy deletes an archive; a finite keepHistory still purges. */
class ArchiveRetentionTest {

    private static long archives(Path dir) throws Exception {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().matches("fom\\.log\\.archived\\.\\d+")).count();
        }
    }

    @Test
    void convenience_constructors_keep_all() {
        assertThat(new SnapshotPolicy.FixedInterval(Duration.ofHours(1)).keepHistory())
                .isEqualTo(SnapshotPolicy.KEEP_ALL);
        assertThat(new SnapshotPolicy.FixedInterval(Duration.ofHours(1)).purgesArchives()).isFalse();
        assertThat(new SnapshotPolicy.FixedInterval(Duration.ofHours(1), 3).purgesArchives()).isTrue();
        assertThat(new SizeBasedSnapshotPolicy(10, Duration.ofSeconds(1)).keepHistory())
                .isEqualTo(SnapshotPolicy.KEEP_ALL);
        assertThatThrownBy(() -> new SnapshotPolicy.FixedInterval(Duration.ofHours(1), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SizeBasedSnapshotPolicy(10, Duration.ofSeconds(1), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void default_fixed_interval_keeps_every_archive_on_disk(@TempDir Path tmp) throws Exception {
        var policy = new SnapshotPolicy.FixedInterval(Duration.ofMillis(100));
        try (LogBackend backend = new FileLogBackend(tmp.resolve("fom.log"));
             Engine engine = new Engine(SizeBasedSnapshotPolicyTest.configWith(policy), backend,
                     new JavaSerializableSerDe())) {
            engine.newGraph(SizeBasedSnapshotPolicyTest.singleNodeGraph());
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> archives(tmp) >= 4);
        }
        assertThat(archives(tmp)).isGreaterThanOrEqualTo(4);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void default_policies_never_call_purge_on_the_backend() throws Exception {
        var policy = new CompositeSnapshotPolicy(
                new SnapshotPolicy.FixedInterval(Duration.ofMillis(50)),
                new SizeBasedSnapshotPolicy(1, Duration.ofMillis(20)));
        try (CountingLogBackend backend = new CountingLogBackend();
             Engine engine = new Engine(SizeBasedSnapshotPolicyTest.configWith(policy), backend,
                     new JavaSerializableSerDe())) {
            engine.newGraph(SizeBasedSnapshotPolicyTest.singleNodeGraph());
            for (int i = 0; i < 12; i++) engine.trigger("Echo", "tick-" + i);
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> backend.compacts.get() >= 4);
            assertThat(backend.purges).hasValue(0);
        }
        try (CountingLogBackend backend = new CountingLogBackend();
             Engine engine = new Engine(SizeBasedSnapshotPolicyTest.configWith(
                     new SnapshotPolicy.FixedInterval(Duration.ofMillis(50))), backend, new JavaSerializableSerDe())) {
            engine.newGraph(SizeBasedSnapshotPolicyTest.singleNodeGraph());
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> backend.compacts.get() >= 3);
            assertThat(backend.purges).hasValue(0);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void explicit_keep_history_still_purges(@TempDir Path tmp) throws Exception {
        var policy = new SnapshotPolicy.FixedInterval(Duration.ofMillis(100), 1);
        try (CountingLogBackend counting = new CountingLogBackend();
             Engine engine = new Engine(SizeBasedSnapshotPolicyTest.configWith(
                     new SizeBasedSnapshotPolicy(1, Duration.ofMillis(20), 2)), counting, new JavaSerializableSerDe())) {
            engine.newGraph(SizeBasedSnapshotPolicyTest.singleNodeGraph());
            for (int i = 0; i < 6; i++) engine.trigger("Echo", "tick-" + i);
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> counting.purges.get() >= 1);
        }
        try (LogBackend backend = new FileLogBackend(tmp.resolve("fom.log"));
             Engine engine = new Engine(SizeBasedSnapshotPolicyTest.configWith(policy), backend,
                     new JavaSerializableSerDe())) {
            engine.newGraph(SizeBasedSnapshotPolicyTest.singleNodeGraph());
            Thread.sleep(800); // several firings
        }
        assertThat(archives(tmp)).as("purged down to keepHistory (one more if close() raced the last purge)").isBetween(1L, 2L);
    }

    @Test
    void manual_purge_still_deletes_when_policy_keeps_all(@TempDir Path tmp) throws Exception {
        var policy = new SnapshotPolicy.FixedInterval(Duration.ofHours(1));
        try (LogBackend backend = new FileLogBackend(tmp.resolve("fom.log"));
             Engine engine = new Engine(SizeBasedSnapshotPolicyTest.configWith(policy), backend,
                     new JavaSerializableSerDe())) {
            engine.newGraph(SizeBasedSnapshotPolicyTest.singleNodeGraph());
            for (int i = 0; i < 3; i++) engine.snapshot().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(archives(tmp)).isEqualTo(3);
            engine.purgeArchives(1);
            assertThat(archives(tmp)).isEqualTo(1);
        }
    }
}
