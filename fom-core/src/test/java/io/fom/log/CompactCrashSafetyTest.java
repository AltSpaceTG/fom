package io.fom.log;

import io.fom.SnapshotResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Crash-safety regression for {@link LocalFileLogBackend#compact}. The review
 * flagged the old {@code move(path→archived); move(tmp→path)} ordering: a crash
 * between the two left no live file at {@code path} and lost all state. The fix
 * keeps {@code path} valid at every instant by copying it to the archive first,
 * then atomically replacing it with the compacted temp file.
 *
 * <p>We can't inject a real crash deterministically, so we assert the invariants
 * that make a crash recoverable:</p>
 * <ol>
 *   <li>the archive is a complete, independently-replayable copy of the old log
 *       (so a crash mid-swap leaves the prior state recoverable by hand);</li>
 *   <li>after compaction {@code path} holds exactly the snapshot events and
 *       reopens cleanly;</li>
 *   <li>no {@code .tmp} working file is left behind.</li>
 * </ol>
 */
class CompactCrashSafetyTest {

    private static final String LEADER = "leader-A";

    @TempDir
    Path tmp;

    private static long now() {
        return System.currentTimeMillis();
    }

    @Test
    void compact_preserves_old_log_in_archive_and_swaps_in_new() throws IOException {
        Path file = Files.createTempFile(tmp, "compact-", ".bin");
        Files.delete(file);

        SnapshotResult result;
        try (var backend = new LocalFileLogBackend(file)) {
            // A few events to be superseded by the snapshot.
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            backend.append(new LogChangeGraph(0, now(), new byte[]{1}), LEADER);
            backend.append(new LogInitialized(0, now(), "Foo", java.util.Map.of("k", new byte[]{7})), LEADER);
            backend.append(new LogChangeGraph(0, now(), new byte[]{2}), LEADER);
            assertThat(backend.length()).isEqualTo(4);

            // Snapshot: fresh leader + snapshot marker + the one live init.
            List<LogEvent> snapshot = List.of(
                    new LogLeader(0, now(), LEADER),
                    new LogSnapshot(0, now(), 3),
                    new LogInitialized(0, now(), "Foo", java.util.Map.of("k", new byte[]{7})));
            result = backend.compact(snapshot, LEADER);

            // Active log now holds exactly the snapshot, live in the same instance.
            assertThat(backend.length()).isEqualTo(3);
            assertThat(backend.get(0)).isInstanceOf(LogLeader.class);
            assertThat(backend.get(1)).isInstanceOf(LogSnapshot.class);
            assertThat(backend.get(2)).isInstanceOf(LogInitialized.class);
        }

        Path activePath = Path.of(result.newLogId());
        Path archivePath = Path.of(result.archivedLogId());

        // (1) Both files exist simultaneously — the old log was COPIED, never
        //     moved away, so `path` is never absent during the swap (CR-5).
        assertThat(activePath).isEqualTo(file);
        assertThat(Files.exists(activePath)).as("active log present").isTrue();
        assertThat(Files.exists(archivePath)).as("archive present").isTrue();

        // (3) No leftover working file.
        Path tmpWork = file.resolveSibling(file.getFileName() + ".tmp");
        assertThat(Files.exists(tmpWork)).as("no .tmp left behind").isFalse();

        // (1 cont.) The archive is an independently-valid log with the OLD 4 events.
        try (var archived = new LocalFileLogBackend(archivePath)) {
            assertThat(archived.length()).isEqualTo(4);
            assertThat(archived.get(0)).isInstanceOf(LogLeader.class);
            assertThat(archived.get(2)).isInstanceOf(LogInitialized.class);
        }

        // (2) The active log reopens cleanly with the compacted contents.
        try (var reopened = new LocalFileLogBackend(activePath)) {
            assertThat(reopened.length()).isEqualTo(3);
            assertThat(reopened.get(1)).isInstanceOf(LogSnapshot.class);
            LogInitialized init = (LogInitialized) reopened.get(2);
            assertThat(init.properties()).containsEntry("k", new byte[]{7});
            // and it still accepts new appends after the swap
            assertThat(reopened.append(new LogChangeGraph(0, now(), new byte[]{9}), LEADER)).isPresent();
            assertThat(reopened.length()).isEqualTo(4);
        }
    }
}
