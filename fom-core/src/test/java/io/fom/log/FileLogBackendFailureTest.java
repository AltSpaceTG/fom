package io.fom.log;

import io.fom.Sid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for write-side failure handling in {@link FileLogBackend}. */
class FileLogBackendFailureTest {

    private static final String LEADER = "leader-1";

    @TempDir
    Path tmp;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void event_the_reader_would_reject_is_refused_on_append_and_the_log_stays_intact() throws Exception {
        Path file = tmp.resolve("big.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            // Over the read filter's 10M array-length cap: would be truncated on reopen.
            assertThatThrownBy(() -> backend.append(
                    new LogInitialized(0, 1L, "x", Map.of("k", new byte[10_000_001])), LEADER))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("payload limits");
            assertThat(backend.append(new LogLoaded(0, 1L, new Sid("y", 0)), LEADER)).isPresent();
            assertThat(backend.length()).isEqualTo(2);
        }
        try (var backend = new FileLogBackend(file)) {
            assertThat(backend.length()).isEqualTo(2);
            assertThat(backend.get(1)).isInstanceOf(LogLoaded.class);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void an_oversized_array_is_reported_as_the_array_length_limit_not_as_the_byte_size() throws Exception {
        try (var backend = new FileLogBackend(tmp.resolve("array.bin"))) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            // ~52 MB: under the 64 MiB per-event size, over the 10M-element array cap.
            assertThatThrownBy(() -> backend.append(
                    new LogInitialized(0, 1L, "x", Map.of("k", new byte[50 * 1024 * 1024])), LEADER))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("payload limits")
                    .hasMessageContaining("array of 52,428,800 elements")
                    .hasMessageContaining("over the array-length limit")
                    .hasMessageNotContaining("per-event size limit");
            assertThat(backend.length()).isEqualTo(1);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void an_event_over_64_mib_made_of_allowed_arrays_is_reported_as_the_size_limit() throws Exception {
        try (var backend = new FileLogBackend(tmp.resolve("size.bin"))) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            // Every array is within the 10M-element cap; together they pass 64 MiB.
            var props = new java.util.HashMap<String, byte[]>();
            for (int i = 0; i < 7; i++) props.put("k" + i, new byte[9_600_000]);
            assertThatThrownBy(() -> backend.append(new LogInitialized(0, 1L, "x", props), LEADER))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("payload limits")
                    .hasMessageContaining("over the per-event size limit")
                    .hasMessageNotContaining("array-length limit");
            assertThat(backend.length()).isEqualTo(1);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void large_event_within_the_limits_is_still_accepted() throws Exception {
        Path file = tmp.resolve("large.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            assertThat(backend.append(
                    new LogInitialized(0, 1L, "x", Map.of("k", new byte[5_000_000])), LEADER)).isPresent();
        }
        try (var backend = new FileLogBackend(file)) {
            assertThat(backend.length()).isEqualTo(2);
        }
    }

    @Test
    void an_append_interrupted_mid_write_is_written_again() throws Exception {
        Path file = tmp.resolve("interrupted-mid-write.bin");
        var interrupts = new java.util.concurrent.atomic.AtomicInteger();
        try (var backend = new FileLogBackend(file, (p, options) -> new FailingFileChannel(
                java.nio.channels.FileChannel.open(p, options), new AtomicBoolean(), new AtomicBoolean(), interrupts))) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
            interrupts.set(2); // two interrupts in a row, the third write goes through
            assertThat(backend.append(new LogInitialized(0, 2L, "P", java.util.Map.of()), "L")).isPresent();
            assertThat(Thread.interrupted()).as("the caller learns it was interrupted").isTrue();
            assertThat(backend.length()).isEqualTo(2);

            interrupts.set(3); // interrupted on every attempt: gives up, log unchanged and usable
            assertThatThrownBy(() -> backend.append(new LogInitialized(0, 3L, "Q", java.util.Map.of()), "L"))
                    .hasRootCauseInstanceOf(java.nio.channels.ClosedByInterruptException.class);
            Thread.interrupted();
            assertThat(backend.length()).isEqualTo(2);
            assertThat(backend.append(new LogInitialized(0, 4L, "R", java.util.Map.of()), "L")).isPresent();
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(3);
            assertThat(((LogInitialized) reopened.get(1)).processName()).isEqualTo("P");
            assertThat(((LogInitialized) reopened.get(2)).processName()).isEqualTo("R");
        }
    }

    @Test
    void an_append_interrupted_during_force_after_a_complete_write_is_written_once() throws Exception {
        Path file = tmp.resolve("interrupted-force.bin");
        var forces = new java.util.concurrent.atomic.AtomicInteger();
        try (var backend = new FileLogBackend(file, (p, options) -> new FailingFileChannel(
                java.nio.channels.FileChannel.open(p, options), new AtomicBoolean(), new AtomicBoolean(),
                new java.util.concurrent.atomic.AtomicInteger(), forces))) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
            forces.set(1);
            assertThat(backend.append(new LogInitialized(0, 2L, "P", java.util.Map.of()), "L")).isPresent();
            Thread.interrupted();
            assertThat(backend.length()).isEqualTo(2);
            assertThat(backend.append(new LogInitialized(0, 3L, "Q", java.util.Map.of()), "L")).isPresent();
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).as("no duplicated frame").isEqualTo(3);
            assertThat(((LogInitialized) reopened.get(1)).processName()).isEqualTo("P");
            assertThat(((LogInitialized) reopened.get(2)).processName()).isEqualTo("Q");
        }
    }

    @Test
    void an_interrupt_while_reopening_after_compaction_does_not_break_the_log() throws Exception {
        Path file = tmp.resolve("interrupted-reopen.bin");
        var pendingSizeInterrupts = new java.util.concurrent.atomic.AtomicInteger();
        try (var backend = new FileLogBackend(file, (p, options) -> {
            var channel = new FailingFileChannel(java.nio.channels.FileChannel.open(p, options),
                    new AtomicBoolean(), new AtomicBoolean());
            channel.interruptSizes.set(pendingSizeInterrupts.getAndSet(0)); // hits the reopened channel's scan
            return channel;
        })) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
            backend.append(new LogInitialized(0, 2L, "P", java.util.Map.of()), "L");
            pendingSizeInterrupts.set(1);
            backend.compact(java.util.List.of(new LogLeader(0, 3L, "L"), new LogInitialized(1, 4L, "P", java.util.Map.of())), "L");
            Thread.interrupted();
            assertThat(backend.length()).isEqualTo(2);
            assertThat(backend.append(new LogInitialized(0, 5L, "Q", java.util.Map.of()), "L"))
                    .as("the compacted log still accepts writes").isPresent();
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(3);
        }
    }

    @Test
    void an_interrupt_before_the_write_itself_does_not_fail_the_append() throws Exception {
        Path file = tmp.resolve("interrupted-size.bin");
        var channels = new java.util.concurrent.CopyOnWriteArrayList<FailingFileChannel>();
        try (var backend = new FileLogBackend(file, (p, options) -> {
            var channel = new FailingFileChannel(java.nio.channels.FileChannel.open(p, options),
                    new AtomicBoolean(), new AtomicBoolean());
            channels.add(channel);
            return channel;
        })) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
            channels.get(channels.size() - 1).interruptSizes.set(1); // e.g. interrupted while waiting for the lock
            assertThat(backend.append(new LogInitialized(0, 2L, "P", java.util.Map.of()), "L")).isPresent();
            Thread.interrupted();
            assertThat(backend.length()).isEqualTo(2);
            assertThat(((LogInitialized) backend.get(1)).processName()).isEqualTo("P");
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(2);
        }
    }

    private FileLogBackend failingBackend(Path file, AtomicBoolean failWrite, AtomicBoolean failTruncate)
            throws java.io.IOException {
        return new FileLogBackend(file, (p, options) ->
                new FailingFileChannel(java.nio.channels.FileChannel.open(p, options), failWrite, failTruncate));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void a_failed_write_is_rolled_back_so_later_appends_survive_a_restart() throws Exception {
        Path file = tmp.resolve("rollback.bin");
        var failWrite = new AtomicBoolean();
        try (var backend = failingBackend(file, failWrite, new AtomicBoolean())) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            backend.append(new LogLoaded(0, 1L, new Sid("a", 0)), LEADER);

            failWrite.set(true);
            assertThatThrownBy(() -> backend.append(new LogLoaded(0, 1L, new Sid("lost", 0)), LEADER))
                    .isInstanceOf(RuntimeException.class);
            failWrite.set(false);

            backend.append(new LogLoaded(0, 1L, new Sid("b", 0)), LEADER);
            backend.append(new LogLoaded(0, 1L, new Sid("c", 0)), LEADER);
            assertThat(backend.length()).isEqualTo(4);
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(4);
            assertThat(((LogLoaded) reopened.get(2)).sid().processName()).isEqualTo("b");
            assertThat(((LogLoaded) reopened.get(3)).sid().processName()).isEqualTo("c");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void a_write_that_cannot_be_rolled_back_stops_further_writes() throws Exception {
        Path file = tmp.resolve("broken.bin");
        var failWrite = new AtomicBoolean();
        var failTruncate = new AtomicBoolean();
        try (var backend = failingBackend(file, failWrite, failTruncate)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);

            failWrite.set(true);
            failTruncate.set(true);
            assertThatThrownBy(() -> backend.append(new LogLoaded(0, 1L, new Sid("lost", 0)), LEADER))
                    .isInstanceOf(RuntimeException.class);
            failWrite.set(false);
            failTruncate.set(false);

            assertThatThrownBy(() -> backend.append(new LogLoaded(0, 1L, new Sid("b", 0)), LEADER))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("could not be rolled back");
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).as("the torn frame is dropped on reopen").isEqualTo(1);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void failed_compaction_leaves_the_backend_usable() throws Exception {
        Path file = tmp.resolve("c.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            backend.append(new LogLoaded(0, 1L, new Sid("a", 0)), LEADER);
            // A non-empty directory where compact() writes its temporary file makes it fail.
            Path blocker = Files.createDirectories(tmp.resolve("c.bin.tmp"));
            Files.createFile(blocker.resolve("keep"));
            assertThatThrownBy(() -> backend.compact(List.of(new LogLeader(0, 2L, LEADER)), LEADER))
                    .isInstanceOf(RuntimeException.class);

            assertThat(backend.length()).isEqualTo(2);
            assertThat(backend.get(1)).isInstanceOf(LogLoaded.class);
            assertThat(backend.append(new LogLoaded(0, 1L, new Sid("z", 0)), LEADER)).isPresent();
            try (var archives = Files.list(tmp)) {
                assertThat(archives.filter(p -> p.getFileName().toString().startsWith("c.bin.archived."))).isEmpty();
            }
            Files.delete(blocker.resolve("keep"));
            Files.delete(blocker);
        }
        try (var backend = new FileLogBackend(file)) {
            assertThat(backend.length()).isEqualTo(3);
        }
    }

    @Test
    void deleting_the_log_under_a_running_backend_fails_the_next_append_loudly() throws Exception {
        Path file = tmp.resolve("deleted.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            String real = file.toRealPath().toString();
            Files.delete(file);
            assertThatThrownBy(() -> backend.append(new LogLoaded(0, 1L, new Sid("a", 0)), LEADER))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("log file " + real + " was deleted")
                    .hasMessageContaining("was deleted or replaced while open")
                    .hasMessageContaining("refusing to append to an orphaned file");
            assertThat(backend.length()).isEqualTo(1);
            assertThatThrownBy(() -> backend.compact(List.of(new LogLeader(0, 2L, LEADER)), LEADER))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("orphaned file");
        }
    }

    @Test
    void renaming_the_log_away_fails_appends_and_renaming_it_back_resumes_them() throws Exception {
        Path file = tmp.resolve("renamed.bin");
        Path aside = tmp.resolve("renamed.bin.bak");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            Files.move(file, aside);
            assertThatThrownBy(() -> backend.append(new LogLoaded(0, 1L, new Sid("a", 0)), LEADER))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("orphaned file");

            // Something else creates a fresh file at the path: still not the file we indexed.
            Files.write(file, new byte[0]);
            assertThatThrownBy(() -> backend.append(new LogLoaded(0, 1L, new Sid("a", 0)), LEADER))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("is now a different file");
            Files.delete(file);

            Files.move(aside, file); // put back: transient failure, appends resume
            assertThat(backend.append(new LogLoaded(0, 1L, new Sid("b", 0)), LEADER)).isPresent();
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(2);
        }
    }

    @Test
    void appends_after_a_compaction_are_not_mistaken_for_a_replaced_file() throws Exception {
        Path file = tmp.resolve("compacted.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            backend.append(new LogLoaded(0, 1L, new Sid("a", 0)), LEADER);
            backend.compact(List.of(new LogLeader(0, 2L, LEADER)), LEADER);
            assertThat(backend.append(new LogLoaded(0, 3L, new Sid("b", 0)), LEADER)).isPresent();
        }
    }

    @Test
    void a_torn_tail_that_cannot_be_saved_leaves_no_partial_sibling_and_names_the_path() throws Exception {
        Path file = tmp.resolve("torn-full.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
        }
        long intact = Files.size(file);
        Files.write(file, new byte[]{0, 0, 0, 50, 1, 2, 3, 4, 5}, java.nio.file.StandardOpenOption.APPEND);
        long torn = Files.size(file);

        assertThatThrownBy(() -> new FileLogBackend(file, (p, options) -> {
            var ch = new FailingFileChannel(java.nio.channels.FileChannel.open(p, options),
                    new AtomicBoolean(), new AtomicBoolean());
            ch.failTransferTo.set(true);
            return ch;
        }))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("could not save the torn tail of " + file.toRealPath())
                .hasMessageContaining(".truncated.")
                .hasMessageContaining("No space left on device")
                .hasMessageContaining("was not modified")
                .hasMessageContaining("free some space");
        try (var siblings = Files.list(tmp)) {
            assertThat(siblings.filter(p -> p.getFileName().toString().contains(".truncated."))).isEmpty();
        }
        assertThat(Files.size(file)).isEqualTo(torn);

        try (var reopened = new FileLogBackend(file)) { // space freed: recovery goes through
            assertThat(reopened.length()).isEqualTo(1);
        }
        assertThat(Files.size(file)).isEqualTo(intact);
    }

    @Test
    void a_torn_tail_that_cannot_be_saved_for_another_reason_does_not_blame_disk_space() throws Exception {
        Path file = tmp.resolve("torn-denied.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
        }
        Files.write(file, new byte[]{0, 0, 0, 50, 1, 2, 3, 4, 5}, java.nio.file.StandardOpenOption.APPEND);

        assertThatThrownBy(() -> new FileLogBackend(file, (p, options) -> {
            var ch = new FailingFileChannel(java.nio.channels.FileChannel.open(p, options),
                    new AtomicBoolean(), new AtomicBoolean());
            ch.transferToError.set(new java.nio.file.AccessDeniedException(p + ".truncated.1 (injected)"));
            return ch;
        }))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("AccessDeniedException")
                .hasMessageContaining("was not modified")
                .hasMessageNotContaining("free some space");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void a_failed_append_names_the_underlying_cause_in_its_message() throws Exception {
        Path file = tmp.resolve("enospc.bin");
        var failWrite = new AtomicBoolean();
        try (var backend = failingBackend(file, failWrite, new AtomicBoolean())) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            failWrite.set(true);
            // The engine reports only getMessage()/toString(): the cause must be in there.
            assertThatThrownBy(() -> backend.append(new LogLoaded(0, 1L, new Sid("lost", 0)), LEADER))
                    .hasMessageContaining("Append failed on " + file.toRealPath())
                    .hasMessageContaining("No space left on device")
                    .hasCauseInstanceOf(java.io.IOException.class);
        }
    }

    @Test
    void the_out_of_space_check_looks_at_the_whole_cause_chain() {
        assertThat(FileLogBackend.isOutOfSpace(new java.io.IOException("No space left on device"))).isTrue();
        assertThat(FileLogBackend.isOutOfSpace(new java.io.IOException("wrapped",
                new java.io.IOException("Disk quota exceeded")))).isTrue();
        assertThat(FileLogBackend.isOutOfSpace(new java.nio.file.AccessDeniedException("/x"))).isFalse();
    }
}
