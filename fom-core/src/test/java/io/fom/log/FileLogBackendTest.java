package io.fom.log;

import io.fom.test.LogBackendContractTest;

import io.fom.Sid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.List;

class FileLogBackendTest extends LogBackendContractTest {

    @TempDir
    Path tmp;

    @Override
    protected LogBackend create() {
        try {
            // unique path per @BeforeEach so individual tests don't collide on the lock
            Path file = Files.createTempFile(tmp, "fom-log-", ".bin");
            Files.delete(file); // backend will create
            return new FileLogBackend(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected LogBackend reopen(LogBackend original) {
        FileLogBackend f = (FileLogBackend) original;
        try {
            return new FileLogBackend(Path.of(f.logId()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ───────────────── persistence-specific tests ─────────────────

    @Test
    void second_open_on_same_path_fails_with_lock() throws IOException {
        Path file = Files.createTempFile(tmp, "lockcheck-", ".bin");
        Files.delete(file);
        var first = new FileLogBackend(file);
        try {
            assertThatThrownBy(() -> new FileLogBackend(file))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("lock");
        } finally {
            first.close();
        }
    }

    @Test
    void reopen_after_close_preserves_events_and_leader() throws IOException {
        Path file = Files.createTempFile(tmp, "reopen-", ".bin");
        Files.delete(file);

        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            backend.append(new LogInitialized(0, now(), "Foo",
                    Map.of("k1", new byte[]{1, 2}, "k2", new byte[]{3, 4})), LEADER);
            backend.append(new LogLoaded(0, now(), new Sid("Foo", 1)), LEADER);
        }

        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(3);
            assertThat(reopened.get(0)).isInstanceOf(LogLeader.class);
            LogInitialized init = (LogInitialized) reopened.get(1);
            assertThat(init.processName()).isEqualTo("Foo");
            assertThat(init.properties()).containsEntry("k1", new byte[]{1, 2});
            assertThat(init.properties()).containsEntry("k2", new byte[]{3, 4});
            assertThat(reopened.get(2)).isInstanceOf(LogLoaded.class);

            // Append after reopen succeeds with the same leader id (leader survives restart).
            assertThat(reopened.append(new LogChangeGraph(0, now(), List.of(new LogChangeGraph.Node("g9", List.of(), List.of(), null))), LEADER)).isPresent();
        }
    }

    @Test
    void corrupted_tail_is_truncated_on_reopen() throws IOException {
        Path file = Files.createTempFile(tmp, "corrupt-", ".bin");
        Files.delete(file);

        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            backend.append(new LogChangeGraph(0, now(), List.of(new LogChangeGraph.Node("g1", List.of(), List.of(), null))), LEADER);
        }
        long sizeAfterTwo = Files.size(file);

        // Append junk to the file outside the framing.
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.write(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
        }
        assertThat(Files.size(file)).isGreaterThan(sizeAfterTwo);

        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(2);
            assertThat(Files.size(file)).isEqualTo(sizeAfterTwo);
            // and the backend can append again normally
            assertThat(reopened.append(new LogChangeGraph(0, now(), List.of(new LogChangeGraph.Node("g4", List.of(), List.of(), null))), LEADER)).isPresent();
            assertThat(reopened.length()).isEqualTo(3);
        }
        // The discarded bytes are kept next to the log.
        try (Stream<Path> files = Files.list(tmp)) {
            var saved = files.filter(p -> p.getFileName().toString().startsWith(file.getFileName() + ".truncated."))
                    .toList();
            assertThat(saved).hasSize(1);
            assertThat(Files.readAllBytes(saved.get(0)))
                    .containsExactly((byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF);
        }
    }

    @Test
    void a_half_written_last_frame_is_cut_off() throws IOException {
        Path file = tmp.resolve("torn.bin");
        long lastFrame;
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            lastFrame = Files.size(file);
            backend.append(new LogInitialized(0, now(), "P", Map.of("k", new byte[64])), LEADER);
        }
        long full = Files.size(file);
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(full - 10);
            raf.write(new byte[10]); // the tail of the last frame never reached the disk
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(1);
            assertThat(Files.size(file)).isEqualTo(lastFrame);
        }
    }

    @Test
    void damage_before_the_last_frame_refuses_to_open_and_keeps_the_file() throws IOException {
        Path file = tmp.resolve("middle.bin");
        List<Long> starts = new java.util.ArrayList<>();
        try (var backend = new FileLogBackend(file)) {
            starts.add(Files.size(file));
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            for (int i = 0; i < 5; i++) {
                starts.add(Files.size(file));
                backend.append(new LogInitialized(0, now(), "P" + i, Map.of()), LEADER);
            }
        }
        long frame3 = starts.get(3);
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(frame3 + 8 + 20);
            int b = raf.read();
            raf.seek(frame3 + 8 + 20);
            raf.write(b ^ 0xFF);
        }
        byte[] before = Files.readAllBytes(file);

        assertThatThrownBy(() -> new FileLogBackend(file))
                .isInstanceOf(LogCorruptedException.class)
                .hasMessageContaining("offset " + frame3)
                .satisfies(e -> assertThat(((LogCorruptedException) e).readableEvents()).isEqualTo(3));
        assertThat(Files.readAllBytes(file)).isEqualTo(before);
        // The refused open released its lock: after a manual truncate the log opens.
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(frame3);
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(3);
        }
    }

    @Test
    void an_interrupt_racing_with_appends_does_not_break_the_log() throws Exception {
        Path file = tmp.resolve("interrupt.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            var stop = new java.util.concurrent.atomic.AtomicBoolean();
            Thread writer = Thread.ofPlatform().start(() -> {
                while (!stop.get()) {
                    try {
                        backend.append(new LogInitialized(0, now(), "W", Map.of()), LEADER);
                    } catch (RuntimeException ignored) {
                        // an append hit by the interrupt may fail; the log must survive it
                    }
                }
            });
            for (int i = 0; i < 200; i++) {
                writer.interrupt();
                Thread.sleep(1);
            }
            stop.set(true);
            writer.join(5_000);

            int before = backend.length();
            assertThat(backend.append(new LogInitialized(0, now(), "after", Map.of()), LEADER)).isPresent();
            assertThat(backend.length()).isEqualTo(before + 1);
            for (int i = 0; i < backend.length(); i++) {
                assertThat(backend.get(i)).isNotNull();
            }
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(((LogInitialized) reopened.get(reopened.length() - 1)).processName()).isEqualTo("after");
        }
    }

    @Test
    void compact_refused_after_a_takeover_leaves_no_tmp_or_archive_behind() throws IOException {
        Path file = tmp.resolve("deposed.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            backend.append(new LogInitialized(0, now(), "P", Map.of()), LEADER);
            backend.append(new LogLeader(0, now(), OTHER_LEADER), OTHER_LEADER); // takeover
            long sizeBefore = Files.size(file);

            assertThatThrownBy(() -> backend.compact(
                    List.of(new LogLeader(0, now(), LEADER), new LogSnapshot(3, now(), 2)), LEADER))
                    .isInstanceOf(io.fom.api.LeadershipLostException.class);

            assertThat(Files.size(file)).as("the live log file is byte-identical").isEqualTo(sizeBefore);
            try (Stream<Path> files = Files.list(tmp)) {
                assertThat(files.map(p -> p.getFileName().toString()))
                        .as("a refused compaction writes nothing at all")
                        .noneMatch(n -> n.startsWith("deposed.bin.archived.") || n.equals("deposed.bin.tmp"));
            }
            assertThat(backend.length()).isEqualTo(3);
            assertThat(backend.introspect().currentLeader()).isEqualTo(OTHER_LEADER);
        }
    }

    @Test
    void purge_removes_partial_archive_copies_and_ignores_unrelated_files() throws IOException {
        Path file = tmp.resolve("purge.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            Path partial = tmp.resolve("purge.bin.archived.123.partial");
            Path unrelated = tmp.resolve("purge.bin.archived.notes.txt");
            Files.write(partial, new byte[]{1});
            Files.write(unrelated, new byte[]{2});
            for (int i = 0; i < 3; i++) {
                backend.compact(List.of(new LogLeader(0, now(), LEADER)), LEADER);
            }
            backend.purgeArchives(1);
            try (Stream<Path> files = Files.list(tmp)) {
                assertThat(files.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("purge.bin.archived.")))
                        .hasSize(2)
                        .contains("purge.bin.archived.notes.txt")
                        .noneMatch(n -> n.endsWith(".partial"));
            }
        }
    }

    @Test
    void open_removes_leftovers_of_a_compaction_that_crashed() throws IOException {
        Path file = tmp.resolve("crashed.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            backend.append(new LogInitialized(0, now(), "P", Map.of()), LEADER);
        }
        // What a crash between writing the compacted tmp and the archive rename leaves behind.
        Path staleTmp = tmp.resolve("crashed.bin.tmp");
        Path partial = tmp.resolve("crashed.bin.archived.1737045123456.partial");
        Path archive = tmp.resolve("crashed.bin.archived.1737045000000");
        Path unrelatedPartial = tmp.resolve("crashed.bin.archived.notes.partial");
        Path otherLogsTmp = tmp.resolve("other.bin.tmp");
        Files.write(staleTmp, new byte[4096]);
        Files.write(partial, new byte[]{1, 2, 3});
        Files.copy(file, archive);
        Files.write(unrelatedPartial, new byte[]{4});
        Files.write(otherLogsTmp, new byte[]{5});

        try (var reopened = new FileLogBackend(file)) {
            assertThat(staleTmp).doesNotExist();
            assertThat(partial).doesNotExist();
            assertThat(archive).as("a complete archive is kept").exists();
            assertThat(unrelatedPartial).exists();
            assertThat(otherLogsTmp).exists();
            assertThat(reopened.length()).isEqualTo(2);
            assertThat(reopened.append(new LogInitialized(0, now(), "Q", Map.of()), LEADER)).isPresent();
            reopened.compact(List.of(new LogLeader(0, now(), LEADER), new LogSnapshot(3, now(), 2)), LEADER);
            assertThat(reopened.length()).isEqualTo(2);
        }
    }

    @Test
    void a_leftover_tmp_directory_is_not_removed_on_open() throws IOException {
        Path file = tmp.resolve("dir.bin");
        Path dir = Files.createDirectories(tmp.resolve("dir.bin.tmp"));
        new FileLogBackend(file).close();
        assertThat(dir).isDirectory();
    }

    @Test
    void invalid_magic_fails_open_loudly() throws IOException {
        Path file = Files.createTempFile(tmp, "badmagic-", ".bin");
        Files.write(file, new byte[]{'X', 'X', 'X', 'X', 1, 2, 3, 4});

        assertThatThrownBy(() -> new FileLogBackend(file))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("magic");
    }

    // ───────────────── field-test regressions ─────────────────

    @Test
    void an_event_over_the_payload_limit_is_refused_at_append_and_nothing_is_written() throws IOException {
        Path file = tmp.resolve("oversize.bin");
        // 7 arrays of 10,000,000 bytes: each within the filter's array-length cap, and the
        // filter's stream-byte count never sees the last array's contents, so only a size
        // check matches the reader, which refuses any frame over 64 MiB as corrupt.
        Map<String, byte[]> props = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 7; i++) props.put("k" + i, new byte[10_000_000]);
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            long before = Files.size(file);
            assertThatThrownBy(() -> backend.append(new LogInitialized(0, now(), "Big", props), LEADER))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("LogInitialized")
                    .hasMessageContaining("64 MiB");
            props.clear();
            assertThat(Files.size(file)).isEqualTo(before);
            assertThat(backend.length()).isEqualTo(1);
            assertThat(backend.append(new LogInitialized(0, now(), "Small", Map.of()), LEADER)).isPresent();
        }
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(2);
        }
    }

    /** A log of a leader + {@code count} small events; returns each frame's start offset. */
    private List<Long> logWithFrames(Path file, int count) throws IOException {
        List<Long> starts = new java.util.ArrayList<>();
        try (var backend = new FileLogBackend(file)) {
            starts.add(Files.size(file));
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            for (int i = 1; i < count; i++) {
                starts.add(Files.size(file));
                backend.append(new LogInitialized(0, now(), "P" + i, Map.of("k", new byte[]{(byte) i})), LEADER);
            }
        }
        return starts;
    }

    private static void writeIntAt(Path file, long offset, int value) throws IOException {
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            raf.writeInt(value);
        }
    }

    private static int readIntAt(Path file, long offset) throws IOException {
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            return raf.readInt();
        }
    }

    private List<Path> truncatedSiblings(Path file) throws IOException {
        try (Stream<Path> files = Files.list(tmp)) {
            return files.filter(p -> p.getFileName().toString().startsWith(file.getFileName() + ".truncated.")).toList();
        }
    }

    @Test
    void a_damaged_length_in_a_middle_frame_reaching_past_the_end_is_corruption_not_a_torn_tail() throws IOException {
        Path file = tmp.resolve("midlen.bin");
        List<Long> starts = logWithFrames(file, 69);
        long frame20 = starts.get(20);
        // One flipped bit: the length now points past the end of the (small) file, but is still
        // under the 64 MiB limit, so the header alone looks like an incomplete last write.
        int len = readIntAt(file, frame20);
        writeIntAt(file, frame20, len | (1 << 22));
        byte[] before = Files.readAllBytes(file);
        assertThat((long) (len | (1 << 22))).isGreaterThan(before.length);

        assertThatThrownBy(() -> new FileLogBackend(file))
                .isInstanceOf(LogCorruptedException.class)
                .hasMessageContaining("offset " + frame20)
                .hasMessageContaining("intact frame follows at offset " + starts.get(21))
                .satisfies(e -> assertThat(((LogCorruptedException) e).readableEvents()).isEqualTo(20));
        assertThat(Files.readAllBytes(file)).isEqualTo(before);
        assertThat(truncatedSiblings(file)).isEmpty();
    }

    @Test
    void a_damaged_length_in_a_middle_frame_reaching_exactly_the_end_is_corruption_not_a_torn_tail() throws IOException {
        Path file = tmp.resolve("midlen-exact.bin");
        List<Long> starts = logWithFrames(file, 10);
        long frame4 = starts.get(4);
        // The length now spans every later frame up to the end: a "last frame" with a bad CRC.
        writeIntAt(file, frame4, (int) (Files.size(file) - frame4 - 8));
        byte[] before = Files.readAllBytes(file);

        assertThatThrownBy(() -> new FileLogBackend(file))
                .isInstanceOf(LogCorruptedException.class)
                .hasMessageContaining("offset " + frame4)
                .satisfies(e -> assertThat(((LogCorruptedException) e).readableEvents()).isEqualTo(4));
        assertThat(Files.readAllBytes(file)).isEqualTo(before);
        assertThat(truncatedSiblings(file)).isEmpty();
    }

    private static byte[] javaSerialize(Object o) throws IOException {
        var bytes = new java.io.ByteArrayOutputStream();
        try (var oos = new java.io.ObjectOutputStream(bytes)) {
            oos.writeObject(o);
        }
        return bytes.toByteArray();
    }

    /** Leader + one LogInitialized with {@code props}, whose last {@code tear} bytes are then lost. */
    private long tornInitLog(Path file, Map<String, byte[]> props, int tear) throws IOException {
        long lastFrame;
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            lastFrame = Files.size(file);
            backend.append(new LogInitialized(0, now(), "P", props), LEADER);
        }
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(raf.length() - tear); // the end of the last frame never reached the disk
        }
        return lastFrame;
    }

    @Test
    void a_torn_frame_full_of_serialized_properties_is_cut_off_quickly() throws IOException {
        // What JavaSerializableSerDe produces: every value starts with the stream magic and, in
        // the outer stream, follows "75 71 00 7E 00 xx <len>" — a frame header claiming ~8.26 MB.
        // The frame is ~16 MB, so thousands of those claimed frames fit inside it.
        Path file = tmp.resolve("torn-serialized.bin");
        Map<String, byte[]> props = new java.util.TreeMap<>();
        for (int i = 0; i < 8000; i++) {
            props.put(String.format("p%05d", i), javaSerialize(new int[500]));
        }
        long lastFrame = tornInitLog(file, props, 40);
        assertThat(Files.size(file) - lastFrame).isGreaterThan(12_000_000L);

        long started = System.nanoTime();
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(1);
            assertThat(Files.size(file)).isEqualTo(lastFrame);
        }
        assertThat(java.time.Duration.ofNanos(System.nanoTime() - started)).isLessThan(java.time.Duration.ofMillis(1500));
        assertThat(truncatedSiblings(file)).hasSize(1);
    }

    @Test
    void a_torn_frame_embedding_a_whole_fom_log_is_still_cut_off() throws IOException {
        // A property holding another fom log verbatim (an uploaded file through a pass-through
        // SerDe): complete, CRC-valid frames sit inside the torn frame's payload, but they do
        // not start a chain of frames running to the end of the file.
        Path inner = tmp.resolve("inner.bin");
        logWithFrames(inner, 5);
        byte[] embedded = Files.readAllBytes(inner);
        Map<String, byte[]> props = new java.util.TreeMap<>();
        props.put("a-upload", embedded);
        byte[] tail = new byte[4096];
        java.util.Arrays.fill(tail, (byte) 0x5A);
        props.put("z-after", tail); // TreeMap order: the tear lands in here, after the upload
        Path file = tmp.resolve("embeds.bin");
        long lastFrame = tornInitLog(file, props, 40);
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(1);
            assertThat(Files.size(file)).isEqualTo(lastFrame);
        }
    }

    /** A torn frame at the end of a leader-only log whose payload continues with {@code units}. */
    private long crafted(Path file, int claimedLength, byte[] units) throws IOException {
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
        }
        long pos = Files.size(file);
        var buf = java.nio.ByteBuffer.allocate(12 + units.length);
        buf.putInt(claimedLength).putInt(0).put(new byte[]{(byte) 0xAC, (byte) 0xED, 0, 5}).put(units);
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(pos);
            raf.write(buf.array());
        }
        return pos;
    }

    /** {@code count} copies of {@code [len][crc 0][stream magic]}: each a frame header whose chain runs on. */
    private static byte[] frameLikeUnits(int len, int count) {
        var buf = java.nio.ByteBuffer.allocate(12 * count);
        for (int i = 0; i < count; i++) {
            buf.putInt(len).putInt(0).put(new byte[]{(byte) 0xAC, (byte) 0xED, 0, 5});
        }
        return buf.array();
    }

    @Test
    void frame_like_bytes_with_wrong_crcs_after_a_torn_header_are_still_a_torn_tail() throws IOException {
        // Structurally valid chains everywhere (length 4, magic), so every candidate reaches the
        // CRC check — and fails it: nothing intact follows, the tail is cut off.
        Path file = tmp.resolve("crafted-small.bin");
        long pos = crafted(file, 1 << 20, frameLikeUnits(4, 10_000));
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(1);
            assertThat(Files.size(file)).isEqualTo(pos);
        }
    }

    @Test
    void a_crafted_tail_cannot_make_the_open_do_unbounded_crc_work() throws IOException {
        // ~4 MiB of headers every 12 bytes, each claiming ~1 MiB and chaining onto the next:
        // hundreds of thousands of candidates, each a 1 MiB CRC. The open must fail closed fast.
        Path file = tmp.resolve("crafted-big.bin");
        int len = 12 * 87_382 - 8; // 8 + len is a multiple of 12: every header chains onto another
        crafted(file, 60 << 20, frameLikeUnits(len, (4 << 20) / 12));
        byte[] before = Files.readAllBytes(file);
        org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(10), () ->
                assertThatThrownBy(() -> new FileLogBackend(file))
                        .isInstanceOf(LogCorruptedException.class)
                        .hasMessageContaining("refusing rather than guessing"));
        assertThat(Files.readAllBytes(file)).isEqualTo(before);
        assertThat(truncatedSiblings(file)).isEmpty();
    }

    @Test
    void the_leader_lock_is_keyed_on_the_real_file_not_the_path_spelling() throws IOException {
        Path dir = Files.createDirectories(tmp.resolve("real"));
        Path file = dir.resolve("log.bin");
        Path link = tmp.resolve("link.bin");
        Path dangling = tmp.resolve("dangling.bin");
        Path fresh = dir.resolve("fresh.bin");
        try (var first = new FileLogBackend(file)) {
            first.append(new LogLeader(0, now(), LEADER), LEADER);
            Files.createSymbolicLink(link, file);
            assertThatThrownBy(() -> new FileLogBackend(link))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("lock");
        }
        // A symlink to a log that does not exist yet: the lock is taken next to its target.
        Files.createSymbolicLink(dangling, fresh);
        try (var viaLink = new FileLogBackend(dangling)) {
            viaLink.append(new LogLeader(0, now(), LEADER), LEADER);
            assertThatThrownBy(() -> new FileLogBackend(fresh))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("lock");
        }
        // Compacting through the link replaces the real file and leaves the link a link.
        try (var viaLink = new FileLogBackend(link)) {
            viaLink.compact(List.of(new LogLeader(0, now(), LEADER)), LEADER);
        }
        assertThat(Files.isSymbolicLink(link)).isTrue();
        try (var reopened = new FileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(1);
        }
    }

    @Test
    void archives_stay_in_order_after_the_wall_clock_steps_backwards() throws IOException {
        Path file = tmp.resolve("clock.bin");
        // An archive written before the wall clock was stepped back an hour.
        Path older = tmp.resolve("clock.bin.archived." + (System.currentTimeMillis() + 3_600_000L));
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            Files.copy(file, older);
            var result = backend.compact(List.of(new LogLeader(0, now(), LEADER)), LEADER);
            var result2 = backend.compact(List.of(new LogLeader(0, now(), LEADER)), LEADER);
            backend.purgeArchives(1);
            assertThat(Path.of(result2.archivedLogId())).as("the newest archive is kept").exists();
            assertThat(Path.of(result.archivedLogId())).doesNotExist();
            assertThat(older).doesNotExist();
        }
    }

    @Test
    void an_archive_stamped_absurdly_far_ahead_does_not_derail_the_archive_names() throws IOException {
        Path file = tmp.resolve("future.bin");
        Path planted = tmp.resolve("future.bin.archived.999999999999999999");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            Files.copy(file, planted);
            var first = backend.compact(List.of(new LogLeader(0, now(), LEADER)), LEADER);
            var second = backend.compact(List.of(new LogLeader(0, now(), LEADER)), LEADER);
            assertThat(Path.of(second.archivedLogId()).getFileName().toString())
                    .matches("future\\.bin\\.archived\\.[0-9]{1,18}");
            backend.purgeArchives(1);
            assertThat(Path.of(second.archivedLogId())).as("the newest real archive is kept").exists();
            assertThat(Path.of(first.archivedLogId())).doesNotExist();
            assertThat(planted).doesNotExist();
        }
    }

    @Test
    void compaction_keeps_the_log_files_posix_permissions() throws Exception {
        Path file = tmp.resolve("private.bin");
        try (var backend = new FileLogBackend(file)) {
            org.junit.jupiter.api.Assumptions.assumeTrue(
                    Files.getFileAttributeView(file, java.nio.file.attribute.PosixFileAttributeView.class) != null,
                    "POSIX file system");
            var owner = Files.getOwner(file);
            for (String perms : List.of("rw-------", "rw-r-----", "rw-rw-rw-")) {
                var expected = java.nio.file.attribute.PosixFilePermissions.fromString(perms);
                Files.setPosixFilePermissions(file, expected);
                backend.append(new LogLeader(0, 1L, "L"), "L");
                backend.compact(List.of(new LogLeader(0, 2L, "L")), "L");
                assertThat(Files.getPosixFilePermissions(file)).as("after compaction with %s", perms)
                        .isEqualTo(expected);
                assertThat(Files.getOwner(file)).isEqualTo(owner);
            }
        }
    }

    @Test
    void a_saved_torn_tail_is_no_more_readable_than_the_log() throws Exception {
        Path file = tmp.resolve("private-torn.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
        }
        org.junit.jupiter.api.Assumptions.assumeTrue(
                Files.getFileAttributeView(file, java.nio.file.attribute.PosixFileAttributeView.class) != null,
                "POSIX file system");
        var expected = java.nio.file.attribute.PosixFilePermissions.fromString("rw-------");
        Files.setPosixFilePermissions(file, expected);
        Files.write(file, new byte[]{0, 0, 0, 50, 1, 2, 3}, java.nio.file.StandardOpenOption.APPEND);
        try (var backend = new FileLogBackend(file)) {
            assertThat(backend.length()).isEqualTo(1);
        }
        try (Stream<Path> siblings = Files.list(tmp)) {
            List<Path> saved = siblings.filter(p -> p.getFileName().toString().startsWith("private-torn.bin.truncated."))
                    .toList();
            assertThat(saved).hasSize(1);
            assertThat(Files.getPosixFilePermissions(saved.get(0))).isEqualTo(expected);
        }
    }

    @Test
    void a_file_name_leaving_no_room_for_sibling_suffixes_is_refused_up_front() throws Exception {
        int max = FileLogBackend.MAX_FILE_NAME_BYTES - FileLogBackend.LONGEST_SIBLING_SUFFIX_BYTES;
        assertThat(max).isEqualTo(224);
        Path tooLong = tmp.resolve("a".repeat(max + 1));
        assertThatThrownBy(() -> new FileLogBackend(tooLong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(max))
                .hasMessageContaining("255");
        assertThat(tooLong).doesNotExist();
        assertThat(tooLong.resolveSibling(tooLong.getFileName() + ".lock")).doesNotExist();
        // Multi-byte names are measured in UTF-8 bytes, not chars.
        assertThatThrownBy(() -> new FileLogBackend(tmp.resolve("\u00e9".repeat(max / 2 + 1))))
                .isInstanceOf(IllegalArgumentException.class);

        Path longest = tmp.resolve("b".repeat(max));
        try (var backend = new FileLogBackend(longest)) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
            backend.compact(List.of(new LogLeader(0, 2L, "L")), "L");
            assertThat(backend.length()).isEqualTo(1);
        }
    }

    /** Writes a log of {@code n} events (clocks 0..n-1) at {@code file}. */
    private static void writeLog(Path file, int n) throws IOException {
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
            for (int i = 1; i < n; i++) {
                backend.append(new LogLoaded(0, 1L, new Sid("p" + i, 0)), "L");
            }
        }
    }

    @Test
    void a_log_spliced_from_two_timelines_is_refused_on_open_and_left_untouched() throws IOException {
        Path a = tmp.resolve("a.bin");
        Path b = tmp.resolve("b.bin");
        writeLog(a, 3); // clocks 0, 1, 2
        writeLog(b, 2); // clocks 0, 1: another node's timeline
        byte[] bytesA = Files.readAllBytes(a);
        byte[] bytesB = Files.readAllBytes(b);
        Path spliced = tmp.resolve("spliced.bin");
        byte[] splicedBytes = new byte[bytesA.length + bytesB.length - 4];
        System.arraycopy(bytesA, 0, splicedBytes, 0, bytesA.length);
        System.arraycopy(bytesB, 4, splicedBytes, bytesA.length, bytesB.length - 4); // B's frames, no header
        Files.write(spliced, splicedBytes);

        assertThatThrownBy(() -> new FileLogBackend(spliced))
                .isInstanceOf(LogCorruptedException.class)
                .hasMessageContaining("clock 0 of LogLeader is not greater than the previous event's clock 2")
                .hasMessageContaining("spliced")
                .satisfies(e -> {
                    var c = (LogCorruptedException) e;
                    assertThat(c.offset()).isEqualTo(bytesA.length);
                    assertThat(c.readableEvents()).isEqualTo(3);
                });
        assertThat(Files.readAllBytes(spliced)).isEqualTo(splicedBytes);
    }

    @Test
    void a_repeated_clock_is_refused_on_open() throws IOException {
        Path file = tmp.resolve("dup.bin");
        long lastFrame;
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
            backend.append(new LogLoaded(0, 1L, new Sid("a", 0)), "L");
            lastFrame = Files.size(file);
            backend.append(new LogLoaded(0, 1L, new Sid("b", 0)), "L"); // clock 2
        }
        byte[] bytes = Files.readAllBytes(file);
        byte[] frame = java.util.Arrays.copyOfRange(bytes, (int) lastFrame, bytes.length);
        Files.write(file, frame, java.nio.file.StandardOpenOption.APPEND); // clock 2 again

        assertThatThrownBy(() -> new FileLogBackend(file))
                .isInstanceOf(LogCorruptedException.class)
                .hasMessageContaining("clock 2 of LogLoaded is not greater than the previous event's clock 2")
                .satisfies(e -> assertThat(((LogCorruptedException) e).offset()).isEqualTo(bytes.length));
    }

    @Test
    void a_log_whose_clocks_have_gaps_still_opens() throws IOException {
        Path file = tmp.resolve("gaps.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
            backend.append(new LogLoaded(0, 1L, new Sid("a", 0)), "L");
            backend.append(new LogLoaded(0, 1L, new Sid("b", 0)), "L");
            backend.compact(List.of(new LogLeader(0, 2L, "L"), new LogLoaded(2, 1L, new Sid("b", 0))), "L");
        }
        try (var backend = new FileLogBackend(file)) {
            assertThat(backend.length()).isEqualTo(2);
            assertThat(backend.append(new LogLoaded(0, 1L, new Sid("c", 0)), "L")).get()
                    .extracting(LogEvent::clock).isEqualTo(3L);
        }
    }

    @Test
    void an_in_place_overwrite_of_the_live_log_fails_the_next_write() throws Exception {
        Path file = tmp.resolve("overwritten.bin");
        Path backup = tmp.resolve("backup.bin");
        writeLog(file, 2);
        Files.copy(file, backup);
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLoaded(0, 1L, new Sid("x", 0)), "L");
            Object inode = Files.readAttributes(file, java.nio.file.attribute.BasicFileAttributes.class).fileKey();
            // "cp backup.bin live.bin": same inode, older (shorter) content
            byte[] old = Files.readAllBytes(backup);
            try (var ch = java.nio.channels.FileChannel.open(file, java.nio.file.StandardOpenOption.WRITE)) {
                ch.truncate(0);
                ch.write(java.nio.ByteBuffer.wrap(old), 0);
            }
            assertThat(Files.readAttributes(file, java.nio.file.attribute.BasicFileAttributes.class).fileKey())
                    .isEqualTo(inode);

            assertThatThrownBy(() -> backend.append(new LogLoaded(0, 1L, new Sid("y", 0)), "L"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("was modified by something else while open")
                    .hasMessageContaining("is " + old.length + " bytes");
            assertThatThrownBy(() -> backend.compact(List.of(new LogLeader(0, 2L, "L")), "L"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("was modified by something else while open");
            assertThat(backend.length()).isEqualTo(3);
            assertThat(Files.readAllBytes(file)).isEqualTo(old);
        }
    }

    @Test
    void an_in_place_overwrite_of_the_live_log_fails_reads_as_a_modification_not_corruption() throws Exception {
        Path file = tmp.resolve("overwritten-read.bin");
        Path backup = tmp.resolve("backup-read.bin");
        writeLog(file, 2);
        Files.copy(file, backup);
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLoaded(0, 1L, new Sid("x", 0)), "L");
            byte[] live = Files.readAllBytes(file);
            byte[] old = Files.readAllBytes(backup);
            try (var ch = java.nio.channels.FileChannel.open(file, java.nio.file.StandardOpenOption.WRITE)) {
                ch.truncate(0);
                ch.write(java.nio.ByteBuffer.wrap(old), 0);
            }
            // engine.snapshot() reads the events to plan before it reaches compact()
            assertThatThrownBy(() -> backend.get(2))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("was modified by something else while open")
                    .hasMessageContaining("refusing to read")
                    .hasMessageNotContaining("read past EOF");
            assertThatThrownBy(() -> backend.get(0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("was modified by something else while open");
            assertThatThrownBy(() -> backend.getBetween(0, 3))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("was modified by something else while open");
            assertThat(backend.length()).isEqualTo(3);

            // Putting back the content this backend wrote makes it readable again.
            try (var ch = java.nio.channels.FileChannel.open(file, java.nio.file.StandardOpenOption.WRITE)) {
                ch.write(java.nio.ByteBuffer.wrap(live), 0);
            }
            assertThat(backend.get(2)).isInstanceOf(LogLoaded.class);
        }
    }

    @Test
    void bytes_appended_in_place_by_someone_else_fail_the_next_write() throws Exception {
        Path file = tmp.resolve("appended.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, "L"), "L");
            try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
                raf.seek(raf.length());
                raf.write(new byte[]{1, 2, 3});
            }
            assertThatThrownBy(() -> backend.append(new LogLoaded(0, 1L, new Sid("y", 0)), "L"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("was modified by something else while open");
            // Transient like a replaced file: undo the foreign write and appends resume.
            try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
                raf.setLength(raf.length() - 3);
            }
            assertThat(backend.append(new LogLoaded(0, 1L, new Sid("y", 0)), "L")).isPresent();
        }
    }
}
