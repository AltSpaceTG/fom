package io.fom.log;

import io.fom.Sid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalFileLogBackendTest extends LogBackendContractTest {

    @TempDir
    Path tmp;

    @Override
    protected LogBackend create() {
        try {
            // unique path per @BeforeEach so individual tests don't collide on the lock
            Path file = Files.createTempFile(tmp, "fom-log-", ".bin");
            Files.delete(file); // backend will create
            return new LocalFileLogBackend(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected LogBackend reopen(LogBackend original) {
        LocalFileLogBackend f = (LocalFileLogBackend) original;
        try {
            return new LocalFileLogBackend(Path.of(f.logId()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ───────────────── persistence-specific tests ─────────────────

    @Test
    void second_open_on_same_path_fails_with_lock() throws IOException {
        Path file = Files.createTempFile(tmp, "lockcheck-", ".bin");
        Files.delete(file);
        var first = new LocalFileLogBackend(file);
        try {
            assertThatThrownBy(() -> new LocalFileLogBackend(file))
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

        try (var backend = new LocalFileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            backend.append(new LogInitialized(0, now(), "Foo",
                    Map.of("k1", new byte[]{1, 2}, "k2", new byte[]{3, 4})), LEADER);
            backend.append(new LogLoaded(0, now(), new Sid("Foo", 1)), LEADER);
        }

        try (var reopened = new LocalFileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(3);
            assertThat(reopened.get(0)).isInstanceOf(LogLeader.class);
            LogInitialized init = (LogInitialized) reopened.get(1);
            assertThat(init.processName()).isEqualTo("Foo");
            assertThat(init.properties()).containsEntry("k1", new byte[]{1, 2});
            assertThat(init.properties()).containsEntry("k2", new byte[]{3, 4});
            assertThat(reopened.get(2)).isInstanceOf(LogLoaded.class);

            // Append after reopen succeeds with the same leader id (leader survives restart).
            assertThat(reopened.append(new LogChangeGraph(0, now(), new byte[]{9}), LEADER)).isPresent();
        }
    }

    @Test
    void corrupted_tail_is_truncated_on_reopen() throws IOException {
        Path file = Files.createTempFile(tmp, "corrupt-", ".bin");
        Files.delete(file);

        try (var backend = new LocalFileLogBackend(file)) {
            backend.append(new LogLeader(0, now(), LEADER), LEADER);
            backend.append(new LogChangeGraph(0, now(), new byte[]{1, 2, 3}), LEADER);
        }
        long sizeAfterTwo = Files.size(file);

        // Append junk to the file outside the framing.
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.write(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
        }
        assertThat(Files.size(file)).isGreaterThan(sizeAfterTwo);

        try (var reopened = new LocalFileLogBackend(file)) {
            assertThat(reopened.length()).isEqualTo(2);
            assertThat(Files.size(file)).isEqualTo(sizeAfterTwo);
            // and the backend can append again normally
            assertThat(reopened.append(new LogChangeGraph(0, now(), new byte[]{4}), LEADER)).isPresent();
            assertThat(reopened.length()).isEqualTo(3);
        }
    }

    @Test
    void invalid_magic_fails_open_loudly() throws IOException {
        Path file = Files.createTempFile(tmp, "badmagic-", ".bin");
        Files.write(file, new byte[]{'X', 'X', 'X', 'X', 1, 2, 3, 4});

        assertThatThrownBy(() -> new LocalFileLogBackend(file))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("magic");
    }
}
