package io.fom.log;

import com.evil.Gadget;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Security regression at the {@link FileLogBackend} layer: a forged log
 * frame with a <em>valid CRC</em> (CRC is corruption detection, not a MAC) but
 * a gadget-class payload must NOT be deserialized into the live index. The
 * allowlist {@code ObjectInputFilters.logPayload()} the backend installs has to
 * reject it; the backend then refuses to open the file and leaves it untouched.
 */
class TamperedLogTest {

    private static final byte[] MAGIC = {'F', 'O', 'M', 1};
    private static final String LEADER = "leader-A";

    @TempDir
    Path tmp;

    @BeforeEach
    void reset() {
        Gadget.reset();
    }

    @Test
    void forged_gadget_frame_with_valid_crc_is_rejected_and_the_log_refused() throws IOException {
        Path file = Files.createTempFile(tmp, "tampered-", ".bin");
        Files.delete(file);

        // 1. A legitimate single-event log (clock 0 = LogLeader).
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, LEADER), LEADER);
            assertThat(backend.length()).isEqualTo(1);
        }
        long sizeAfterLeader = Files.size(file);

        // 2. Attacker appends a well-framed frame whose payload is a gadget, with a
        //    CORRECT CRC — exactly what a tamperer with write access would produce.
        byte[] payload = javaSerialize(new Gadget());
        int crc = (int) crc32(payload);
        ByteBuffer frame = ByteBuffer.allocate(8 + payload.length);
        frame.putInt(payload.length);
        frame.putInt(crc);
        frame.put(payload);
        frame.flip();
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.write(frame.array());
        }
        assertThat(Files.size(file)).isGreaterThan(sizeAfterLeader);
        byte[] tampered = Files.readAllBytes(file);

        // 3. Reopen: the forged frame's CRC matches, but deserialization is
        //    allowlist-filtered, so the gadget is refused. Intact bytes that cannot
        //    be decoded are not a crash artefact: the open fails loudly.
        assertThatThrownBy(() -> new FileLogBackend(file))
                .isInstanceOf(LogCorruptedException.class)
                .satisfies(e -> {
                    var corrupted = (LogCorruptedException) e;
                    assertThat(corrupted.offset()).isEqualTo(sizeAfterLeader);
                    assertThat(corrupted.readableEvents()).isEqualTo(1);
                });

        // The gadget's readObject side-effect must never have run.
        assertThat(Gadget.EXECUTED)
                .as("gadget must never be deserialized through the backend")
                .isFalse();

        // Nothing was discarded: the file is left as the tamperer wrote it.
        assertThat(Files.readAllBytes(file)).isEqualTo(tampered);
    }

    // ───────────────── helpers (mirror the backend's framing) ─────────────────

    private static byte[] javaSerialize(Serializable value) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
        }
        return baos.toByteArray();
    }

    private static long crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue();
    }

    /** Damage done to a running node's file is caught on read, and a snapshot does not launder it. */
    @Test
    void damage_after_open_is_refused_on_read_and_not_copied_by_compact() throws IOException {
        Path file = tmp.resolve("live.bin");
        try (var backend = new FileLogBackend(file)) {
            backend.append(new LogLeader(0, 1L, "me"), "me");
            backend.append(new LogTrigger(0, 1L, java.util.List.of("first-process-name")), "me");
            backend.append(new LogTrigger(0, 1L, java.util.List.of("second-process-name")), "me");
            long size = Files.size(file);
            try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
                long at = size / 2; // inside a frame in the middle of the file
                raf.seek(at);
                int b = raf.read();
                raf.seek(at);
                raf.write(b ^ 0x5A);
            }
            int damaged = -1;
            for (int i = 0; i < backend.length(); i++) {
                try {
                    backend.get(i);
                } catch (RuntimeException e) {
                    assertThat(e).hasMessageContaining("damaged");
                    damaged = i;
                }
            }
            assertThat(damaged).as("one frame reports the damage").isGreaterThanOrEqualTo(0);
            assertThatThrownBy(() -> backend.getBetween(0, backend.length())).hasMessageContaining("damaged");
        }
        // The file was not rewritten: reopening still finds the damage.
        assertThatThrownBy(() -> new FileLogBackend(file)).hasMessageContaining("CRC");
    }
}
