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

/**
 * Security regression at the {@link LocalFileLogBackend} layer: a forged log
 * frame with a <em>valid CRC</em> (CRC is corruption detection, not a MAC) but
 * a gadget-class payload must NOT be deserialized into the live index. The
 * allowlist {@code ObjectInputFilters.logPayload()} the backend installs has to
 * reject it; the backend then truncates the bad tail.
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
    void forged_gadget_frame_with_valid_crc_is_rejected_and_truncated() throws IOException {
        Path file = Files.createTempFile(tmp, "tampered-", ".bin");
        Files.delete(file);

        // 1. A legitimate single-event log (clock 0 = LogLeader).
        try (var backend = new LocalFileLogBackend(file)) {
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

        // 3. Reopen: the forged frame's CRC matches, but deserialization is
        //    allowlist-filtered, so the gadget is refused and the tail truncated.
        try (var reopened = new LocalFileLogBackend(file)) {
            assertThat(reopened.length())
                    .as("forged gadget frame must not be indexed")
                    .isEqualTo(1);
            assertThat(reopened.get(0)).isInstanceOf(LogLeader.class);
        }

        // The gadget's readObject side-effect must never have run.
        assertThat(Gadget.EXECUTED)
                .as("gadget must never be deserialized through the backend")
                .isFalse();

        // The corrupt tail was physically truncated back to the valid prefix.
        assertThat(Files.size(file)).isEqualTo(sizeAfterLeader);
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
}
