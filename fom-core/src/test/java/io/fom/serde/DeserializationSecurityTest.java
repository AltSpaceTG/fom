package io.fom.serde;

import com.evil.Gadget;
import io.fom.log.LogLeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Security regression for {@link ObjectInputFilters}. Reproduces the
 * gadget-chain deserialization vector the code review flagged and asserts the
 * filters block it:
 *
 * <ul>
 *   <li>{@link ObjectInputFilters#logPayload()} — strict allowlist: accepts
 *       {@code io.fom.*} + JDK types, rejects any other class (a gadget) before
 *       its {@code readObject} can run.</li>
 *   <li>{@link ObjectInputFilters#resourceLimits()} — accepts arbitrary user
 *       classes (it must, for user trigger values) but caps stream depth so a
 *       "deserialization bomb" is rejected.</li>
 * </ul>
 */
class DeserializationSecurityTest {

    @BeforeEach
    void reset() {
        Gadget.reset();
    }

    // ───────────────── helpers ─────────────────

    private static byte[] serialize(Serializable value) {
        try (var baos = new ByteArrayOutputStream();
             var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object readFiltered(byte[] bytes, java.io.ObjectInputFilter filter)
            throws IOException, ClassNotFoundException {
        try (var bais = new ByteArrayInputStream(bytes);
             var ois = new ObjectInputStream(bais)) {
            ois.setObjectInputFilter(filter);
            return ois.readObject();
        }
    }

    // ───────────────── logPayload allowlist ─────────────────

    @Test
    void logPayload_allows_fom_event_records() throws Exception {
        byte[] bytes = serialize(new LogLeader(0, 1L, "leader-A"));
        Object back = readFiltered(bytes, ObjectInputFilters.logPayload());
        assertThat(back).isInstanceOf(LogLeader.class);
        assertThat(((LogLeader) back).instanceId()).isEqualTo("leader-A");
    }

    @Test
    void logPayload_allows_jdk_collections_and_byte_arrays() throws Exception {
        var map = new HashMap<String, byte[]>();
        map.put("k", new byte[]{1, 2, 3});
        @SuppressWarnings("unchecked")
        Map<String, byte[]> back =
                (Map<String, byte[]>) readFiltered(serialize(map), ObjectInputFilters.logPayload());
        assertThat(back).containsKey("k");
        assertThat(back.get("k")).containsExactly(1, 2, 3);
    }

    @Test
    void logPayload_rejects_gadget_class_before_readObject_runs() {
        byte[] bytes = serialize(new Gadget());

        assertThatThrownBy(() -> readFiltered(bytes, ObjectInputFilters.logPayload()))
                .isInstanceOf(InvalidClassException.class);

        // The crucial assertion: the gadget's readObject side-effect NEVER ran,
        // i.e. the filter rejected the class at resolve time, not after instantiation.
        assertThat(Gadget.EXECUTED)
                .as("gadget readObject must not execute under the allowlist filter")
                .isFalse();
    }

    @Test
    void without_a_filter_the_gadget_would_have_executed() throws Exception {
        // Contrast case proving the payload IS a working gadget — so the filter
        // above is doing real work, not rejecting an inert blob.
        byte[] bytes = serialize(new Gadget());
        try (var bais = new ByteArrayInputStream(bytes);
             var ois = new ObjectInputStream(bais)) {  // no filter installed
            Object back = ois.readObject();
            assertThat(back).isInstanceOf(Gadget.class);
        }
        assertThat(Gadget.EXECUTED)
                .as("unfiltered deserialization runs the gadget — this is the vector being defended")
                .isTrue();
    }

    // ───────────────── resourceLimits ─────────────────

    @Test
    void resourceLimits_allows_arbitrary_user_class() throws Exception {
        // resourceLimits must NOT allowlist by package — user trigger values are
        // arbitrary classes. A single Gadget instance is within all caps.
        Object back = readFiltered(serialize(new Gadget()), ObjectInputFilters.resourceLimits());
        assertThat(back).isInstanceOf(Gadget.class);
    }

    @Test
    void resourceLimits_rejects_a_deep_object_graph() {
        // Build a linked chain deeper than MAX_DEPTH (64) to trip the depth cap.
        Node head = null;
        for (int i = 0; i < 200; i++) {
            head = new Node(head);
        }
        byte[] bytes = serialize(head);

        assertThatThrownBy(() -> readFiltered(bytes, ObjectInputFilters.resourceLimits()))
                .isInstanceOf(InvalidClassException.class);
    }

    /** A self-referential chain: each instance nests the previous → deep stream. */
    record Node(Node next) implements Serializable { }
}
