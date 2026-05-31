package io.fom.serde;

import io.fom.Graph;

import java.util.Map;

/**
 * Engine-wide serialisation SPI. Distinct from the per-value
 * {@link io.fom.Codec} used by {@link io.fom.TypedKey} — that one encodes a
 * single typed value, this one drives the engine's log payloads.
 *
 * <p>Implementations must be deterministic and produce stable byte sequences
 * within a major version.</p>
 */
public interface SerDe {

    /** Serialise the value carried by an external trigger to bytes. */
    byte[] serializeTriggerValue(String processName, Object value) throws SerDeException;

    /** Inverse of {@link #serializeTriggerValue}. */
    Object loadTriggerValue(String processName, byte[] bytes) throws SerDeException;

    /**
     * Serialise the {@code ProcessProperties} payload for a
     * {@code LogInitialized} event. The map of typed-byte cells is treated as
     * an opaque value; individual cells were already encoded by their
     * respective {@link io.fom.Codec}s.
     */
    byte[] serializeProcessProperties(Map<String, byte[]> properties) throws SerDeException;

    /** Inverse of {@link #serializeProcessProperties}. */
    Map<String, byte[]> loadProcessProperties(byte[] bytes) throws SerDeException;

    /** Serialise the process graph for {@code LogChangeGraph}. */
    byte[] serializeProcessGraph(Graph graph) throws SerDeException;

    /** Inverse of {@link #serializeProcessGraph}. */
    Graph loadProcessGraph(byte[] bytes) throws SerDeException;
}
