package io.fom.log;

import java.util.Objects;

/**
 * A new process graph was committed.
 * {@code serializedGraph} is the {@code byte[]} produced by
 * {@link io.fom.serde.SerDe#serializeProcessGraph}.
 */
public record LogChangeGraph(int clock, long timestamp, short formatVersion, byte[] serializedGraph)
        implements LogEvent {

    public static final short CURRENT_FORMAT = 1;

    public LogChangeGraph {
        Objects.requireNonNull(serializedGraph, "serializedGraph");
        if (serializedGraph.length == 0) {
            throw new IllegalArgumentException("serializedGraph must not be empty");
        }
    }

    public LogChangeGraph(int clock, long timestamp, byte[] serializedGraph) {
        this(clock, timestamp, CURRENT_FORMAT, serializedGraph);
    }
}
