package io.fom.serde;

import java.io.Serializable;

/**
 * Writes the one user-typed value the log holds, a node's {@code param}, and reads it
 * back. {@code LogChangeGraph} records it so a restart can tell whether the node changed
 * since its state was saved. Everything else in the log is an fom type or the process's
 * own {@code byte[]} cells.
 *
 * <p>A param must read back {@code equals} to the original.</p>
 */
public interface SerDe {

    /** Serialise a node's {@code param}. */
    byte[] serializeParam(String processName, Serializable param) throws SerDeException;

    /** Inverse of {@link #serializeParam}. */
    Object loadParam(String processName, byte[] bytes) throws SerDeException;
}
