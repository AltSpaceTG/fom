package com.evil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * Test stand-in for a deserialization "gadget": a {@link Serializable} class
 * that lives outside the {@code io.fom.*} / {@code java.*} allowlist and runs
 * code from its {@code readObject}. A correct {@link io.fom.serde.ObjectInputFilters}
 * allowlist must reject it <em>before</em> {@code readObject} runs, so
 * {@link #EXECUTED} stays {@code false}.
 */
public final class Gadget implements Serializable {

    /** Flipped true if this class is ever actually deserialized (i.e. the filter failed). */
    public static volatile boolean EXECUTED = false;

    public static void reset() {
        EXECUTED = false;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        EXECUTED = true; // stand-in for "attacker code ran"
    }
}
