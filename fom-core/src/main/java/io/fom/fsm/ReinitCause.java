package io.fom.fsm;

import java.io.Serializable;

/** Why a {@link ProcessFSM} is re-initialised. */
public sealed interface ReinitCause extends Serializable {

    /** Manual {@code engine.trigger(name, value)} or a watcher tick. */
    record Triggered(Serializable value) implements ReinitCause { }

    /** A reactive dependency got a new Sid. */
    record DependencyChanged(String depName, long oldDepClock, long newDepClock) implements ReinitCause { }
}
