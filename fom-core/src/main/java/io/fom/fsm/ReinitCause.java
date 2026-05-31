package io.fom.fsm;

import java.io.Serializable;

/**
 * Why a {@link ProcessFSM} is being re-initialised. Stored in
 * {@code LogDependencyChanged} / {@code LogTrigger} for tracing and used by
 * {@code GraphMachine}'s dedup window to collapse multiple causes targeting
 * the same process within a single firing.
 */
public sealed interface ReinitCause extends Serializable {

    /** Manual {@code engine.trigger(name, value)} or a watcher tick. */
    record Triggered(Serializable value) implements ReinitCause { }

    /** A reactive dependency's Sid changed; the consumer re-inits to consume the new state. */
    record DependencyChanged(String depName, int oldDepClock, int newDepClock) implements ReinitCause { }
}
