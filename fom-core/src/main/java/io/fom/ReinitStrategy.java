package io.fom;

/**
 * What a process does with its serving version while it re-initialises (after a trigger, a
 * watcher tick or a reactive dependency change).
 */
public enum ReinitStrategy {
    /**
     * The old version keeps answering queries until the new one has initialised and loaded; if the
     * new one fails, the old one keeps serving. Peak memory holds both versions for a while.
     */
    KEEP_OLD,
    /**
     * The old version is retired first ({@code LogDead}, {@code cleanUp}), then the new one
     * initialises; queries wait meanwhile, and a failed re-init leaves the process {@code Dead}.
     * Never holds two versions at once.
     */
    RELEASE_FIRST
}
