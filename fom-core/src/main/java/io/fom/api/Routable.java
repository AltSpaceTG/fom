package io.fom.api;

/**
 * A query message that names its own target process; this wins over the graph's type
 * routing. Handy when a message ships in a library with the process that serves it.
 *
 * <p>{@link #targetProcess()} must be pure and fast: the engine may call it more than once.</p>
 */
public interface Routable {

    String targetProcess();
}
