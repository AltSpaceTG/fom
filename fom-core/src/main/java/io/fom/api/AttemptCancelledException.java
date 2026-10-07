package io.fom.api;

import java.util.concurrent.CancellationException;

/**
 * An init or load attempt the engine itself cut short — a pause, a removal, a graph swap,
 * {@code close()} or {@code cancelInit} — reported to {@link EngineObserver#onInitFailed} or
 * {@link EngineObserver#onLoadFailed}.
 *
 * <p>It lets an observer tell an operator's stop from a real failure: a plain
 * {@link CancellationException} can also come from the user's own init or load code (a timeout
 * inside it, say), which the engine treats as an ordinary failed attempt.</p>
 */
public final class AttemptCancelledException extends CancellationException {

    private static final long serialVersionUID = 1L;

    public AttemptCancelledException(String message) {
        super(message);
    }
}
