package io.fom.api;

/**
 * Thrown by {@code ProcessLoader.load()} when persisted state can't be revived.
 * After {@code load.max-retries} consecutive failures the engine falls back
 * to {@link ProcessInitializer}.
 */
public class LoadException extends RuntimeException {

    public LoadException(String message) {
        super(message);
    }

    public LoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
