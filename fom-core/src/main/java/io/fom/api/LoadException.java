package io.fom.api;

/**
 * Thrown by {@code load} when persisted state can't be revived. After
 * {@code maxLoadRetries} failures the engine runs {@code init} instead.
 */
public class LoadException extends RuntimeException {

    public LoadException(String message) {
        super(message);
    }

    public LoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
