package io.fom.api;

/**
 * Thrown by {@code ProcessInitializer.init()} when initialisation cannot
 * complete. The engine applies exponential backoff and retries until
 * {@code init.timeout} fires.
 */
public class InitializationException extends RuntimeException {

    public InitializationException(String message) {
        super(message);
    }

    public InitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
