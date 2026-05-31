package io.fom.api;

/**
 * Thrown by the engine when {@code init.timeout} elapses across all retry
 * attempts.
 */
public class InitializationTimeoutException extends RuntimeException {

    public InitializationTimeoutException(String message) {
        super(message);
    }

    public InitializationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
