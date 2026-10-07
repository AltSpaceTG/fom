package io.fom.api;

/** Thrown by {@code init} when it cannot finish; the engine retries with backoff until the init budget runs out. */
public class InitializationException extends RuntimeException {

    public InitializationException(String message) {
        super(message);
    }

    public InitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
