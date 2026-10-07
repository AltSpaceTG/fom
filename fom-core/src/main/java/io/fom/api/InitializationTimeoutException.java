package io.fom.api;

/** The init budget ran out across all retries. */
public class InitializationTimeoutException extends RuntimeException {

    public InitializationTimeoutException(String message) {
        super(message);
    }

    public InitializationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
