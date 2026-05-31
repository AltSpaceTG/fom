package io.fom.serde;

/**
 * Thrown by {@link SerDe} when (de)serialisation fails.
 */
public class SerDeException extends RuntimeException {

    public SerDeException(String message) {
        super(message);
    }

    public SerDeException(String message, Throwable cause) {
        super(message, cause);
    }
}
