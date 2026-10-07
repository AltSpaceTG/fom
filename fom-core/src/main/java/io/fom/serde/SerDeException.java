package io.fom.serde;

/** Thrown by a {@link SerDe} that cannot write or read a value. */
public class SerDeException extends RuntimeException {

    public SerDeException(String message) {
        super(message);
    }

    public SerDeException(String message, Throwable cause) {
        super(message, cause);
    }
}
