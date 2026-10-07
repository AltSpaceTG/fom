package io.fom;

/** Thrown by {@link Codec#decode(byte[])} for bytes that don't decode to the expected type. */
public class CodecException extends RuntimeException {

    public CodecException(String message) {
        super(message);
    }

    public CodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
