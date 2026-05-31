package io.fom;

/**
 * Thrown by {@link Codec#decode(byte[])} when the byte sequence is malformed
 * for the expected type.
 */
public class CodecException extends RuntimeException {

    public CodecException(String message) {
        super(message);
    }

    public CodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
