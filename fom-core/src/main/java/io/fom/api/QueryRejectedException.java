package io.fom.api;

/**
 * Thrown when a bounded mailbox refused the inbound query due to its
 * {@code overflow-policy = reject} setting.
 */
public class QueryRejectedException extends RuntimeException {

    public QueryRejectedException(String message) {
        super(message);
    }

    public QueryRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
