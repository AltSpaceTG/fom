package io.fom.api;

/**
 * Thrown when a process cannot accept a query: it is unknown, paused, shutting
 * down or {@code Dead}.
 */
public class QueryRejectedException extends RuntimeException {

    public QueryRejectedException(String message) {
        super(message);
    }

    public QueryRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
