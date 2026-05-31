package io.fom.api;

/**
 * Thrown synchronously by {@code QueryableContext.query()} for unrecoverable
 * routing errors — e.g. querying a name that is not declared in
 * {@code dependencies()}.
 */
public class QueryException extends RuntimeException {

    public QueryException(String message) {
        super(message);
    }

    public QueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
