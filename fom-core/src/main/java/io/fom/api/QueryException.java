package io.fom.api;

/** A query that cannot be routed, such as one to an undeclared dependency. */
public class QueryException extends RuntimeException {

    public QueryException(String message) {
        super(message);
    }

    public QueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
