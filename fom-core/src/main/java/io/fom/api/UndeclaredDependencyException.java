package io.fom.api;

import java.util.Objects;

/**
 * A process called {@code ctx.query} with a dependency its node does not declare. For that
 * process it is a bug, so its attempt fails at once instead of retrying. A consumer that gets
 * it as another process's answer retries as usual; {@link #requester()} tells the two apart.
 */
public class UndeclaredDependencyException extends QueryException {

    private static final long serialVersionUID = 1L;

    private final String requester;

    public UndeclaredDependencyException(String requester, String message) {
        super(message);
        this.requester = Objects.requireNonNull(requester, "requester");
    }

    /** The process whose own {@code ctx.query} named the undeclared dependency. */
    public String requester() {
        return requester;
    }
}
