package io.fom.api;

/**
 * A query timed out while its process was still initialising. The init itself
 * carries on.
 */
public class InitInProgressException extends RuntimeException {

    public InitInProgressException(String processName) {
        super("Init in progress for process: " + processName);
    }

    public InitInProgressException(String processName, Throwable cause) {
        super("Init in progress for process: " + processName, cause);
    }

    private InitInProgressException(String message, boolean ignored) {
        super(message);
    }

    /** The init of {@code processName} was cancelled by an operator ({@code Engine.cancelInit}). */
    public static InitInProgressException cancelled(String processName) {
        return new InitInProgressException("Init of process '" + processName + "' was cancelled", false);
    }
}
