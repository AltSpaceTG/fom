package io.fom.api;

/**
 * Returned to a waiter whose top-level query timed out while the target
 * process was still initialising. The init lifecycle is independent of
 * query timeouts and continues regardless.
 */
public class InitInProgressException extends RuntimeException {

    public InitInProgressException(String processName) {
        super("Init in progress for process: " + processName);
    }

    public InitInProgressException(String processName, Throwable cause) {
        super("Init in progress for process: " + processName, cause);
    }
}
