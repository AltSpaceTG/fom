package io.fom.tenant;

/**
 * Thrown when an authorization policy refuses access to a tenant's process.
 */
public class TenantAccessDeniedException extends RuntimeException {

    public TenantAccessDeniedException(String message) {
        super(message);
    }
}
