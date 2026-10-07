package io.fom.tenant;

import java.io.Serializable;
import java.util.Objects;

/**
 * Opaque tenant identifier used as the key for per-tenant authorization,
 * lifecycle and metrics tagging.
 */
public record TenantId(String value) implements Serializable {

    public TenantId {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("TenantId value must not be empty");
        }
    }

    public static TenantId of(String value) {
        return new TenantId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
