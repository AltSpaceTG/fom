package io.fom.tenant;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * The principal making a tenant-aware call, usually an authenticated identity from the
 * application (a JWT subject, an OAuth user). {@link #anonymous()} belongs to no tenant.
 */
public record TenantCaller(String identity, Set<TenantId> tenants) {

    public TenantCaller {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(tenants, "tenants");
        tenants = Set.copyOf(tenants);
    }

    public static TenantCaller anonymous() {
        return new TenantCaller("anonymous", Set.of());
    }

    /** Repeated tenant ids (e.g. from token claims) are collapsed. */
    public static TenantCaller of(String identity, TenantId... tenants) {
        return new TenantCaller(identity, Set.copyOf(Arrays.asList(tenants)));
    }
}
