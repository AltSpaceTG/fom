package io.fom.tenant;

import java.util.Objects;
import java.util.Set;

/**
 * Identifies the principal making a tenant-aware call. The default
 * {@link #anonymous()} caller has access to no tenant. Real callers will
 * typically wrap an authenticated identity from the surrounding application
 * (a JWT subject, an OAuth user, etc.).
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

    public static TenantCaller of(String identity, TenantId... tenants) {
        return new TenantCaller(identity, Set.of(tenants));
    }
}
