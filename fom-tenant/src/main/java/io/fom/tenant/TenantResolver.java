package io.fom.tenant;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Maps a process name to its {@link TenantId}. A resolver must never guess:
 * when a name does not clearly belong to one tenant it returns empty, and
 * {@link TenantAwareEngine} then denies access unless the process is declared
 * global.
 */
@FunctionalInterface
public interface TenantResolver {

    /** The tenant a process belongs to, or empty if the name does not resolve to exactly one. */
    Optional<TenantId> resolve(String processName);

    /**
     * Strict suffix convention {@code <base><separator><tenant>}, e.g.
     * {@code "Stations_PUB123" → PUB123}. The separator must occur exactly once
     * and both parts must be non-empty; any other name (no separator, several,
     * like {@code "Stations_ACME_EU"}) does not resolve. Use {@link #regex} or
     * {@link #registry} when base names or tenant ids contain the separator.
     */
    static TenantResolver suffixAfter(String separator) {
        if (separator == null || separator.isEmpty()) {
            throw new IllegalArgumentException("separator must be non-empty");
        }
        return name -> {
            int idx = name.indexOf(separator);
            int tenantStart = idx + separator.length();
            if (idx <= 0 || tenantStart >= name.length() || name.indexOf(separator, idx + 1) >= 0) {
                return Optional.empty();
            }
            return Optional.of(TenantId.of(name.substring(tenantStart)));
        };
    }

    /** Regex-based resolver: the whole name must match; the tenant id is the first capture group. */
    static TenantResolver regex(Pattern pattern) {
        if (pattern == null) {
            throw new IllegalArgumentException("pattern must be non-null");
        }
        return name -> {
            var m = pattern.matcher(name);
            if (m.matches() && m.groupCount() >= 1 && m.group(1) != null && !m.group(1).isEmpty()) {
                return Optional.of(TenantId.of(m.group(1)));
            }
            return Optional.empty();
        };
    }

    /** Explicit process → tenant map; names not in it do not resolve. */
    static TenantResolver registry(Map<String, TenantId> processToTenant) {
        Map<String, TenantId> copy = Map.copyOf(Objects.requireNonNull(processToTenant, "processToTenant"));
        return name -> Optional.ofNullable(copy.get(name));
    }

    /** Custom resolver from a function. */
    static TenantResolver of(Function<String, Optional<TenantId>> fn) {
        return fn::apply;
    }
}
