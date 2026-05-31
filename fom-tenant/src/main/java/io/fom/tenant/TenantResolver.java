package io.fom.tenant;

import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Maps a process name to its {@link TenantId}. The default
 * {@link #suffixAfter(String)} factory matches the convention used through
 * the spec: {@code "Inventory_PUB123" → TenantId("PUB123")}.
 */
@FunctionalInterface
public interface TenantResolver {

    /** Returns the tenant a process belongs to, or empty if the name is not tenant-scoped. */
    Optional<TenantId> resolve(String processName);

    /** Suffix-based resolver: anything after the last occurrence of {@code separator}. */
    static TenantResolver suffixAfter(String separator) {
        if (separator == null || separator.isEmpty()) {
            throw new IllegalArgumentException("separator must be non-empty");
        }
        return name -> {
            int idx = name.lastIndexOf(separator);
            if (idx < 0 || idx == name.length() - separator.length()) return Optional.empty();
            return Optional.of(TenantId.of(name.substring(idx + separator.length())));
        };
    }

    /** Regex-based resolver: tenant id taken from the first capture group. */
    static TenantResolver regex(Pattern pattern) {
        if (pattern == null) {
            throw new IllegalArgumentException("pattern must be non-null");
        }
        return name -> {
            var m = pattern.matcher(name);
            if (m.matches() && m.groupCount() >= 1) {
                return Optional.of(TenantId.of(m.group(1)));
            }
            return Optional.empty();
        };
    }

    /** Custom resolver from a function. */
    static TenantResolver of(Function<String, Optional<TenantId>> fn) {
        return fn::apply;
    }
}
