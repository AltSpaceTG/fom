/**
 * Tenant-aware Engine wrapper: convention naming + per-tenant authz + lifecycle.
 */
module io.fom.tenant {
    requires transitive io.fom.core;
    requires org.slf4j;

    exports io.fom.tenant;
}
