/**
 * Postgres-backed LogBackend for multi-node leader coordination.
 */
module io.fom.jdbc {
    requires transitive io.fom.core;
    requires transitive java.sql;
    requires org.postgresql.jdbc;
    requires org.slf4j;

    exports io.fom.jdbc;
}
