package io.fom.jdbc;

import io.fom.log.LogBackend;
import io.fom.log.LogBackendContractTest;
import io.fom.log.LogInitialized;
import io.fom.log.LogLeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the shared {@link LogBackendContractTest} against a real Postgres
 * (Testcontainers) plus a few Postgres-specific tests (advisory-lock leadership,
 * reopen). The contract enforces the SPI invariants — including 0-based clocks
 * and out-of-range {@code IndexOutOfBoundsException} — that the prior hand-rolled
 * tests did not cover.
 */
@Testcontainers
class PostgresLogBackendTest extends LogBackendContractTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("fom")
                    .withUsername("test")
                    .withPassword("test");

    private static DataSource dataSource;
    private String logId;

    @BeforeAll
    static void setupDataSource() {
        var ds = new PGSimpleDataSource();
        ds.setUrl(postgres.getJdbcUrl());
        ds.setUser(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        dataSource = ds;
    }

    @Override
    protected LogBackend create() {
        logId = "ct_" + UUID.randomUUID().toString().replace("-", "");
        try {
            return new PostgresLogBackend(dataSource, logId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected LogBackend reopen(LogBackend original) {
        try {
            return new PostgresLogBackend(dataSource, logId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // ───────────────── Postgres-specific tests ─────────────────

    @Test
    void second_open_while_first_holds_advisory_lock_fails_fast() {
        // backend (from the contract's setUp) already holds the advisory lock on logId.
        assertThatThrownBy(() -> new PostgresLogBackend(dataSource, logId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("advisory lock");
    }

    @Test
    void reads_back_persisted_events_after_reopen() throws SQLException {
        backend.append(new LogLeader(0, 1000L, "instance-A"), "instance-A");
        backend.append(new LogInitialized(0, 1001L, "Foo",
                Map.of("k1", new byte[]{1, 2}, "k2", new byte[]{3})), "instance-A");

        assertThat(backend.length()).isEqualTo(2);
        backend.close();
        try (var reopened = new PostgresLogBackend(dataSource, logId)) {
            assertThat(reopened.length()).isEqualTo(2);
            assertThat(reopened.get(1)).isInstanceOf(LogInitialized.class);
        }
        backend = null; // already closed; suppress the contract tearDown double-close
    }
}
