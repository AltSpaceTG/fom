package io.fom.examples;

import io.fom.jdbc.PostgresLogBackend;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Multi-node leadership with the Postgres backend: the first instance to open a
 * {@code logId} acquires a Postgres advisory lock and becomes the leader; a
 * second instance opening the same {@code logId} fails fast.
 *
 * <p>This example needs a running Postgres. Pass a JDBC URL (and optional
 * user/password) as arguments, or via the {@code FOM_PG_URL} / {@code FOM_PG_USER}
 * / {@code FOM_PG_PASSWORD} environment variables. With nothing provided it
 * prints instructions and exits.</p>
 *
 * <pre>
 *   # spin up a throwaway Postgres:
 *   docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=test --name fom-pg postgres:16-alpine
 *
 *   ./gradlew :examples:multiNodePostgres \
 *     -PexampleArgs="jdbc:postgresql://localhost:5432/postgres postgres test"
 * </pre>
 */
public final class MultiNodePostgresExample {

    public static void main(String[] args) throws Exception {
        String url = arg(args, 0, System.getenv("FOM_PG_URL"));
        String user = arg(args, 1, System.getenv().getOrDefault("FOM_PG_USER", "postgres"));
        String password = arg(args, 2, System.getenv().getOrDefault("FOM_PG_PASSWORD", "test"));

        if (url == null || url.isBlank()) {
            System.out.println("""
                    No Postgres URL provided — this example needs a running database.

                    Start one:
                      docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=test \\
                        --name fom-pg postgres:16-alpine

                    Then run:
                      ./gradlew :examples:multiNodePostgres \\
                        -PexampleArgs="jdbc:postgresql://localhost:5432/postgres postgres test"
                    """);
            return;
        }

        var ds = new PGSimpleDataSource();
        ds.setUrl(url);
        ds.setUser(user);
        ds.setPassword(password);

        String logId = "examples_multinode";

        // First instance: becomes leader by taking the advisory lock.
        try (var leader = new PostgresLogBackend(ds, logId)) {
            System.out.println("Instance #1 acquired leadership on logId='" + logId + "'");

            // Second instance on the same logId must fail fast.
            try (var ignored = new PostgresLogBackend(ds, logId)) {
                System.out.println("Instance #2: UNEXPECTEDLY also acquired leadership");
            } catch (IllegalStateException e) {
                System.out.println("Instance #2 correctly rejected: " + e.getMessage());
            }
        }

        // Once instance #1 closed (releasing the lock), a new instance can take over.
        try (var takeover = new PostgresLogBackend(ds, logId)) {
            System.out.println("After #1 closed, instance #3 acquired leadership (failover OK)");
        }
    }

    private static String arg(String[] args, int i, String fallback) {
        return i < args.length ? args[i] : fallback;
    }
}
