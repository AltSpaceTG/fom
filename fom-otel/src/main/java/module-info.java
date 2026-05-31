/**
 * OpenTelemetry tracing for fom — spans around init/load/query.
 */
module io.fom.otel {
    requires transitive io.fom.core;
    requires transitive io.opentelemetry.api;
    requires org.slf4j;

    exports io.fom.otel;
}
