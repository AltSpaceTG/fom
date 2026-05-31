/**
 * Micrometer adapter for fom EngineObserver.
 */
module io.fom.micrometer {
    requires transitive io.fom.core;
    requires transitive micrometer.core;
    requires org.slf4j;

    exports io.fom.micrometer;
}
