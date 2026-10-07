/**
 * fom core runtime — process-graph framework on Java 21.
 * Zero non-JDK runtime dependencies except {@code org.slf4j}.
 */
module io.fom.core {
    requires java.base;
    requires org.slf4j;

    exports io.fom;
    exports io.fom.api;
    exports io.fom.log;
    exports io.fom.serde;
}
