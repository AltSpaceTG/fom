/**
 * Apache Fury-backed {@link io.fom.serde.SerDe} for fom — recommended for prod.
 */
module io.fom.fury {
    requires transitive io.fom.core;
    requires org.slf4j;
    requires org.apache.fury.core;

    exports io.fom.fury;
}
