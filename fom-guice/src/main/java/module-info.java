/**
 * Google Guice DI adapter for fom — {@link io.fom.SerializableSupplier} resolves bindings.
 */
module io.fom.guice {
    requires transitive io.fom.core;
    requires transitive com.google.guice;
    requires org.slf4j;

    exports io.fom.guice;
}
