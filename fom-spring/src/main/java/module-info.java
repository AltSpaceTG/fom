/**
 * Spring DI adapter for fom — {@link io.fom.SerializableSupplier} resolves beans.
 */
module io.fom.spring {
    requires transitive io.fom.core;
    requires transitive spring.context;
    requires org.slf4j;

    exports io.fom.spring;
}
