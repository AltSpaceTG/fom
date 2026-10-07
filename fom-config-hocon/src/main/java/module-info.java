/**
 * HOCON parser for {@link io.fom.EngineConfig} plus Quartz cron support for
 * {@link io.fom.SnapshotPolicy}.
 */
module io.fom.config.hocon {
    requires transitive io.fom.core;
    requires org.slf4j;
    requires transitive typesafe.config;
    requires com.cronutils;

    exports io.fom.config;
}
