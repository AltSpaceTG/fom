plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
}

rootProject.name = "fom"

include("fom-core")
include("fom-fury")
include("fom-config-hocon")
include("fom-guice")
include("fom-spring")
include("fom-kotlin")
include("fom-micrometer")
include("fom-otel")
include("fom-test")
include("fom-tenant")
include("fom-log")
include("fom-log-maintenance")
include("fom-jdbc")
