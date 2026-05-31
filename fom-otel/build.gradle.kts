plugins {
    `java-library`
}

description = "OpenTelemetry tracing for fom — spans around init/load/compute."

dependencies {
    api(project(":fom-core"))
    api("io.opentelemetry:opentelemetry-api:1.45.0")
    testImplementation("io.opentelemetry:opentelemetry-sdk:1.45.0")
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing:1.45.0")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
