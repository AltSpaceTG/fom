plugins {
    `java-library`
}

description = "fom core — Java 21 process framework runtime. Zero non-JDK deps except slf4j."

dependencies {
    api("org.slf4j:slf4j-api:2.0.16")

    testImplementation(project(":fom-test"))
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
