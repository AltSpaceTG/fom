plugins {
    `java-library`
    `java-test-fixtures`
}

description = "fom core — Java 21 process framework runtime. Zero non-JDK deps except slf4j."

dependencies {
    api("org.slf4j:slf4j-api:2.0.16")

    testFixturesImplementation(platform("org.junit:junit-bom:5.11.4"))
    testFixturesImplementation("org.junit.jupiter:junit-jupiter")
    testFixturesImplementation("org.assertj:assertj-core:3.27.3")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
