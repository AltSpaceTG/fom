plugins {
    `java-library`
}

description = "Contract tests for fom SPIs (LogBackend, SerDe, Process interrupts) — extend them from your implementation's tests."

dependencies {
    api(project(":fom-core"))
    api(platform("org.junit:junit-bom:5.11.4"))
    api("org.junit.jupiter:junit-jupiter-api")
    api("org.assertj:assertj-core:3.27.3")

    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
