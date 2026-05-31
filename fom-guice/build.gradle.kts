plugins {
    `java-library`
}

description = "Google Guice DI adapter for fom — SerializableSupplier resolves bindings."

dependencies {
    api(project(":fom-core"))
    api("com.google.inject:guice:7.0.0")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
