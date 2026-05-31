plugins {
    `java-library`
}

description = "Spring DI adapter for fom — SerializableSupplier resolves beans."

dependencies {
    api(project(":fom-core"))
    api("org.springframework:spring-context:6.2.0")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
