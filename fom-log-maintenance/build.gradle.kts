plugins {
    `java-library`
}

description = "Additional snapshot strategies: size-based + composite policies."

dependencies {
    api(project(":fom-core"))

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
