plugins {
    `java-library`
}

description = "Apache Fury-backed SerDe for fom — recommended for prod."

dependencies {
    api(project(":fom-core"))
    api("org.apache.fury:fury-core:0.10.3")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(testFixtures(project(":fom-core")))
}
