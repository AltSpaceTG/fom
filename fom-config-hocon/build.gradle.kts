plugins {
    `java-library`
}

description = "HOCON parser for EngineConfig + cron syntax for SnapshotPolicy."

dependencies {
    api(project(":fom-core"))
    api("com.typesafe:config:1.4.3")
    api("com.cronutils:cron-utils:9.2.1")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
