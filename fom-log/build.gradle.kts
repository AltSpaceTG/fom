plugins {
    `java-library`
    application
}

description = "Standalone CLI utility for inspecting/migrating fom log files."

dependencies {
    api(project(":fom-core"))
    implementation("info.picocli:picocli:4.7.6")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("io.fom.log.cli.FomLogCli")
}
