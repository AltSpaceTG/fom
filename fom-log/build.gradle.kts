plugins {
    `java-library`
    application
}

description = "Standalone CLI utility for inspecting, diagnosing and compacting fom log files."

// An slf4j provider for the CLI distribution only (silences "No SLF4J providers");
// the published library stays binding-free.
val cliRuntime: Configuration by configurations.creating

dependencies {
    api(project(":fom-core"))
    implementation("info.picocli:picocli:4.7.6")
    cliRuntime("org.slf4j:slf4j-nop:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("io.fom.log.cli.FomLogCli")
}

distributions {
    main {
        contents {
            from(cliRuntime) {
                into("lib")
                exclude { it.name.startsWith("slf4j-api") } // already on the runtime classpath
            }
        }
    }
}

tasks.named<CreateStartScripts>("startScripts") {
    classpath = classpath!! + cliRuntime
}

tasks.named<JavaExec>("run") {
    classpath += cliRuntime
}
