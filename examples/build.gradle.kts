plugins {
    kotlin("jvm") version "2.2.0"
}

description = "Runnable examples for fom. Not published to Maven Central."

dependencies {
    implementation(project(":fom-core"))
    implementation(project(":fom-fury"))
    implementation(project(":fom-kotlin"))
    implementation(project(":fom-micrometer"))
    implementation(project(":fom-tenant"))
    implementation(project(":fom-config-hocon"))
    implementation(project(":fom-jdbc"))

    // Print engine logs to the console when running an example.
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

kotlin {
    jvmToolchain(21)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

// Each example is a class with a main(). Register one Gradle task per example
// so you can run e.g. `./gradlew :examples:quickstart`.
val examples = linkedMapOf(
    "quickstart" to "io.fom.examples.QuickstartExample",
    "warmRestart" to "io.fom.examples.WarmRestartExample",
    "multiProcess" to "io.fom.examples.MultiProcessExample",
    "triggersWatchers" to "io.fom.examples.TriggersAndWatchersExample",
    "snapshots" to "io.fom.examples.SnapshotExample",
    "multiTenant" to "io.fom.examples.MultiTenantExample",
    "observability" to "io.fom.examples.ObservabilityExample",
    "furySerde" to "io.fom.examples.FurySerdeExample",
    "kotlinDsl" to "io.fom.examples.KotlinDslExampleKt",
    "multiNodePostgres" to "io.fom.examples.MultiNodePostgresExample",
)

examples.forEach { (taskName, mainClassName) ->
    tasks.register<JavaExec>(taskName) {
        group = "fom examples"
        description = "Run the '$taskName' example ($mainClassName)."
        mainClass.set(mainClassName)
        classpath = sourceSets["main"].runtimeClasspath
        // Allow passing args through: ./gradlew :examples:multiNodePostgres -PexampleArgs="jdbc:..."
        if (project.hasProperty("exampleArgs")) {
            args((project.property("exampleArgs") as String).split(" "))
        }
    }
}
