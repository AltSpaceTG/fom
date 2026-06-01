allprojects {
    group = "io.github.altspacetg"
    version = "0.1.0-SNAPSHOT"
}

// Shared staging repository: every module publishes into one Maven-layout
// directory under the root build dir, which is then zipped into a single
// bundle for upload to the Maven Central Portal. See docs/guides/publishing.md.
val stagingDir = layout.buildDirectory.dir("staging-deploy")

subprojects {
    repositories {
        mavenCentral()
    }

    plugins.withId("java-library") {
        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(21))
            }
            withSourcesJar()
            withJavadocJar()
        }

        // Test fixtures (fom-core) are a test-only artifact — don't publish them
        // to Maven Central. Suppress the variant so it stays off the 'java' component.
        plugins.withId("java-test-fixtures") {
            val component = components["java"] as AdhocComponentWithVariants
            component.withVariantsFromConfiguration(
                configurations["testFixturesApiElements"]) { skip() }
            component.withVariantsFromConfiguration(
                configurations["testFixturesRuntimeElements"]) { skip() }
        }

        tasks.withType<JavaCompile>().configureEach {
            options.encoding = "UTF-8"
            options.release.set(21)
            options.compilerArgs.addAll(
                listOf(
                    "-Xlint:all,-serial,-processing,-requires-automatic,-requires-transitive-automatic",
                    "-Werror",
                    "-parameters",
                )
            )
        }

        tasks.withType<Javadoc>().configureEach {
            (options as StandardJavadocDocletOptions).apply {
                addStringOption("Xdoclint:none", "-quiet")
                encoding = "UTF-8"
                charSet = "UTF-8"
            }
        }

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            testLogging {
                events("passed", "skipped", "failed")
            }
        }
    }

    // ───────────────── Maven Central publishing ─────────────────
    // Applies to every module that produces a JVM library (Java or Kotlin),
    // except the non-library `examples` module.
    plugins.withId("java") {
        if (project.path != ":examples") {
            configurePublishing(stagingDir)
        }
    }
}

/**
 * Configure maven-publish + signing for a library module using only Gradle's
 * built-in plugins (no third-party dependency to download). Each module:
 *  - publishes its main jar + sources + javadoc,
 *  - attaches Central-required POM metadata,
 *  - signs artifacts when a signing key is provided,
 *  - deploys into the shared staging directory.
 */
fun Project.configurePublishing(stagingDir: Provider<Directory>) {
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    // Kotlin-jvm modules don't apply 'java-library', so ensure sources/javadoc
    // jars exist for them too.
    extensions.findByType(JavaPluginExtension::class.java)?.apply {
        withSourcesJar()
        withJavadocJar()
    }

    extensions.configure<PublishingExtension> {
        publications {
            create<MavenPublication>("maven") {
                from(components["java"])
                pom {
                    name.set(this@configurePublishing.name)
                    description.set(
                        provider { this@configurePublishing.description }
                            .orElse("FOM — JVM process-graph framework")
                    )
                    url.set("https://github.com/AltSpaceTG/fom")
                    inceptionYear.set("2026")
                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                            distribution.set("repo")
                        }
                    }
                    developers {
                        developer {
                            id.set("AltSpaceTG")
                            name.set("Danila Sorokin")
                            url.set("https://github.com/AltSpaceTG")
                        }
                    }
                    scm {
                        url.set("https://github.com/AltSpaceTG/fom")
                        connection.set("scm:git:https://github.com/AltSpaceTG/fom.git")
                        developerConnection.set("scm:git:ssh://git@github.com/AltSpaceTG/fom.git")
                    }
                }
            }
        }
        repositories {
            maven {
                name = "staging"
                url = uri(stagingDir)
            }
        }
    }

    extensions.configure<SigningExtension> {
        // Sign only when a key is actually configured, so local/CI builds without
        // keys (and SNAPSHOT builds) don't fail. Central requires .asc signatures.
        val inMemoryKey = providers.gradleProperty("signingInMemoryKey").orNull
        val keyId = providers.gradleProperty("signing.keyId").orNull
        isRequired = inMemoryKey != null || keyId != null
        if (inMemoryKey != null) {
            useInMemoryPgpKeys(
                inMemoryKey,
                providers.gradleProperty("signingInMemoryKeyPassword").orElse("").get(),
            )
        }
        if (isRequired) {
            sign(extensions.getByType(PublishingExtension::class.java).publications["maven"])
        }
    }
}

// ───────────────── bundle for the Central Portal ─────────────────
// Zips the staging Maven layout into a single archive the Portal accepts.
tasks.register<Zip>("centralBundle") {
    group = "publishing"
    description = "Zip the staged Maven artifacts into a Central Portal upload bundle."
    dependsOn(subprojects.mapNotNull { it.tasks.findByName("publishMavenPublicationToStagingRepository") })
    from(stagingDir)
    archiveFileName.set("central-bundle.zip")
    destinationDirectory.set(layout.buildDirectory)
}
