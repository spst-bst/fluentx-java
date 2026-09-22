plugins {
    java
    `maven-publish`
    signing
    id("com.diffplug.spotless") version "7.0.2" apply false
}

allprojects {
    group = "io.fluentx"
    version = "0.1.0-SNAPSHOT"
}

subprojects {
    if (name != "fluentx-bom") {
        apply(plugin = "java-library")
        apply(plugin = "maven-publish")
        apply(plugin = "signing")
        apply(plugin = "jacoco")
        apply(plugin = "com.diffplug.spotless")

        java {
            sourceCompatibility = JavaVersion.VERSION_17
            targetCompatibility = JavaVersion.VERSION_17
            withJavadocJar()
            withSourcesJar()
        }

        repositories {
            mavenCentral()
        }

        // ── Formatting hygiene (non-destructive: no AST reformat) ─────────────
        configure<com.diffplug.gradle.spotless.SpotlessExtension> {
            java {
                target("src/**/*.java")
                importOrder()
                removeUnusedImports()
                trimTrailingWhitespace()
                endWithNewline()
            }
        }

        // ── Coverage: report on every test run, gate in `check` ───────────────
        configure<JacocoPluginExtension> {
            toolVersion = "0.8.13" // 0.8.13+ parses Java 24 (class file 68) bytecode
        }
        tasks.withType<org.gradle.testing.jacoco.tasks.JacocoReport>().configureEach {
            dependsOn(tasks.named("test"))
            reports {
                xml.required.set(true)
                html.required.set(true)
            }
        }
        // The coverage gate applies only to the published library modules.
        // fluentx-examples and fluentx-benchmarks carry runnable/JMH code with no
        // unit tests, so a coverage minimum is not meaningful for them.
        if (name == "fluentx-streams" || name == "fluentx-gatherers") {
            tasks.withType<org.gradle.testing.jacoco.tasks.JacocoCoverageVerification>().configureEach {
                dependsOn(tasks.named("test"))
                violationRules {
                    rule {
                        limit {
                            counter = "INSTRUCTION"
                            minimum = "0.90".toBigDecimal()
                        }
                    }
                }
            }
            tasks.named("check") {
                dependsOn("jacocoTestCoverageVerification")
            }
        }

        dependencies {
            testImplementation(platform("org.junit:junit-bom:5.11.0"))
            testImplementation("org.junit.jupiter:junit-jupiter")
            testRuntimeOnly("org.junit.platform:junit-platform-launcher")
        }

        tasks.withType<JavaCompile> {
            options.encoding = "UTF-8"
        }

        tasks.withType<Javadoc> {
            options.encoding = "UTF-8"
            (options as StandardJavadocDocletOptions).apply {
                charSet("UTF-8")
                addBooleanOption("Xdoclint:none", true)
            }
        }

        tasks.test {
            useJUnitPlatform()
        }

        publishing {
            publications {
                create<MavenPublication>("maven") {
                    from(components["java"])
                    pom {
                        name.set(project.name)
                        description.set(project.description ?: "FluentX — Fluent Java utility library")
                        url.set("https://github.com/spst-bst/fluentx-java")
                        licenses {
                            license {
                                name.set("Apache License, Version 2.0")
                                url.set("https://www.apache.org/licenses/LICENSE-2.0")
                            }
                        }
                        developers {
                            developer {
                                id.set("spst-bst")
                                name.set("FluentX Contributors")
                                url.set("https://github.com/spst-bst")
                            }
                        }
                        scm {
                            connection.set("scm:git:git://github.com/spst-bst/fluentx-java.git")
                            developerConnection.set("scm:git:ssh://github.com/spst-bst/fluentx-java.git")
                            url.set("https://github.com/spst-bst/fluentx-java")
                        }
                    }
                }
            }
            repositories {
                maven {
                    val releasesRepo = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
                    val snapshotsRepo = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
                    url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepo else releasesRepo
                    credentials {
                        username = project.findProperty("ossrhUsername") as String? ?: ""
                        password = project.findProperty("ossrhPassword") as String? ?: ""
                    }
                }
            }
        }

        signing {
            sign(publishing.publications["maven"])
        }
    }
}
