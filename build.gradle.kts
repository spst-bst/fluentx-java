plugins {
    java
    `maven-publish`
    signing
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

        java {
            sourceCompatibility = JavaVersion.VERSION_17
            targetCompatibility = JavaVersion.VERSION_17
            withJavadocJar()
            withSourcesJar()
        }

        repositories {
            mavenCentral()
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
                                email.set("saiprasadt@gmail.com")
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
