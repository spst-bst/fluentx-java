plugins {
    `java-platform`
    `maven-publish`
    signing
}

description = "FluentX BOM — Bill of Materials for all FluentX modules"

dependencies {
    constraints {
        api(project(":fluentx-streams"))
        api(project(":fluentx-gatherers"))
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["javaPlatform"])
            pom {
                name.set("fluentx-bom")
                description.set(project.description)
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
}
