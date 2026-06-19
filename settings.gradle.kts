plugins {
    // Auto-provisions JDK toolchains (e.g. the Java 24 toolchain used by
    // fluentx-gatherers) so CI does not depend on locally installed JDKs.
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
}

rootProject.name = "fluentx-java"

include(
    "fluentx-streams",
    "fluentx-gatherers",
    "fluentx-bom",
    "fluentx-examples",
    "fluentx-benchmarks"
)
