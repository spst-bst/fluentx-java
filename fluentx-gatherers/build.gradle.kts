description = "FluentX Gatherers — Stream Gatherer interop for FluentStream operations"

dependencies {
    api(project(":fluentx-streams"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Gatherers (JEP 485) are STABLE from JDK 24. This module compiles and runs on a
// Java 24 toolchain — no preview flags, no version-locked classfiles. The produced
// artifact requires a Java 24+ runtime (the rest of FluentX stays on Java 17).
// ─────────────────────────────────────────────────────────────────────────────

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(24)
    }
    // Override the Java 17 source/target inherited from the root build so the
    // bytecode itself requires Java 24 and fails fast on older runtimes.
    sourceCompatibility = JavaVersion.VERSION_24
    targetCompatibility = JavaVersion.VERSION_24
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}
