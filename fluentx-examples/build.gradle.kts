plugins {
    application
}

description = "FluentX Examples — Runnable consumer examples for all FluentX modules"

application {
    mainClass.set("io.fluentx.examples.AllExamples")
}

dependencies {
    implementation(project(":fluentx-streams"))
}

// Examples are runnable demos, not published API — suppress Javadoc linting
tasks.withType<Javadoc> {
    (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:none", true)
}
