description = "FluentX Strings — Fluent, chainable string utilities for Java 17+"

// TODO: Implement FluentString with slugify, truncate, mask, similarity, camelToSnake

tasks.withType<Javadoc> {
    (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:none", true)
}
