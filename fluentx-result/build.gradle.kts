description = "FluentX Result — Checked-exception-safe Result<T,E> type for Java 17+"

// TODO: Implement Result<T,E> / Try<T> with fluent API and Stream integration

tasks.withType<Javadoc> {
    (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:none", true)
}
