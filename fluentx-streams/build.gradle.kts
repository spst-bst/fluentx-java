description = "FluentX Streams — Extended Stream utilities for Java 17+"

tasks.withType<Javadoc> {
    (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:none", true)
}
