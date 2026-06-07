# Contributing to FluentX

Thank you for your interest! Here's how to get started.

## Development Setup

1. Fork and clone the repo
2. Requires Java 17+ and Gradle 8+
3. Run `./gradlew build` to verify everything compiles

## Guidelines

- One utility method per PR is totally fine
- Every method must have a Javadoc example
- Every method must have JUnit 5 tests covering happy path + edge cases
- No external runtime dependencies — FluentX has zero deps

## Adding a New Method to FluentStream

1. Add the method to `FluentStream.java` with full Javadoc
2. Add tests in `FluentStreamTest.java`
3. Update `README.md` examples if it's a headline feature
4. Open a PR — we review within a few days

## Code Style

- Java 17 idioms (records, `var`, `List.of`, `Stream.toList()`)
- Prefer `Objects.requireNonNull` for null checks
- Keep methods focused and composable
