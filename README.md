# FluentX

> Fluent Java utility library — the missing parts of the JDK Stream and Collections APIs.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/java-17%2B-orange.svg)](https://openjdk.org/projects/jdk/17/)

## Modules

| Module | Description |
|--------|-------------|
| `fluentx-streams` | `zipWithIndex`, `zip`, `scan`, `chunk`, `window`, `groupConsecutive`, `takeUntil`, `distinctBy` |
| `fluentx-collections` | *(coming soon)* `mapValues`, `filterKeys`, `groupBy`, `keyBy` |
| `fluentx-strings` | *(coming soon)* `slugify`, `truncate`, `mask`, `similarity` |
| `fluentx-result` | *(coming soon)* `Result<T,E>` / `Try<T>` for exception-safe lambdas |

## Quick Start

```xml
<!-- Maven -->
<dependency>
    <groupId>io.fluentx</groupId>
    <artifactId>fluentx-streams</artifactId>
    <version>0.1.0</version>
</dependency>
```

```kotlin
// Gradle (Kotlin DSL)
implementation("io.fluentx:fluentx-streams:0.1.0")
```

## Examples

```java
// Iterate with index — no more external counter
FluentStream.of("a", "b", "c")
    .zipWithIndex()
    .forEach(e -> System.out.println(e.index() + ": " + e.value()));
// 0: a  1: b  2: c

// Running accumulation (scan)
FluentStream.of(1, 2, 3, 4)
    .scan(0, Integer::sum)
    .forEach(System.out::println);
// 0  1  3  6  10

// Sliding window
FluentStream.of(1, 2, 3, 4, 5)
    .window(3)
    .forEach(System.out::println);
// [1, 2, 3]  [2, 3, 4]  [3, 4, 5]

// Fixed-size chunks
FluentStream.of(1, 2, 3, 4, 5)
    .chunk(2)
    .forEach(System.out::println);
// [1, 2]  [3, 4]  [5]

// Distinct by key
FluentStream.of("apple", "apricot", "banana")
    .distinctBy(s -> s.charAt(0))
    .forEach(System.out::println);
// apple  banana

// Group consecutive equal elements
FluentStream.of(1, 1, 2, 3, 3, 1)
    .groupConsecutive()
    .forEach(System.out::println);
// [1, 1]  [2]  [3, 3]  [1]
```

## Building

```bash
gradle wrapper
./gradlew build test
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
