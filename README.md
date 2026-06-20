# FluentX

> Fluent Java utility library — the missing parts of the JDK Stream and Collections APIs.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/java-17%2B-orange.svg)](https://openjdk.org/projects/jdk/17/)

## Modules

| Module | Description |
|--------|-------------|
| `fluentx-streams` | `zipWithIndex`, `zip`, `zipWithNext`, `scan`, `chunk`, `window`, `groupConsecutive`, `takeUntil`, `distinctBy`, `interleave`, `crossProduct`, `partition`, `frequencies` |
| `fluentx-gatherers` | The same operations as standard `Gatherer`s for `Stream.gather(...)` (requires Java 24+) |
| `fluentx-bom` | Bill of Materials for aligned dependency versions |

> **Status:** `0.1.0-SNAPSHOT` — pre-release, not yet published to Maven Central.

## Quick Start

Snapshots are published to the Sonatype snapshots repository:

```xml
<!-- Maven -->
<repositories>
    <repository>
        <id>ossrh-snapshots</id>
        <url>https://s01.oss.sonatype.org/content/repositories/snapshots/</url>
    </repository>
</repositories>

<dependency>
    <groupId>io.fluentx</groupId>
    <artifactId>fluentx-streams</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```kotlin
// Gradle (Kotlin DSL)
repositories {
    maven("https://s01.oss.sonatype.org/content/repositories/snapshots/")
}
implementation("io.fluentx:fluentx-streams:0.1.0-SNAPSHOT")
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

// Pair each element with its successor
FluentStream.of(1, 2, 3, 4)
    .zipWithNext()
    .forEach(System.out::println);
// (1,2)  (2,3)  (3,4)

// Partition into matching / non-matching in one pass
var p = FluentStream.of(1, 2, 3, 4, 5).partition(n -> n % 2 == 0);
// p.matching() -> [2, 4]   p.notMatching() -> [1, 3, 5]

// Frequency count
FluentStream.of("a", "b", "a", "c", "a").frequencies();
// {a=3, b=1, c=1}
```

> **Parallel streams:** the stateful operations (`zipWithIndex`, `scan`, `chunk`,
> `window`, `groupConsecutiveBy`, `distinctBy`, `zip`, `zipWithNext`, `interleave`)
> require a sequential source and throw `IllegalStateException` on a parallel stream
> rather than silently degrading. Call `.sequential()` before wrapping if needed.

### Gatherers (JDK 22+)

Prefer native pipelines? `fluentx-gatherers` exposes the same operations as standard
[`Gatherer`](https://docs.oracle.com/en/java/javase/24/docs/api/java.base/java/util/stream/Gatherer.html)s:

```java
import static io.fluentx.gatherers.FluentGatherers.*;

List<List<Integer>> windows = Stream.of(1, 2, 3, 4, 5)
        .gather(window(3))
        .toList();
// [1,2,3]  [2,3,4]  [3,4,5]
```

`Stream.gather(...)` is stable from JDK 24 (JEP 485), so this module requires a
Java 24+ runtime. The core `fluentx-streams` module stays Java 17 compatible.

## Building

```bash
gradle wrapper
./gradlew build test
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
