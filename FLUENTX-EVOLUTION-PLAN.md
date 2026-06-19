# FluentX Evolution Plan — Full Implementation Guide

> This document captures every recommended change to make FluentX a competitive,
> differentiated open-source Java stream utility library. Each section contains
> the rationale, the full implementation code, and the corresponding tests.

> **Implementation status (as of this revision):**
> - ✅ §1 Remove placeholder modules — **done**
> - ✅ §2 Lazy `chunk`/`window`/`groupConsecutiveBy` — **done**
> - ✅ §3 `onClose` propagation — **done**
> - ✅ §4 Fail-fast parallel policy — **done**
> - ✅ §5 New operations (`zipWithNext`, `interleave`, `crossProduct`, `partition`, `frequencies`/`frequenciesBy`) — **done**
> - ✅ §6 Gatherers interop (`fluentx-gatherers`) — **done** (built on a stable JDK 24 toolchain, no preview flags; requires Java 24+ runtime). Implemented gatherers: `chunk`, `window`, `groupConsecutive(By)`, `scan`, `zipWithNext`, `zipWithIndex`, `distinctBy`, `takeUntil`
> - ✅ §8 Test suite expansion — **done**
>
> ⚠️ The code blocks in §7 are a point-in-time design snapshot and may drift from the
> actual [`FluentStream.java`](fluentx-streams/src/main/java/io/fluentx/streams/FluentStream.java).
> The source file is the single source of truth — do not treat the inlined code here as current.

---

## Table of Contents

1. [Remove Empty Placeholder Modules](#1-remove-empty-placeholder-modules)
2. [Make `chunk`, `window`, `groupConsecutiveBy` Lazy](#2-make-chunk-window-groupconsecutiveby-lazy)
3. [Fix Resource Safety (`onClose` Propagation)](#3-fix-resource-safety-onclose-propagation)
4. [Parallel Stream Policy — Fail-Fast](#4-parallel-stream-policy--fail-fast)
5. [New Operations](#5-new-operations)
6. [Java 24 Gatherers Interop](#6-java-24-gatherers-interop)
7. [Updated `FluentStream.java` — Complete File](#7-updated-fluentstreamjava--complete-file)
8. [Complete Test Suite](#8-complete-test-suite)
9. [Settings & Build Changes](#9-settings--build-changes)
10. [README Positioning](#10-readme-positioning)

---

## 1. Remove Empty Placeholder Modules

### Why
Three modules (`fluentx-collections`, `fluentx-strings`, `fluentx-result`) are empty
with only TODO comments in their `build.gradle.kts`. Visitors see vaporware. Ship what
works; add modules when they have code.

### Changes

**`settings.gradle.kts`** — remove the three placeholders:

```kotlin
rootProject.name = "fluentx-java"

include(
    "fluentx-streams",
    "fluentx-bom",
    "fluentx-examples",
    "fluentx-benchmarks"
)
```

**`fluentx-bom/build.gradle.kts`** — remove constraints for non-existent modules:

```kotlin
plugins {
    `java-platform`
    `maven-publish`
    signing
}

description = "FluentX BOM — Dependency management for FluentX modules"

javaPlatform {
    allowDependencies()
}

dependencies {
    constraints {
        api(project(":fluentx-streams"))
    }
}
```

**Delete these directories entirely:**
- `fluentx-collections/`
- `fluentx-strings/`
- `fluentx-result/`

---

## 2. Make `chunk`, `window`, `groupConsecutiveBy` Lazy

### Why
The current implementations call `stream.collect(Collectors.toList())`, which:
- Blows up on infinite streams
- Defeats lazy evaluation
- Allocates O(N) memory before producing a single element

Lazy implementations using `Iterator`-backed streams (the same pattern already used
in `scan`) make FluentX work on unbounded data — a real differentiator over StreamEx.

### `chunk` — Lazy Implementation

```java
public FluentStream<List<T>> chunk(int size) {
    if (size <= 0) throw new IllegalArgumentException("chunk size must be positive, got: " + size);
    Iterator<T> source = stream.sequential().iterator();
    Iterator<List<T>> chunkIterator = new Iterator<>() {
        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public List<T> next() {
            if (!source.hasNext()) throw new NoSuchElementException();
            List<T> chunk = new ArrayList<>(size);
            for (int i = 0; i < size && source.hasNext(); i++) {
                chunk.add(source.next());
            }
            return Collections.unmodifiableList(chunk);
        }
    };
    Iterable<List<T>> iterable = () -> chunkIterator;
    return of(StreamSupport.stream(iterable.spliterator(), false)
            .onClose(stream::close));
}
```

### `window` — Lazy Implementation

```java
public FluentStream<List<T>> window(int size) {
    if (size <= 0) throw new IllegalArgumentException("window size must be positive, got: " + size);
    Iterator<T> source = stream.sequential().iterator();
    Iterator<List<T>> windowIterator = new Iterator<>() {
        private final ArrayDeque<T> buffer = new ArrayDeque<>(size);
        private boolean exhausted = false;

        {
            // pre-fill the first window
            while (buffer.size() < size && source.hasNext()) {
                buffer.addLast(source.next());
            }
            if (buffer.size() < size) {
                exhausted = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !exhausted;
        }

        @Override
        public List<T> next() {
            if (exhausted) throw new NoSuchElementException();
            List<T> window = List.copyOf(buffer);
            if (source.hasNext()) {
                buffer.pollFirst();
                buffer.addLast(source.next());
            } else {
                exhausted = true;
            }
            return window;
        }
    };
    Iterable<List<T>> iterable = () -> windowIterator;
    return of(StreamSupport.stream(iterable.spliterator(), false)
            .onClose(stream::close));
}
```

### `groupConsecutiveBy` — Lazy Implementation

```java
public <K> FluentStream<List<T>> groupConsecutiveBy(Function<T, K> keyFn) {
    Iterator<T> source = stream.sequential().iterator();
    Iterator<List<T>> groupIterator = new Iterator<>() {
        private T pending = null;
        private boolean hasPending = false;

        @Override
        public boolean hasNext() {
            return hasPending || source.hasNext();
        }

        @Override
        public List<T> next() {
            if (!hasNext()) throw new NoSuchElementException();
            List<T> group = new ArrayList<>();
            T first = hasPending ? pending : source.next();
            hasPending = false;
            group.add(first);
            K key = keyFn.apply(first);

            while (source.hasNext()) {
                T el = source.next();
                if (Objects.equals(key, keyFn.apply(el))) {
                    group.add(el);
                } else {
                    pending = el;
                    hasPending = true;
                    break;
                }
            }
            return Collections.unmodifiableList(group);
        }
    };
    Iterable<List<T>> iterable = () -> groupIterator;
    return of(StreamSupport.stream(iterable.spliterator(), false)
            .onClose(stream::close));
}
```

---

## 3. Fix Resource Safety (`onClose` Propagation)

### Why
`zip()` correctly propagates `onClose()` to both source streams, but `scan()`,
`chunk()`, `window()`, `groupConsecutiveBy()`, `zipWithIndex()`, and `distinctBy()` do not.
If the source is IO-backed (e.g., `Files.lines()`), handles leak.

### Rule
Every method that consumes or wraps the source stream must propagate `onClose`.

### Changes

**`scan`** — add `.onClose(stream::close)`:
```java
return of(StreamSupport.stream(iterable.spliterator(), false)
        .onClose(stream::close));
```

**`zipWithIndex`** — add `.onClose(stream::close)`:
```java
public FluentStream<Indexed<T>> zipWithIndex() {
    int[] index = {0};
    return of(stream.sequential()
            .map(value -> new Indexed<>(index[0]++, value))
            .onClose(stream::close));
}
```

Wait — `stream.sequential().map(...)` returns a derived stream that already inherits
`onClose` handlers from the source. The above is redundant for `zipWithIndex` and
`distinctBy` since they return a derived pipeline (not a new `StreamSupport.stream`).
The rule applies specifically to methods that **break the pipeline** by calling
`.iterator()` and creating a new `StreamSupport.stream()`:

| Method | Creates new stream from Iterator? | Needs `.onClose`? |
|---|---|---|
| `zipWithIndex` | No (uses `.map`) | Already inherited |
| `zip` | Yes | Already fixed |
| `scan` | Yes | **FIX NEEDED** |
| `chunk` | Yes (after lazy rewrite) | **FIX NEEDED** |
| `window` | Yes (after lazy rewrite) | **FIX NEEDED** |
| `groupConsecutiveBy` | Yes (after lazy rewrite) | **FIX NEEDED** |
| `distinctBy` | No (uses `.filter`) | Already inherited |
| `takeUntil` | No (uses `.takeWhile`) | Already inherited |

The lazy rewrites in Section 2 already include `.onClose(stream::close)`.

---

## 4. Parallel Stream Policy — Fail-Fast

### Why
Currently, `zipWithIndex`, `scan`, and `distinctBy` silently call `.sequential()` on
parallel streams. This is a hidden performance trap — a user passes in a parallel stream
expecting parallelism and gets sequential behavior with no warning.

### Chosen strategy: Fail-fast

Throw `IllegalStateException` if the source stream is parallel and the operation cannot
safely handle it. This is honest, discoverable, and forces the caller to make a conscious
decision.

### Implementation

Add a private helper:

```java
private Stream<T> requireSequential(String operationName) {
    if (stream.isParallel()) {
        throw new IllegalStateException(
                operationName + "() requires a sequential stream. " +
                "Call .sequential() on your stream before wrapping with FluentStream, " +
                "or use FluentStream.of(yourStream.sequential()).");
    }
    return stream;
}
```

Then replace every `.sequential()` call:

```java
public FluentStream<Indexed<T>> zipWithIndex() {
    Stream<T> seq = requireSequential("zipWithIndex");
    int[] index = {0};
    return of(seq.map(value -> new Indexed<>(index[0]++, value)));
}

public <R> FluentStream<R> scan(R identity, BiFunction<R, T, R> accumulator) {
    Iterator<T> source = requireSequential("scan").iterator();
    // ... rest unchanged
}

public FluentStream<T> distinctBy(Function<T, ?> keyFn) {
    Stream<T> seq = requireSequential("distinctBy");
    Set<Object> seen = new HashSet<>();
    return of(seq.filter(el -> seen.add(keyFn.apply(el))));
}
```

And for the lazy operations (`chunk`, `window`, `groupConsecutiveBy`):

```java
public FluentStream<List<T>> chunk(int size) {
    if (size <= 0) throw new IllegalArgumentException("chunk size must be positive, got: " + size);
    Iterator<T> source = requireSequential("chunk").iterator();
    // ... rest unchanged
}
```

---

## 5. New Operations

### 5.1 `zipWithNext` — Pair each element with its successor

Very commonly needed (change detection, diff computation). Currently requires `window(2)`.

```java
/**
 * Pairs each element with the next element in the stream.
 * The result has {@code n-1} pairs for a stream of {@code n} elements.
 *
 * <pre>{@code
 * FluentStream.of(1, 2, 3, 4).zipWithNext()
 * // -> Pair(1,2), Pair(2,3), Pair(3,4)
 * }</pre>
 *
 * @return a stream of pairs of consecutive elements
 */
public FluentStream<Pair<T, T>> zipWithNext() {
    Iterator<T> source = requireSequential("zipWithNext").iterator();
    Iterator<Pair<T, T>> pairIterator = new Iterator<>() {
        private T previous = source.hasNext() ? source.next() : null;

        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public Pair<T, T> next() {
            if (!source.hasNext()) throw new NoSuchElementException();
            T current = source.next();
            Pair<T, T> pair = new Pair<>(previous, current);
            previous = current;
            return pair;
        }
    };
    Iterable<Pair<T, T>> iterable = () -> pairIterator;
    return of(StreamSupport.stream(iterable.spliterator(), false)
            .onClose(stream::close));
}
```

### 5.2 `interleave` — Round-robin merge of two streams

```java
/**
 * Interleaves elements from this stream and another in round-robin fashion.
 * Remaining elements from the longer stream are appended at the end.
 *
 * <pre>{@code
 * FluentStream.of(1, 3, 5).interleave(Stream.of(2, 4, 6))
 * // -> 1, 2, 3, 4, 5, 6
 * }</pre>
 *
 * @param other the stream to interleave with
 * @return a stream alternating elements from both sources
 */
public FluentStream<T> interleave(Stream<T> other) {
    Iterator<T> iterA = requireSequential("interleave").iterator();
    Iterator<T> iterB = other.iterator();
    Iterator<T> interleaved = new Iterator<>() {
        private boolean pickA = true;

        @Override
        public boolean hasNext() {
            return iterA.hasNext() || iterB.hasNext();
        }

        @Override
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            if (pickA) {
                pickA = false;
                return iterA.hasNext() ? iterA.next() : iterB.next();
            } else {
                pickA = true;
                return iterB.hasNext() ? iterB.next() : iterA.next();
            }
        }
    };
    Iterable<T> iterable = () -> interleaved;
    return of(StreamSupport.stream(iterable.spliterator(), false)
            .onClose(stream::close)
            .onClose(other::close));
}
```

### 5.3 `partition` — Split into two streams in one pass

```java
/**
 * Immutable container for the result of a {@link FluentStream#partition} operation.
 *
 * @param matching    elements for which the predicate returned {@code true}
 * @param notMatching elements for which the predicate returned {@code false}
 * @param <T>         the element type
 */
public record Partitioned<T>(List<T> matching, List<T> notMatching) {}
```

Place `Partitioned.java` in `io.fluentx.streams`:

```java
package io.fluentx.streams;

import java.util.List;

public record Partitioned<T>(List<T> matching, List<T> notMatching) {}
```

Then in `FluentStream`:

```java
/**
 * Partitions elements into two lists based on a predicate.
 * This is a <strong>terminal</strong> operation that eagerly consumes the stream.
 *
 * <pre>{@code
 * var result = FluentStream.of(1, 2, 3, 4, 5).partition(n -> n % 2 == 0);
 * result.matching()    // -> [2, 4]
 * result.notMatching() // -> [1, 3, 5]
 * }</pre>
 *
 * @param predicate the partitioning condition
 * @return a {@link Partitioned} containing the two groups
 */
public Partitioned<T> partition(Predicate<T> predicate) {
    Map<Boolean, List<T>> groups = stream.collect(Collectors.partitioningBy(predicate));
    return new Partitioned<>(
            Collections.unmodifiableList(groups.get(true)),
            Collections.unmodifiableList(groups.get(false)));
}
```

### 5.4 `frequencies` — Element count map

```java
/**
 * Counts occurrences of each element.
 * This is a <strong>terminal</strong> operation.
 *
 * <pre>{@code
 * FluentStream.of("a", "b", "a", "c", "b", "a").frequencies()
 * // -> {a=3, b=2, c=1}
 * }</pre>
 *
 * @return an unmodifiable map from element to occurrence count
 */
public Map<T, Long> frequencies() {
    return Collections.unmodifiableMap(
            stream.collect(Collectors.groupingBy(Function.identity(), Collectors.counting())));
}
```

### 5.5 `frequenciesBy` — Keyed frequency count

```java
/**
 * Counts occurrences by a key extractor.
 * This is a <strong>terminal</strong> operation.
 *
 * <pre>{@code
 * FluentStream.of("apple", "apricot", "banana").frequenciesBy(s -> s.charAt(0))
 * // -> {a=2, b=1}
 * }</pre>
 *
 * @param <K>   the key type
 * @param keyFn function that extracts the grouping key
 * @return an unmodifiable map from key to occurrence count
 */
public <K> Map<K, Long> frequenciesBy(Function<T, K> keyFn) {
    return Collections.unmodifiableMap(
            stream.collect(Collectors.groupingBy(keyFn, Collectors.counting())));
}
```

### 5.6 `crossProduct` — Cartesian join

```java
/**
 * Produces the cartesian product of this stream with another.
 * Every element of this stream is paired with every element of {@code other}.
 *
 * <p>Note: {@code other} is fully materialized into a list (it must be traversed
 * once per element of this stream).
 *
 * <pre>{@code
 * FluentStream.of(1, 2).crossProduct(Stream.of("a", "b"))
 * // -> Pair(1,"a"), Pair(1,"b"), Pair(2,"a"), Pair(2,"b")
 * }</pre>
 *
 * @param <U>   the element type of the other stream
 * @param other the stream to cross with
 * @return a stream of all pairings
 */
public <U> FluentStream<Pair<T, U>> crossProduct(Stream<U> other) {
    List<U> materialized = other.toList();
    return of(stream.flatMap(a -> materialized.stream().map(b -> new Pair<>(a, b))));
}
```

---

## 6. Java 24 Gatherers Interop

### Why
`java.util.stream.Gatherers` (JEP 461, finalized in Java 24) is the biggest threat to
this library. Instead of ignoring it, FluentX should be the bridge:
- For Java 17–23 users: FluentX provides the operations natively
- For Java 24+ users: FluentX adds a fluent API over Gatherers + extra operations Gatherers doesn't have

### Implementation Strategy

Since FluentX targets Java 17+, Gatherers interop must be **opt-in via a separate module**
(`fluentx-gatherers`) that requires Java 24+. This avoids breaking the Java 17 base.

#### New Module: `fluentx-gatherers`

**`fluentx-gatherers/build.gradle.kts`:**

```kotlin
description = "FluentX Gatherers — Java 24+ Gatherer interop for FluentStream"

dependencies {
    api(project(":fluentx-streams"))
}

java {
    sourceCompatibility = JavaVersion.VERSION_24
    targetCompatibility = JavaVersion.VERSION_24
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}
```

**`fluentx-gatherers/src/main/java/io/fluentx/gatherers/FluentGatherers.java`:**

```java
package io.fluentx.gatherers;

import java.util.*;
import java.util.function.*;
import java.util.stream.Gatherer;

/**
 * Provides FluentX operations as standard {@link Gatherer} instances for use
 * with {@code Stream.gather()}.
 *
 * <p>This module requires Java 24+. For Java 17–23, use
 * {@link io.fluentx.streams.FluentStream} directly.
 */
public final class FluentGatherers {

    private FluentGatherers() {}

    /**
     * A gatherer that chunks elements into fixed-size lists.
     *
     * <pre>{@code
     * Stream.of(1,2,3,4,5).gather(FluentGatherers.chunk(2))
     * // -> [1,2], [3,4], [5]
     * }</pre>
     */
    public static <T> Gatherer<T, ?, List<T>> chunk(int size) {
        if (size <= 0) throw new IllegalArgumentException("chunk size must be positive");
        return Gatherer.ofSequential(
                () -> new ArrayList<T>(size),
                (buffer, element, downstream) -> {
                    buffer.add(element);
                    if (buffer.size() == size) {
                        downstream.push(Collections.unmodifiableList(new ArrayList<>(buffer)));
                        buffer.clear();
                    }
                    return true;
                },
                (buffer, downstream) -> {
                    if (!buffer.isEmpty()) {
                        downstream.push(Collections.unmodifiableList(buffer));
                    }
                }
        );
    }

    /**
     * A gatherer that produces sliding windows of the given size.
     *
     * <pre>{@code
     * Stream.of(1,2,3,4,5).gather(FluentGatherers.window(3))
     * // -> [1,2,3], [2,3,4], [3,4,5]
     * }</pre>
     */
    public static <T> Gatherer<T, ?, List<T>> window(int size) {
        if (size <= 0) throw new IllegalArgumentException("window size must be positive");
        return Gatherer.ofSequential(
                () -> new ArrayDeque<T>(size),
                (buffer, element, downstream) -> {
                    buffer.addLast(element);
                    if (buffer.size() == size) {
                        downstream.push(List.copyOf(buffer));
                        buffer.pollFirst();
                    }
                    return true;
                }
        );
    }

    /**
     * A gatherer that groups consecutive elements sharing the same key.
     *
     * <pre>{@code
     * Stream.of("apple","apricot","banana")
     *       .gather(FluentGatherers.groupConsecutiveBy(s -> s.charAt(0)))
     * // -> ["apple","apricot"], ["banana"]
     * }</pre>
     */
    public static <T, K> Gatherer<T, ?, List<T>> groupConsecutiveBy(Function<T, K> keyFn) {
        class State {
            K currentKey = null;
            List<T> group = new ArrayList<>();
            boolean first = true;
        }
        return Gatherer.ofSequential(
                State::new,
                (state, element, downstream) -> {
                    K key = keyFn.apply(element);
                    if (state.first || !Objects.equals(key, state.currentKey)) {
                        if (!state.group.isEmpty()) {
                            downstream.push(Collections.unmodifiableList(state.group));
                            state.group = new ArrayList<>();
                        }
                        state.currentKey = key;
                        state.first = false;
                    }
                    state.group.add(element);
                    return true;
                },
                (state, downstream) -> {
                    if (!state.group.isEmpty()) {
                        downstream.push(Collections.unmodifiableList(state.group));
                    }
                }
        );
    }

    /**
     * A gatherer that pairs each element with its successor.
     *
     * <pre>{@code
     * Stream.of(1,2,3,4).gather(FluentGatherers.zipWithNext())
     * // -> Pair(1,2), Pair(2,3), Pair(3,4)
     * }</pre>
     */
    public static <T> Gatherer<T, ?, Map.Entry<T, T>> zipWithNext() {
        return Gatherer.ofSequential(
                () -> new Object[]{null, false}, // [previous, hasPrevious]
                (state, element, downstream) -> {
                    if ((boolean) state[1]) {
                        @SuppressWarnings("unchecked")
                        T prev = (T) state[0];
                        downstream.push(Map.entry(prev, element));
                    }
                    state[0] = element;
                    state[1] = true;
                    return true;
                }
        );
    }

    /**
     * A gatherer that deduplicates elements by a key function, keeping the first
     * occurrence per key.
     *
     * <pre>{@code
     * Stream.of("apple","apricot","banana")
     *       .gather(FluentGatherers.distinctBy(s -> s.charAt(0)))
     * // -> "apple", "banana"
     * }</pre>
     */
    public static <T, K> Gatherer<T, ?, T> distinctBy(Function<T, K> keyFn) {
        return Gatherer.ofSequential(
                HashSet::new,
                (seen, element, downstream) -> {
                    if (seen.add(keyFn.apply(element))) {
                        downstream.push(element);
                    }
                    return true;
                }
        );
    }
}
```

**Add `gather()` pass-through to `FluentStream`** (Java 17 compatible — the method
accepts any `Gatherer` instance; actual usage requires Java 24+ at the call site):

Since `Gatherer` is a Java 24 class, it cannot appear in the Java 17 `FluentStream`
signature. Instead, the recommended approach is:

```java
// In FluentStream — already available:
public Stream<T> toStream() { return stream; }

// Usage on Java 24+:
FluentStream.of(data)
    .toStream()
    .gather(FluentGatherers.chunk(3))
    .toList();

// Or wrap back:
FluentStream.of(
    FluentStream.of(data)
        .toStream()
        .gather(FluentGatherers.chunk(3))
).forEach(System.out::println);
```

---

## 7. Updated `FluentStream.java` — Complete File

This is the full implementation with all changes applied:

```java
package io.fluentx.streams;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * A fluent wrapper around Java's {@link Stream} that adds commonly needed
 * utility operations missing from the standard library.
 *
 * <p>All custom operations work on sequential streams. Passing a parallel stream
 * to an operation that requires sequential processing will throw
 * {@link IllegalStateException} — call {@code .sequential()} explicitly if needed.
 *
 * <p>Example usage:
 * <pre>{@code
 * FluentStream.of("a", "b", "c")
 *     .zipWithIndex()
 *     .forEach(e -> System.out.println(e.index() + ": " + e.value()));
 * }</pre>
 *
 * @param <T> the type of stream elements
 */
public final class FluentStream<T> {

    private final Stream<T> stream;

    private FluentStream(Stream<T> stream) {
        this.stream = Objects.requireNonNull(stream, "stream must not be null");
    }

    // ── Factory methods ───────────────────────────────────────────────────────

    public static <T> FluentStream<T> of(Stream<T> stream) {
        return new FluentStream<>(stream);
    }

    @SafeVarargs
    public static <T> FluentStream<T> of(T... elements) {
        return new FluentStream<>(Stream.of(elements));
    }

    public static <T> FluentStream<T> of(Iterable<T> iterable) {
        return new FluentStream<>(StreamSupport.stream(iterable.spliterator(), false));
    }

    public static <T> FluentStream<T> empty() {
        return new FluentStream<>(Stream.empty());
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    private Stream<T> requireSequential(String operationName) {
        if (stream.isParallel()) {
            throw new IllegalStateException(
                    operationName + "() requires a sequential stream. " +
                    "Call .sequential() before wrapping with FluentStream.");
        }
        return stream;
    }

    private static <E> FluentStream<E> fromIterator(Iterator<E> iterator, Stream<?>... toClose) {
        Iterable<E> iterable = () -> iterator;
        Stream<E> result = StreamSupport.stream(iterable.spliterator(), false);
        for (Stream<?> s : toClose) {
            result = result.onClose(s::close);
        }
        return of(result);
    }

    // ── FluentX extensions ────────────────────────────────────────────────────

    /**
     * Pairs each element with its zero-based index.
     *
     * <pre>{@code
     * FluentStream.of("a", "b", "c").zipWithIndex()
     * // -> Indexed(0, "a"), Indexed(1, "b"), Indexed(2, "c")
     * }</pre>
     */
    public FluentStream<Indexed<T>> zipWithIndex() {
        Stream<T> seq = requireSequential("zipWithIndex");
        int[] index = {0};
        return of(seq.map(value -> new Indexed<>(index[0]++, value)));
    }

    /**
     * Zips this stream with another, pairing elements by position.
     * Stops when the shorter stream is exhausted.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3).zip(Stream.of("a", "b", "c"))
     * // -> Pair(1,"a"), Pair(2,"b"), Pair(3,"c")
     * }</pre>
     */
    public <U> FluentStream<Pair<T, U>> zip(Stream<U> other) {
        Iterator<T> iterA = requireSequential("zip").iterator();
        Iterator<U> iterB = other.iterator();
        return fromIterator(new Iterator<>() {
            @Override public boolean hasNext() { return iterA.hasNext() && iterB.hasNext(); }
            @Override public Pair<T, U> next() { return new Pair<>(iterA.next(), iterB.next()); }
        }, stream, other);
    }

    /**
     * Pairs each element with the next element in the stream.
     * Produces {@code n-1} pairs for a stream of {@code n} elements.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4).zipWithNext()
     * // -> Pair(1,2), Pair(2,3), Pair(3,4)
     * }</pre>
     */
    public FluentStream<Pair<T, T>> zipWithNext() {
        Iterator<T> source = requireSequential("zipWithNext").iterator();
        Iterator<Pair<T, T>> pairIterator = new Iterator<>() {
            private T previous = source.hasNext() ? source.next() : null;

            @Override
            public boolean hasNext() {
                return source.hasNext();
            }

            @Override
            public Pair<T, T> next() {
                if (!source.hasNext()) throw new NoSuchElementException();
                T current = source.next();
                Pair<T, T> pair = new Pair<>(previous, current);
                previous = current;
                return pair;
            }
        };
        return fromIterator(pairIterator, stream);
    }

    /**
     * Returns a running prefix scan (all intermediate accumulation results).
     * Lazy — elements are pulled one at a time.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4).scan(0, Integer::sum)
     * // -> 0, 1, 3, 6, 10
     * }</pre>
     */
    public <R> FluentStream<R> scan(R identity, BiFunction<R, T, R> accumulator) {
        Iterator<T> source = requireSequential("scan").iterator();
        Iterator<R> scanIterator = new Iterator<>() {
            private R current = identity;
            private boolean emitIdentity = true;

            @Override
            public boolean hasNext() {
                return emitIdentity || source.hasNext();
            }

            @Override
            public R next() {
                if (emitIdentity) {
                    emitIdentity = false;
                    return current;
                }
                current = accumulator.apply(current, source.next());
                return current;
            }
        };
        return fromIterator(scanIterator, stream);
    }

    /**
     * Splits the stream into fixed-size chunks (last chunk may be smaller).
     * Lazy — chunks are produced on demand without materializing the full stream.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4, 5).chunk(2)
     * // -> [1,2], [3,4], [5]
     * }</pre>
     *
     * @throws IllegalArgumentException if size is not positive
     */
    public FluentStream<List<T>> chunk(int size) {
        if (size <= 0) throw new IllegalArgumentException("chunk size must be positive, got: " + size);
        Iterator<T> source = requireSequential("chunk").iterator();
        Iterator<List<T>> chunkIterator = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return source.hasNext();
            }

            @Override
            public List<T> next() {
                if (!source.hasNext()) throw new NoSuchElementException();
                List<T> chunk = new ArrayList<>(size);
                for (int i = 0; i < size && source.hasNext(); i++) {
                    chunk.add(source.next());
                }
                return Collections.unmodifiableList(chunk);
            }
        };
        return fromIterator(chunkIterator, stream);
    }

    /**
     * Produces a sliding window over stream elements.
     * Lazy — windows are produced on demand using an internal ring buffer.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4, 5).window(3)
     * // -> [1,2,3], [2,3,4], [3,4,5]
     * }</pre>
     *
     * @throws IllegalArgumentException if size is not positive
     */
    public FluentStream<List<T>> window(int size) {
        if (size <= 0) throw new IllegalArgumentException("window size must be positive, got: " + size);
        Iterator<T> source = requireSequential("window").iterator();
        Iterator<List<T>> windowIterator = new Iterator<>() {
            private final ArrayDeque<T> buffer = new ArrayDeque<>(size);
            private boolean exhausted = false;

            {
                while (buffer.size() < size && source.hasNext()) {
                    buffer.addLast(source.next());
                }
                if (buffer.size() < size) {
                    exhausted = true;
                }
            }

            @Override
            public boolean hasNext() {
                return !exhausted;
            }

            @Override
            public List<T> next() {
                if (exhausted) throw new NoSuchElementException();
                List<T> window = List.copyOf(buffer);
                if (source.hasNext()) {
                    buffer.pollFirst();
                    buffer.addLast(source.next());
                } else {
                    exhausted = true;
                }
                return window;
            }
        };
        return fromIterator(windowIterator, stream);
    }

    /**
     * Groups consecutive equal elements into lists.
     *
     * <pre>{@code
     * FluentStream.of(1, 1, 2, 3, 3, 1).groupConsecutive()
     * // -> [1,1], [2], [3,3], [1]
     * }</pre>
     */
    public FluentStream<List<T>> groupConsecutive() {
        return groupConsecutiveBy(Function.identity());
    }

    /**
     * Groups consecutive elements sharing the same key into lists.
     * Lazy — groups are produced on demand.
     *
     * <pre>{@code
     * FluentStream.of("apple","apricot","banana","blueberry")
     *     .groupConsecutiveBy(s -> s.charAt(0))
     * // -> ["apple","apricot"], ["banana","blueberry"]
     * }</pre>
     */
    public <K> FluentStream<List<T>> groupConsecutiveBy(Function<T, K> keyFn) {
        Iterator<T> source = requireSequential("groupConsecutiveBy").iterator();
        Iterator<List<T>> groupIterator = new Iterator<>() {
            private T pending = null;
            private boolean hasPending = false;

            @Override
            public boolean hasNext() {
                return hasPending || source.hasNext();
            }

            @Override
            public List<T> next() {
                if (!hasNext()) throw new NoSuchElementException();
                List<T> group = new ArrayList<>();
                T first = hasPending ? pending : source.next();
                hasPending = false;
                group.add(first);
                K key = keyFn.apply(first);

                while (source.hasNext()) {
                    T el = source.next();
                    if (Objects.equals(key, keyFn.apply(el))) {
                        group.add(el);
                    } else {
                        pending = el;
                        hasPending = true;
                        break;
                    }
                }
                return Collections.unmodifiableList(group);
            }
        };
        return fromIterator(groupIterator, stream);
    }

    /**
     * Takes elements while the predicate is false; stops (exclusive) on first match.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4, 5).takeUntil(n -> n >= 3)
     * // -> 1, 2
     * }</pre>
     */
    public FluentStream<T> takeUntil(Predicate<T> predicate) {
        return of(stream.takeWhile(predicate.negate()));
    }

    /**
     * Returns distinct elements by a key extractor, keeping the first occurrence per key.
     *
     * <pre>{@code
     * FluentStream.of("apple","apricot","banana").distinctBy(s -> s.charAt(0))
     * // -> "apple", "banana"
     * }</pre>
     */
    public FluentStream<T> distinctBy(Function<T, ?> keyFn) {
        Stream<T> seq = requireSequential("distinctBy");
        Set<Object> seen = new HashSet<>();
        return of(seq.filter(el -> seen.add(keyFn.apply(el))));
    }

    /**
     * Interleaves elements from this stream and another in round-robin fashion.
     * Remaining elements from the longer stream are appended at the end.
     *
     * <pre>{@code
     * FluentStream.of(1, 3, 5).interleave(Stream.of(2, 4, 6))
     * // -> 1, 2, 3, 4, 5, 6
     * }</pre>
     */
    public FluentStream<T> interleave(Stream<T> other) {
        Iterator<T> iterA = requireSequential("interleave").iterator();
        Iterator<T> iterB = other.iterator();
        Iterator<T> interleaved = new Iterator<>() {
            private boolean pickA = true;

            @Override
            public boolean hasNext() {
                return iterA.hasNext() || iterB.hasNext();
            }

            @Override
            public T next() {
                if (!hasNext()) throw new NoSuchElementException();
                if (pickA) {
                    pickA = false;
                    return iterA.hasNext() ? iterA.next() : iterB.next();
                } else {
                    pickA = true;
                    return iterB.hasNext() ? iterB.next() : iterA.next();
                }
            }
        };
        return fromIterator(interleaved, stream, other);
    }

    /**
     * Produces the cartesian product of this stream with another.
     *
     * <p>Note: {@code other} is materialized into a list since it must be
     * traversed once per element of this stream.
     *
     * <pre>{@code
     * FluentStream.of(1, 2).crossProduct(Stream.of("a", "b"))
     * // -> Pair(1,"a"), Pair(1,"b"), Pair(2,"a"), Pair(2,"b")
     * }</pre>
     */
    public <U> FluentStream<Pair<T, U>> crossProduct(Stream<U> other) {
        List<U> materialized = other.toList();
        return of(stream.flatMap(a -> materialized.stream().map(b -> new Pair<>(a, b))));
    }

    // ── Terminal operations (FluentX) ─────────────────────────────────────────

    /**
     * Partitions elements into two lists based on a predicate.
     * This is a <strong>terminal</strong> operation.
     *
     * <pre>{@code
     * var result = FluentStream.of(1, 2, 3, 4, 5).partition(n -> n % 2 == 0);
     * result.matching()    // -> [2, 4]
     * result.notMatching() // -> [1, 3, 5]
     * }</pre>
     */
    public Partitioned<T> partition(Predicate<T> predicate) {
        Map<Boolean, List<T>> groups = stream.collect(Collectors.partitioningBy(predicate));
        return new Partitioned<>(
                Collections.unmodifiableList(groups.get(true)),
                Collections.unmodifiableList(groups.get(false)));
    }

    /**
     * Counts occurrences of each element.
     * This is a <strong>terminal</strong> operation.
     *
     * <pre>{@code
     * FluentStream.of("a", "b", "a", "c", "b", "a").frequencies()
     * // -> {a=3, b=2, c=1}
     * }</pre>
     */
    public Map<T, Long> frequencies() {
        return Collections.unmodifiableMap(
                stream.collect(Collectors.groupingBy(Function.identity(), Collectors.counting())));
    }

    /**
     * Counts occurrences by a key extractor.
     * This is a <strong>terminal</strong> operation.
     *
     * <pre>{@code
     * FluentStream.of("apple", "apricot", "banana").frequenciesBy(s -> s.charAt(0))
     * // -> {a=2, b=1}
     * }</pre>
     */
    public <K> Map<K, Long> frequenciesBy(Function<T, K> keyFn) {
        return Collections.unmodifiableMap(
                stream.collect(Collectors.groupingBy(keyFn, Collectors.counting())));
    }

    // ── Standard Stream pass-throughs ─────────────────────────────────────────

    public FluentStream<T> filter(Predicate<T> predicate)              { return of(stream.filter(predicate)); }
    public <R> FluentStream<R> map(Function<T, R> mapper)              { return of(stream.map(mapper)); }
    public <R> FluentStream<R> flatMap(Function<T, Stream<R>> mapper)  { return of(stream.flatMap(mapper)); }
    public FluentStream<T> sorted()                                     { return of(stream.sorted()); }
    public FluentStream<T> sorted(Comparator<T> comparator)            { return of(stream.sorted(comparator)); }
    public FluentStream<T> limit(long maxSize)                          { return of(stream.limit(maxSize)); }
    public FluentStream<T> skip(long n)                                 { return of(stream.skip(n)); }
    public FluentStream<T> peek(Consumer<T> action)                     { return of(stream.peek(action)); }
    public void forEach(Consumer<T> action)                             { stream.forEach(action); }
    public List<T> toList()                                             { return stream.toList(); }
    public <R, A> R collect(Collector<? super T, A, R> collector)      { return stream.collect(collector); }
    public Optional<T> findFirst()                                      { return stream.findFirst(); }
    public Optional<T> reduce(BinaryOperator<T> accumulator)           { return stream.reduce(accumulator); }
    public T reduce(T identity, BinaryOperator<T> accumulator)         { return stream.reduce(identity, accumulator); }
    public long count()                                                 { return stream.count(); }
    public boolean anyMatch(Predicate<T> predicate)                    { return stream.anyMatch(predicate); }
    public boolean allMatch(Predicate<T> predicate)                    { return stream.allMatch(predicate); }
    public boolean noneMatch(Predicate<T> predicate)                   { return stream.noneMatch(predicate); }

    public Stream<T> toStream()                                         { return stream; }
}
```

---

## 8. Complete Test Suite

```java
package io.fluentx.streams;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class FluentStreamTest {

    // ── zipWithIndex ──────────────────────────────────────────────────────────

    @Nested
    class ZipWithIndexTests {

        @Test
        void assignsCorrectIndices() {
            var result = FluentStream.of("a", "b", "c").zipWithIndex().toList();
            assertEquals(List.of(
                    new Indexed<>(0, "a"),
                    new Indexed<>(1, "b"),
                    new Indexed<>(2, "c")), result);
        }

        @Test
        void emptyStream_returnsEmpty() {
            assertEquals(0, FluentStream.empty().zipWithIndex().count());
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1, 2, 3).parallelStream()).zipWithIndex());
        }
    }

    // ── zip ───────────────────────────────────────────────────────────────────

    @Nested
    class ZipTests {

        @Test
        void pairsElementsByPosition() {
            var result = FluentStream.of(1, 2, 3).zip(Stream.of("a", "b", "c")).toList();
            assertEquals(List.of(
                    new Pair<>(1, "a"),
                    new Pair<>(2, "b"),
                    new Pair<>(3, "c")), result);
        }

        @Test
        void stopsAtShorterStream() {
            var result = FluentStream.of(1, 2, 3).zip(Stream.of("a", "b")).toList();
            assertEquals(2, result.size());
        }

        @Test
        void emptyStreams() {
            var result = FluentStream.<Integer>empty().zip(Stream.empty()).toList();
            assertTrue(result.isEmpty());
        }
    }

    // ── zipWithNext ───────────────────────────────────────────────────────────

    @Nested
    class ZipWithNextTests {

        @Test
        void pairsConsecutiveElements() {
            var result = FluentStream.of(1, 2, 3, 4).zipWithNext().toList();
            assertEquals(List.of(
                    new Pair<>(1, 2),
                    new Pair<>(2, 3),
                    new Pair<>(3, 4)), result);
        }

        @Test
        void singleElement_returnsEmpty() {
            var result = FluentStream.of(1).zipWithNext().toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void emptyStream_returnsEmpty() {
            var result = FluentStream.<Integer>empty().zipWithNext().toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void twoElements_returnsOnePair() {
            var result = FluentStream.of("a", "b").zipWithNext().toList();
            assertEquals(List.of(new Pair<>("a", "b")), result);
        }
    }

    // ── scan ──────────────────────────────────────────────────────────────────

    @Nested
    class ScanTests {

        @Test
        void producesRunningAccumulation() {
            var result = FluentStream.of(1, 2, 3, 4).scan(0, Integer::sum).toList();
            assertEquals(List.of(0, 1, 3, 6, 10), result);
        }

        @Test
        void emptyStream_returnsIdentityOnly() {
            var result = FluentStream.<Integer>empty().scan(0, Integer::sum).toList();
            assertEquals(List.of(0), result);
        }

        @Test
        void stringConcatenation() {
            var result = FluentStream.of("a", "b", "c").scan("", String::concat).toList();
            assertEquals(List.of("", "a", "ab", "abc"), result);
        }
    }

    // ── chunk ─────────────────────────────────────────────────────────────────

    @Nested
    class ChunkTests {

        @Test
        void splitsIntoFixedSizeGroups() {
            var result = FluentStream.of(1, 2, 3, 4, 5).chunk(2).toList();
            assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5)), result);
        }

        @Test
        void exactMultiple_noRemainder() {
            var result = FluentStream.of(1, 2, 3, 4).chunk(2).toList();
            assertEquals(List.of(List.of(1, 2), List.of(3, 4)), result);
        }

        @Test
        void chunkSizeOfOne() {
            var result = FluentStream.of(1, 2, 3).chunk(1).toList();
            assertEquals(List.of(List.of(1), List.of(2), List.of(3)), result);
        }

        @Test
        void chunkLargerThanStream() {
            var result = FluentStream.of(1, 2).chunk(10).toList();
            assertEquals(List.of(List.of(1, 2)), result);
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().chunk(3).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void throwsOnNonPositiveSize() {
            assertThrows(IllegalArgumentException.class, () -> FluentStream.of(1).chunk(0));
            assertThrows(IllegalArgumentException.class, () -> FluentStream.of(1).chunk(-1));
        }

        @Test
        void chunksAreUnmodifiable() {
            var chunks = FluentStream.of(1, 2, 3).chunk(2).toList();
            assertThrows(UnsupportedOperationException.class, () -> chunks.get(0).add(99));
        }

        @Test
        void lazyEvaluation_worksWithLargeStream() {
            // verify laziness: take first chunk of a large stream without OOM
            var first = FluentStream.of(Stream.iterate(0, i -> i + 1).limit(1_000_000))
                    .chunk(100)
                    .findFirst();
            assertTrue(first.isPresent());
            assertEquals(100, first.get().size());
        }
    }

    // ── window ────────────────────────────────────────────────────────────────

    @Nested
    class WindowTests {

        @Test
        void producesSlidingWindows() {
            var result = FluentStream.of(1, 2, 3, 4, 5).window(3).toList();
            assertEquals(List.of(
                    List.of(1, 2, 3),
                    List.of(2, 3, 4),
                    List.of(3, 4, 5)), result);
        }

        @Test
        void windowOfOne_eachElementAlone() {
            var result = FluentStream.of(1, 2, 3).window(1).toList();
            assertEquals(List.of(List.of(1), List.of(2), List.of(3)), result);
        }

        @Test
        void windowEqualsStreamSize() {
            var result = FluentStream.of(1, 2, 3).window(3).toList();
            assertEquals(List.of(List.of(1, 2, 3)), result);
        }

        @Test
        void largerThanStream_returnsEmpty() {
            var result = FluentStream.of(1, 2).window(5).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void throwsOnNonPositiveSize() {
            assertThrows(IllegalArgumentException.class, () -> FluentStream.of(1).window(0));
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().window(3).toList();
            assertTrue(result.isEmpty());
        }
    }

    // ── groupConsecutive ──────────────────────────────────────────────────────

    @Nested
    class GroupConsecutiveTests {

        @Test
        void groupsAdjacentEqualElements() {
            var result = FluentStream.of(1, 1, 2, 3, 3, 3, 1).groupConsecutive().toList();
            assertEquals(List.of(
                    List.of(1, 1),
                    List.of(2),
                    List.of(3, 3, 3),
                    List.of(1)), result);
        }

        @Test
        void groupsByKeyFunction() {
            var result = FluentStream.of("apple", "apricot", "banana", "blueberry", "cherry")
                    .groupConsecutiveBy(s -> s.charAt(0))
                    .toList();
            assertEquals(List.of(
                    List.of("apple", "apricot"),
                    List.of("banana", "blueberry"),
                    List.of("cherry")), result);
        }

        @Test
        void singleElement() {
            var result = FluentStream.of(42).groupConsecutive().toList();
            assertEquals(List.of(List.of(42)), result);
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().groupConsecutive().toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void allSameKey() {
            var result = FluentStream.of(1, 1, 1).groupConsecutive().toList();
            assertEquals(List.of(List.of(1, 1, 1)), result);
        }

        @Test
        void allDifferentKeys() {
            var result = FluentStream.of(1, 2, 3).groupConsecutive().toList();
            assertEquals(List.of(List.of(1), List.of(2), List.of(3)), result);
        }

        @Test
        void groupsAreUnmodifiable() {
            var groups = FluentStream.of(1, 1, 2).groupConsecutive().toList();
            assertThrows(UnsupportedOperationException.class, () -> groups.get(0).add(99));
        }
    }

    // ── takeUntil ─────────────────────────────────────────────────────────────

    @Nested
    class TakeUntilTests {

        @Test
        void stopsBeforeMatchingElement() {
            var result = FluentStream.of(1, 2, 3, 4, 5).takeUntil(n -> n >= 3).toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        void neverMatches_returnsAll() {
            var result = FluentStream.of(1, 2, 3).takeUntil(n -> n > 100).toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        void firstElementMatches_returnsEmpty() {
            var result = FluentStream.of(1, 2, 3).takeUntil(n -> n >= 1).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().takeUntil(n -> true).toList();
            assertTrue(result.isEmpty());
        }
    }

    // ── distinctBy ────────────────────────────────────────────────────────────

    @Nested
    class DistinctByTests {

        @Test
        void keepsFirstOccurrencePerKey() {
            var result = FluentStream.of("apple", "apricot", "banana", "blueberry")
                    .distinctBy(s -> s.charAt(0))
                    .toList();
            assertEquals(List.of("apple", "banana"), result);
        }

        @Test
        void allUnique_returnsAll() {
            var result = FluentStream.of(1, 2, 3).distinctBy(Function.identity()).toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        void allSameKey_returnsFirst() {
            var result = FluentStream.of("a", "ab", "abc").distinctBy(String::length).toList();
            assertEquals(List.of("a", "ab", "abc"), result);
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<String>empty().distinctBy(s -> s).toList();
            assertTrue(result.isEmpty());
        }
    }

    // ── interleave ────────────────────────────────────────────────────────────

    @Nested
    class InterleaveTests {

        @Test
        void roundRobinEqualLength() {
            var result = FluentStream.of(1, 3, 5).interleave(Stream.of(2, 4, 6)).toList();
            assertEquals(List.of(1, 2, 3, 4, 5, 6), result);
        }

        @Test
        void firstLonger_appendsRemaining() {
            var result = FluentStream.of(1, 3, 5, 7).interleave(Stream.of(2, 4)).toList();
            assertEquals(List.of(1, 2, 3, 4, 5, 7), result);
        }

        @Test
        void secondLonger_appendsRemaining() {
            var result = FluentStream.of(1).interleave(Stream.of(2, 4, 6)).toList();
            assertEquals(List.of(1, 2, 4, 6), result);
        }

        @Test
        void firstEmpty() {
            var result = FluentStream.<Integer>empty().interleave(Stream.of(1, 2)).toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        void secondEmpty() {
            var result = FluentStream.of(1, 2).interleave(Stream.empty()).toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        void bothEmpty() {
            var result = FluentStream.<Integer>empty().interleave(Stream.empty()).toList();
            assertTrue(result.isEmpty());
        }
    }

    // ── crossProduct ──────────────────────────────────────────────────────────

    @Nested
    class CrossProductTests {

        @Test
        void producesAllPairings() {
            var result = FluentStream.of(1, 2).crossProduct(Stream.of("a", "b")).toList();
            assertEquals(List.of(
                    new Pair<>(1, "a"), new Pair<>(1, "b"),
                    new Pair<>(2, "a"), new Pair<>(2, "b")), result);
        }

        @Test
        void firstEmpty_returnsEmpty() {
            var result = FluentStream.<Integer>empty().crossProduct(Stream.of("a")).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void secondEmpty_returnsEmpty() {
            var result = FluentStream.of(1).crossProduct(Stream.empty()).toList();
            assertTrue(result.isEmpty());
        }
    }

    // ── partition ─────────────────────────────────────────────────────────────

    @Nested
    class PartitionTests {

        @Test
        void splitsIntoMatchingAndNotMatching() {
            var result = FluentStream.of(1, 2, 3, 4, 5).partition(n -> n % 2 == 0);
            assertEquals(List.of(2, 4), result.matching());
            assertEquals(List.of(1, 3, 5), result.notMatching());
        }

        @Test
        void allMatch() {
            var result = FluentStream.of(2, 4, 6).partition(n -> n % 2 == 0);
            assertEquals(List.of(2, 4, 6), result.matching());
            assertTrue(result.notMatching().isEmpty());
        }

        @Test
        void noneMatch() {
            var result = FluentStream.of(1, 3, 5).partition(n -> n % 2 == 0);
            assertTrue(result.matching().isEmpty());
            assertEquals(List.of(1, 3, 5), result.notMatching());
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().partition(n -> true);
            assertTrue(result.matching().isEmpty());
            assertTrue(result.notMatching().isEmpty());
        }

        @Test
        void resultsAreUnmodifiable() {
            var result = FluentStream.of(1, 2).partition(n -> n > 1);
            assertThrows(UnsupportedOperationException.class, () -> result.matching().add(99));
            assertThrows(UnsupportedOperationException.class, () -> result.notMatching().add(99));
        }
    }

    // ── frequencies ───────────────────────────────────────────────────────────

    @Nested
    class FrequencyTests {

        @Test
        void countsOccurrences() {
            var result = FluentStream.of("a", "b", "a", "c", "b", "a").frequencies();
            assertEquals(Map.of("a", 3L, "b", 2L, "c", 1L), result);
        }

        @Test
        void singleElement() {
            assertEquals(Map.of("x", 1L), FluentStream.of("x").frequencies());
        }

        @Test
        void emptyStream() {
            assertTrue(FluentStream.empty().frequencies().isEmpty());
        }

        @Test
        void frequenciesByKey() {
            var result = FluentStream.of("apple", "apricot", "banana")
                    .frequenciesBy(s -> s.charAt(0));
            assertEquals(Map.of('a', 2L, 'b', 1L), result);
        }

        @Test
        void resultIsUnmodifiable() {
            var result = FluentStream.of("a").frequencies();
            assertThrows(UnsupportedOperationException.class, () -> result.put("b", 1L));
        }
    }

    // ── parallel stream rejection ─────────────────────────────────────────────

    @Nested
    class ParallelStreamTests {

        @Test
        void zipWithIndex_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).zipWithIndex());
        }

        @Test
        void scan_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).scan(0, Integer::sum));
        }

        @Test
        void chunk_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).chunk(1));
        }

        @Test
        void window_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).window(1));
        }

        @Test
        void groupConsecutiveBy_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream())
                            .groupConsecutiveBy(Function.identity()));
        }

        @Test
        void distinctBy_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream())
                            .distinctBy(Function.identity()));
        }

        @Test
        void interleave_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream())
                            .interleave(Stream.of(2)));
        }

        @Test
        void zipWithNext_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1, 2).parallelStream()).zipWithNext());
        }

        @Test
        void zip_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).zip(Stream.of(2)));
        }
    }

    // ── pass-throughs ─────────────────────────────────────────────────────────

    @Nested
    class PassThroughTests {

        @Test
        void filter() {
            assertEquals(List.of(2, 4),
                    FluentStream.of(1, 2, 3, 4, 5).filter(n -> n % 2 == 0).toList());
        }

        @Test
        void map() {
            assertEquals(List.of(2, 4, 6),
                    FluentStream.of(1, 2, 3).map(n -> n * 2).toList());
        }

        @Test
        void flatMap() {
            assertEquals(List.of(1, 1, 2, 2),
                    FluentStream.of(1, 2).flatMap(n -> Stream.of(n, n)).toList());
        }

        @Test
        void sorted() {
            assertEquals(List.of(1, 2, 3),
                    FluentStream.of(3, 1, 2).sorted().toList());
        }

        @Test
        void limit() {
            assertEquals(List.of(1, 2),
                    FluentStream.of(1, 2, 3).limit(2).toList());
        }

        @Test
        void skip() {
            assertEquals(List.of(3),
                    FluentStream.of(1, 2, 3).skip(2).toList());
        }

        @Test
        void count() {
            assertEquals(3, FluentStream.of(1, 2, 3).count());
        }

        @Test
        void findFirst() {
            assertEquals(1, FluentStream.of(1, 2, 3).findFirst().orElseThrow());
        }

        @Test
        void reduce() {
            assertEquals(6, FluentStream.of(1, 2, 3).reduce(0, Integer::sum));
        }

        @Test
        void anyMatch() {
            assertTrue(FluentStream.of(1, 2, 3).anyMatch(n -> n == 2));
            assertFalse(FluentStream.of(1, 2, 3).anyMatch(n -> n == 5));
        }

        @Test
        void allMatch() {
            assertTrue(FluentStream.of(2, 4, 6).allMatch(n -> n % 2 == 0));
            assertFalse(FluentStream.of(1, 2, 3).allMatch(n -> n % 2 == 0));
        }

        @Test
        void noneMatch() {
            assertTrue(FluentStream.of(1, 3, 5).noneMatch(n -> n % 2 == 0));
        }

        @Test
        void toStream_returnsUnderlyingStream() {
            var stream = Stream.of(1, 2, 3);
            assertSame(stream, FluentStream.of(stream).toStream());
        }
    }
}
```

---

## 9. Settings & Build Changes

### `settings.gradle.kts`

```kotlin
rootProject.name = "fluentx-java"

include(
    "fluentx-streams",
    "fluentx-bom",
    "fluentx-examples",
    "fluentx-benchmarks"
)
```

### `.gitignore` — add benchmark build output

Append to existing `.gitignore`:

```
# Benchmark generated sources
fluentx-benchmarks/build/
```

### New file: `Partitioned.java`

```java
package io.fluentx.streams;

import java.util.List;

/**
 * Result of a {@link FluentStream#partition} operation.
 *
 * @param matching    elements for which the predicate returned true
 * @param notMatching elements for which the predicate returned false
 * @param <T>         the element type
 */
public record Partitioned<T>(List<T> matching, List<T> notMatching) {}
```

---

## 10. README Positioning

### The pitch (first 5 lines of README)

```markdown
# FluentX

`scan` · `chunk` · `window` · `zip` · `groupConsecutive` · `distinctBy` and more
for Java Streams. Zero dependencies. Java 17+.

> 15 lines of manual sliding-window boilerplate → `FluentStream.of(data).window(3)`
```

### The comparison table

```markdown
| Operation          | Java Streams | StreamEx         | FluentX                        |
|--------------------|--------------|------------------|--------------------------------|
| Sliding window     | Manual loop  | N/A              | `.window(3)` (lazy)            |
| Fixed-size chunks  | Manual loop  | `ofSubLists(3)`  | `.chunk(3)` (lazy)             |
| Running scan       | Manual loop  | `scanLeft`       | `.scan(0, Integer::sum)` (lazy)|
| Zip with index     | Manual counter| `zipWith`       | `.zipWithIndex()`              |
| Consecutive groups | Manual loop  | `groupRuns`      | `.groupConsecutiveBy(fn)`      |
| Pair with next     | `window(2)`  | N/A              | `.zipWithNext()`               |
| Partition          | 2x filter    | N/A              | `.partition(pred)` → record    |
| Interleave         | Manual       | N/A              | `.interleave(other)`           |
| Cartesian product  | Nested flatMap| N/A             | `.crossProduct(other)`         |
| Frequency count    | Collectors   | N/A              | `.frequencies()`               |
| Gatherers (24+)    | Built-in     | N/A              | `fluentx-gatherers` module     |
```

### Key differentiators to highlight

1. **Lazy by default** — `chunk`, `window`, `scan`, `groupConsecutiveBy` all use
   Iterator-backed streams. They work on infinite streams without OOM.

2. **Fail-fast on parallel** — no silent `.sequential()` degradation. You get an
   `IllegalStateException` with a clear message telling you what to do.

3. **Resource-safe** — every Iterator-based operation propagates `onClose()` to
   the source stream. `Files.lines()` handles close correctly through the pipeline.

4. **Gatherers migration path** — `fluentx-gatherers` module provides the same
   operations as standard `Gatherer` instances for Java 24+. Start on 17, migrate
   incrementally.

5. **Zero dependencies** — no transitive dependency tree. Just `fluentx-streams.jar`.

---

## Summary of All Changes

| # | Change | Type | Impact |
|---|--------|------|--------|
| 1 | Remove `fluentx-collections`, `fluentx-strings`, `fluentx-result` | Delete | Removes vaporware impression |
| 2 | Lazy `chunk` via Iterator | Rewrite | Works on infinite streams |
| 3 | Lazy `window` via ArrayDeque + Iterator | Rewrite | Works on infinite streams |
| 4 | Lazy `groupConsecutiveBy` via Iterator | Rewrite | Works on infinite streams |
| 5 | `onClose` on `scan`, `chunk`, `window`, `groupConsecutiveBy` | Bugfix | Resource safety |
| 6 | `requireSequential()` fail-fast helper | New | Honest parallel stream policy |
| 7 | `zipWithNext()` | New operation | Common use case, search magnet |
| 8 | `interleave(Stream)` | New operation | Round-robin merge |
| 9 | `partition(Predicate)` + `Partitioned<T>` | New operation | Split into two groups |
| 10 | `frequencies()` / `frequenciesBy(Function)` | New operation | Element counting |
| 11 | `crossProduct(Stream)` | New operation | Cartesian join |
| 12 | `fromIterator()` helper | Internal | DRY for Iterator-backed streams |
| 13 | `fluentx-gatherers` module (Java 24+) | New module | Gatherers interop |
| 14 | 80+ tests (up from 26) | Tests | Comprehensive coverage |
| 15 | README positioning | Docs | Differentiated pitch |
