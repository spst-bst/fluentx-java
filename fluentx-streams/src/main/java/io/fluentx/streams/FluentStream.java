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

    /**
     * Wraps an existing {@link Stream}.
     * @param <T>    the element type
     * @param stream the stream to wrap
     * @return a new FluentStream backed by the given stream
     */
    public static <T> FluentStream<T> of(Stream<T> stream) {
        return new FluentStream<>(stream);
    }

    /**
     * Creates a FluentStream from varargs elements.
     * @param <T>      the element type
     * @param elements the elements to stream
     * @return a new FluentStream over the given elements
     */
    @SafeVarargs
    public static <T> FluentStream<T> of(T... elements) {
        return new FluentStream<>(Stream.of(elements));
    }

    /**
     * Creates a FluentStream from any {@link Iterable}.
     * @param <T>      the element type
     * @param iterable the iterable to stream
     * @return a new FluentStream over the given iterable
     */
    public static <T> FluentStream<T> of(Iterable<T> iterable) {
        return new FluentStream<>(StreamSupport.stream(iterable.spliterator(), false));
    }

    /**
     * Creates an empty FluentStream.
     * @param <T> the element type
     * @return an empty FluentStream
     */
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
     *
     * @return a stream of {@link Indexed} values pairing each element with its position
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
     *
     * @param <U>   the element type of the other stream
     * @param other the stream to zip with
     * @return a stream of {@link Pair} values combining elements by position
     */
    public <U> FluentStream<Pair<T, U>> zip(Stream<U> other) {
        Objects.requireNonNull(other, "other must not be null");
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
     *
     * @return a stream of pairs of consecutive elements
     * @implNote This method eagerly pulls the first element from the source at call
     *           time; any upstream side effect for that element runs before the first
     *           terminal pull on the result.
     */
    public FluentStream<Pair<T, T>> zipWithNext() {
        Iterator<T> source = requireSequential("zipWithNext").iterator();
        if (!source.hasNext()) {
            stream.close();
            return empty();
        }
        Iterator<Pair<T, T>> pairIterator = new Iterator<>() {
            private T previous = source.next();

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
     *
     * @param <R>         the result type
     * @param identity    the initial accumulator value (emitted first)
     * @param accumulator function combining the running result with each element
     * @return a stream of all intermediate accumulation results
     */
    public <R> FluentStream<R> scan(R identity, BiFunction<R, T, R> accumulator) {
        Objects.requireNonNull(accumulator, "accumulator must not be null");
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
     * @param size the maximum size of each chunk; must be positive
     * @return a stream of unmodifiable lists, each of at most {@code size} elements
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
     * @param size the width of each window; must be positive
     * @return a stream of overlapping lists of length {@code size}
     * @throws IllegalArgumentException if size is not positive
     * @implNote Unlike {@link #chunk(int)} and {@link #scan}, this method eagerly
     *           pulls the first {@code size} elements from the source at call time to
     *           prime the buffer; any upstream side effects (e.g. {@code peek}) for
     *           those elements run before the first terminal pull on the result.
     */
    public FluentStream<List<T>> window(int size) {
        if (size <= 0) throw new IllegalArgumentException("window size must be positive, got: " + size);
        Iterator<T> source = requireSequential("window").iterator();

        ArrayDeque<T> initialBuffer = new ArrayDeque<>(size);
        while (initialBuffer.size() < size && source.hasNext()) {
            initialBuffer.addLast(source.next());
        }
        if (initialBuffer.size() < size) {
            stream.close();
            return empty();
        }

        Iterator<List<T>> windowIterator = new Iterator<>() {
            private final ArrayDeque<T> buffer = initialBuffer;
            private boolean exhausted = false;
            private boolean first = true;

            @Override
            public boolean hasNext() {
                return !exhausted;
            }

            @Override
            public List<T> next() {
                if (exhausted) throw new NoSuchElementException();
                if (first) {
                    first = false;
                } else {
                    buffer.pollFirst();
                    buffer.addLast(source.next());
                }
                List<T> window = List.copyOf(buffer);
                if (!source.hasNext()) {
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
     *
     * @return a stream of lists, each containing a run of equal consecutive elements
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
     *
     * @param <K>   the key type used to determine group boundaries
     * @param keyFn function that extracts the grouping key from each element
     * @return a stream of unmodifiable lists, each containing elements with the same consecutive key
     */
    public <K> FluentStream<List<T>> groupConsecutiveBy(Function<T, K> keyFn) {
        Objects.requireNonNull(keyFn, "keyFn must not be null");
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
     * Takes elements while the predicate is {@code false}; stops (exclusive) on first match.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4, 5).takeUntil(n -> n >= 3)
     * // -> 1, 2
     * }</pre>
     *
     * @param predicate the stop condition; the first element matching it is excluded
     * @return a stream of elements before the first predicate match
     */
    public FluentStream<T> takeUntil(Predicate<T> predicate) {
        Objects.requireNonNull(predicate, "predicate must not be null");
        return of(stream.takeWhile(predicate.negate()));
    }

    /**
     * Returns distinct elements by a key extractor, keeping the first occurrence per key.
     *
     * <pre>{@code
     * FluentStream.of("apple","apricot","banana").distinctBy(s -> s.charAt(0))
     * // -> "apple", "banana"
     * }</pre>
     *
     * @param keyFn function that extracts the key used for deduplication
     * @return a stream with duplicates (by key) removed, preserving encounter order
     * @implNote Retains every distinct key seen so far in memory. On a very large or
     *           infinite stream with high key cardinality this accumulator grows
     *           unbounded, exactly like {@link Stream#distinct()}.
     */
    public FluentStream<T> distinctBy(Function<T, ?> keyFn) {
        Objects.requireNonNull(keyFn, "keyFn must not be null");
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
     *
     * @param other the stream to interleave with
     * @return a stream alternating elements from both sources
     */
    public FluentStream<T> interleave(Stream<T> other) {
        Objects.requireNonNull(other, "other must not be null");
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
     * Every element of this stream is paired with every element of {@code other}.
     *
     * <p>Note: {@code other} is materialized into a list since it must be
     * traversed once per element of this stream.
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
        Objects.requireNonNull(other, "other must not be null");
        List<U> materialized = other.toList();
        return of(stream.flatMap(a -> materialized.stream().map(b -> new Pair<>(a, b))));
    }

    // ── Terminal operations (FluentX) ─────────────────────────────────────────

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
        Objects.requireNonNull(predicate, "predicate must not be null");
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
     *
     * @return an unmodifiable map from element to occurrence count
     * @throws NullPointerException if the stream contains a {@code null} element
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
     *
     * @param <K>   the key type
     * @param keyFn function that extracts the grouping key
     * @return an unmodifiable map from key to occurrence count
     */
    public <K> Map<K, Long> frequenciesBy(Function<T, K> keyFn) {
        Objects.requireNonNull(keyFn, "keyFn must not be null");
        return Collections.unmodifiableMap(
                stream.collect(Collectors.groupingBy(keyFn, Collectors.counting())));
    }

    // ── Standard Stream pass-throughs ─────────────────────────────────────────

    /** @see java.util.stream.Stream#filter(Predicate) */
    public FluentStream<T> filter(Predicate<T> predicate)              { return of(stream.filter(predicate)); }
    /** @see java.util.stream.Stream#map(Function) */
    public <R> FluentStream<R> map(Function<T, R> mapper)              { return of(stream.map(mapper)); }
    /** @see java.util.stream.Stream#flatMap(Function) */
    public <R> FluentStream<R> flatMap(Function<T, Stream<R>> mapper)  { return of(stream.flatMap(mapper)); }
    /** @see java.util.stream.Stream#sorted() */
    public FluentStream<T> sorted()                                     { return of(stream.sorted()); }
    /** @see java.util.stream.Stream#sorted(Comparator) */
    public FluentStream<T> sorted(Comparator<T> comparator)            { return of(stream.sorted(comparator)); }
    /** @see java.util.stream.Stream#limit(long) */
    public FluentStream<T> limit(long maxSize)                          { return of(stream.limit(maxSize)); }
    /** @see java.util.stream.Stream#skip(long) */
    public FluentStream<T> skip(long n)                                 { return of(stream.skip(n)); }
    /** @see java.util.stream.Stream#peek(Consumer) */
    public FluentStream<T> peek(Consumer<T> action)                     { return of(stream.peek(action)); }
    /** @see java.util.stream.Stream#forEach(Consumer) */
    public void forEach(Consumer<T> action)                             { stream.forEach(action); }
    /** @see java.util.stream.Stream#toList() */
    public List<T> toList()                                             { return stream.toList(); }
    /** @see java.util.stream.Stream#collect(Collector) */
    public <R, A> R collect(Collector<? super T, A, R> collector)      { return stream.collect(collector); }
    /** @see java.util.stream.Stream#findFirst() */
    public Optional<T> findFirst()                                      { return stream.findFirst(); }
    /** @see java.util.stream.Stream#reduce(BinaryOperator) */
    public Optional<T> reduce(BinaryOperator<T> accumulator)           { return stream.reduce(accumulator); }
    /** @see java.util.stream.Stream#reduce(Object, BinaryOperator) */
    public T reduce(T identity, BinaryOperator<T> accumulator)         { return stream.reduce(identity, accumulator); }
    /** @see java.util.stream.Stream#count() */
    public long count()                                                 { return stream.count(); }
    /** @see java.util.stream.Stream#anyMatch(Predicate) */
    public boolean anyMatch(Predicate<T> predicate)                    { return stream.anyMatch(predicate); }
    /** @see java.util.stream.Stream#allMatch(Predicate) */
    public boolean allMatch(Predicate<T> predicate)                    { return stream.allMatch(predicate); }
    /** @see java.util.stream.Stream#noneMatch(Predicate) */
    public boolean noneMatch(Predicate<T> predicate)                   { return stream.noneMatch(predicate); }

    /**
     * Unwraps the underlying {@link Stream} for use with standard Java APIs.
     * @return the underlying {@link Stream}
     */
    public Stream<T> toStream()                                         { return stream; }
}
