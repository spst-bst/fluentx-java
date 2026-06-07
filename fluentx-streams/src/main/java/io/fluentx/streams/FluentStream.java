package io.fluentx.streams;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * A fluent wrapper around Java's {@link Stream} that adds commonly needed
 * utility operations missing from the standard library.
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
        int[] index = {0};
        return of(stream.map(value -> new Indexed<>(index[0]++, value)));
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
        Iterator<T> iterA = stream.iterator();
        Iterator<U> iterB = other.iterator();
        Iterable<Pair<T, U>> iterable = () -> new Iterator<>() {
            @Override public boolean hasNext() { return iterA.hasNext() && iterB.hasNext(); }
            @Override public Pair<T, U> next()  { return new Pair<>(iterA.next(), iterB.next()); }
        };
        return of(StreamSupport.stream(iterable.spliterator(), false));
    }

    /**
     * Returns a running prefix scan (all intermediate accumulation results).
     * Unlike {@code reduce}, emits every intermediate value including the identity.
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
        List<R> results = new ArrayList<>();
        results.add(identity);
        stream.forEach(el -> results.add(accumulator.apply(results.get(results.size() - 1), el)));
        return of(results.stream());
    }

    /**
     * Splits the stream into fixed-size chunks (last chunk may be smaller).
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4, 5).chunk(2)
     * // -> [1,2], [3,4], [5]
     * }</pre>
     *
     * @param size the maximum size of each chunk; must be positive
     * @return a stream of lists, each of at most {@code size} elements
     * @throws IllegalArgumentException if size is not positive
     */
    public FluentStream<List<T>> chunk(int size) {
        if (size <= 0) throw new IllegalArgumentException("chunk size must be positive, got: " + size);
        List<T> all = stream.collect(Collectors.toList());
        List<List<T>> chunks = new ArrayList<>();
        for (int i = 0; i < all.size(); i += size) {
            chunks.add(List.copyOf(all.subList(i, Math.min(i + size, all.size()))));
        }
        return of(chunks.stream());
    }

    /**
     * Produces a sliding window over stream elements.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4, 5).window(3)
     * // -> [1,2,3], [2,3,4], [3,4,5]
     * }</pre>
     *
     * @param size the width of each window; must be positive
     * @return a stream of overlapping sublists of length {@code size}
     * @throws IllegalArgumentException if size is not positive
     */
    public FluentStream<List<T>> window(int size) {
        if (size <= 0) throw new IllegalArgumentException("window size must be positive, got: " + size);
        List<T> all = stream.collect(Collectors.toList());
        List<List<T>> windows = new ArrayList<>();
        for (int i = 0; i <= all.size() - size; i++) {
            windows.add(List.copyOf(all.subList(i, i + size)));
        }
        return of(windows.stream());
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
     *
     * <pre>{@code
     * FluentStream.of("apple","apricot","banana","blueberry")
     *     .groupConsecutiveBy(s -> s.charAt(0))
     * // -> ["apple","apricot"], ["banana","blueberry"]
     * }</pre>
     *
     * @param <K>   the key type used to determine group boundaries
     * @param keyFn function that extracts the grouping key from each element
     * @return a stream of lists, each containing elements with the same consecutive key
     */
    public <K> FluentStream<List<T>> groupConsecutiveBy(Function<T, K> keyFn) {
        List<List<T>> groups  = new ArrayList<>();
        List<T>       current = new ArrayList<>();
        Object[]      lastKey = {null};
        boolean[]     first   = {true};

        stream.forEach(el -> {
            K key = keyFn.apply(el);
            if (first[0] || !Objects.equals(key, lastKey[0])) {
                if (!current.isEmpty()) { groups.add(List.copyOf(current)); current.clear(); }
                first[0]   = false;
                lastKey[0] = key;
            }
            current.add(el);
        });
        if (!current.isEmpty()) groups.add(List.copyOf(current));
        return of(groups.stream());
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
     */
    public FluentStream<T> distinctBy(Function<T, ?> keyFn) {
        Set<Object> seen = new LinkedHashSet<>();
        return of(stream.filter(el -> seen.add(keyFn.apply(el))));
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
