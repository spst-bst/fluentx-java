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

    /** Wraps an existing {@link Stream}. */
    public static <T> FluentStream<T> of(Stream<T> stream) {
        return new FluentStream<>(stream);
    }

    /** Creates a FluentStream from varargs elements. */
    @SafeVarargs
    public static <T> FluentStream<T> of(T... elements) {
        return new FluentStream<>(Stream.of(elements));
    }

    /** Creates a FluentStream from any {@link Iterable}. */
    public static <T> FluentStream<T> of(Iterable<T> iterable) {
        return new FluentStream<>(StreamSupport.stream(iterable.spliterator(), false));
    }

    /** Creates an empty FluentStream. */
    public static <T> FluentStream<T> empty() {
        return new FluentStream<>(Stream.empty());
    }

    // ── FluentX extensions ────────────────────────────────────────────────────

    /**
     * Pairs each element with its zero-based index.
     *
     * <pre>{@code
     * FluentStream.of("a", "b", "c").zipWithIndex()
     * // → Indexed(0, "a"), Indexed(1, "b"), Indexed(2, "c")
     * }</pre>
     */
    public Stream<Indexed<T>> zipWithIndex() {
        int[] index = {0};
        return stream.map(value -> new Indexed<>(index[0]++, value));
    }

    /**
     * Zips this stream with another, pairing elements by position.
     * Stops when the shorter stream is exhausted.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3).zip(Stream.of("a", "b", "c"))
     * // → Pair(1,"a"), Pair(2,"b"), Pair(3,"c")
     * }</pre>
     */
    public <U> Stream<Pair<T, U>> zip(Stream<U> other) {
        Iterator<T> iterA = stream.iterator();
        Iterator<U> iterB = other.iterator();
        Iterable<Pair<T, U>> iterable = () -> new Iterator<>() {
            @Override public boolean hasNext() { return iterA.hasNext() && iterB.hasNext(); }
            @Override public Pair<T, U> next()  { return new Pair<>(iterA.next(), iterB.next()); }
        };
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * Returns a running prefix scan (all intermediate accumulation results).
     * Unlike {@code reduce}, emits every intermediate value including the identity.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4).scan(0, Integer::sum)
     * // → 0, 1, 3, 6, 10
     * }</pre>
     */
    public <R> Stream<R> scan(R identity, BiFunction<R, T, R> accumulator) {
        List<R> results = new ArrayList<>();
        results.add(identity);
        stream.forEach(el -> results.add(accumulator.apply(results.get(results.size() - 1), el)));
        return results.stream();
    }

    /**
     * Splits the stream into fixed-size chunks (last chunk may be smaller).
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4, 5).chunk(2)
     * // → [1,2], [3,4], [5]
     * }</pre>
     *
     * @throws IllegalArgumentException if size is not positive
     */
    public Stream<List<T>> chunk(int size) {
        if (size <= 0) throw new IllegalArgumentException("chunk size must be positive, got: " + size);
        List<T> all = stream.collect(Collectors.toList());
        List<List<T>> chunks = new ArrayList<>();
        for (int i = 0; i < all.size(); i += size) {
            chunks.add(List.copyOf(all.subList(i, Math.min(i + size, all.size()))));
        }
        return chunks.stream();
    }

    /**
     * Produces a sliding window over stream elements.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4, 5).window(3)
     * // → [1,2,3], [2,3,4], [3,4,5]
     * }</pre>
     *
     * @throws IllegalArgumentException if size is not positive
     */
    public Stream<List<T>> window(int size) {
        if (size <= 0) throw new IllegalArgumentException("window size must be positive, got: " + size);
        List<T> all = stream.collect(Collectors.toList());
        List<List<T>> windows = new ArrayList<>();
        for (int i = 0; i <= all.size() - size; i++) {
            windows.add(List.copyOf(all.subList(i, i + size)));
        }
        return windows.stream();
    }

    /**
     * Groups consecutive equal elements into lists.
     *
     * <pre>{@code
     * FluentStream.of(1, 1, 2, 3, 3, 1).groupConsecutive()
     * // → [1,1], [2], [3,3], [1]
     * }</pre>
     */
    public Stream<List<T>> groupConsecutive() {
        return groupConsecutiveBy(Function.identity());
    }

    /**
     * Groups consecutive elements sharing the same key into lists.
     *
     * <pre>{@code
     * FluentStream.of("apple","apricot","banana","blueberry")
     *     .groupConsecutiveBy(s -> s.charAt(0))
     * // → ["apple","apricot"], ["banana","blueberry"]
     * }</pre>
     */
    public <K> Stream<List<T>> groupConsecutiveBy(Function<T, K> keyFn) {
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
        return groups.stream();
    }

    /**
     * Takes elements while the predicate is {@code false}; stops (exclusive) on first match.
     *
     * <pre>{@code
     * FluentStream.of(1, 2, 3, 4, 5).takeUntil(n -> n >= 3)
     * // → 1, 2
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
     * // → "apple", "banana"
     * }</pre>
     */
    public FluentStream<T> distinctBy(Function<T, ?> keyFn) {
        Set<Object> seen = new LinkedHashSet<>();
        return of(stream.filter(el -> seen.add(keyFn.apply(el))));
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

    /** Unwraps the underlying {@link Stream} for use with standard Java APIs. */
    public Stream<T> toStream()                                         { return stream; }
}
