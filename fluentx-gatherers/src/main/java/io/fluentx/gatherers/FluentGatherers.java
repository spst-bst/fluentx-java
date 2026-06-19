package io.fluentx.gatherers;

import io.fluentx.streams.Indexed;
import io.fluentx.streams.Pair;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Gatherer;

/**
 * The FluentX stream operations exposed as standard {@link Gatherer} instances for
 * use with {@code Stream.gather(...)}.
 *
 * <p>Where {@link io.fluentx.streams.FluentStream} wraps a stream in a fluent facade,
 * this class lets the same operations compose directly into a native stream pipeline:
 *
 * <pre>{@code
 * import static io.fluentx.gatherers.FluentGatherers.*;
 *
 * List<List<Integer>> windows = Stream.of(1, 2, 3, 4, 5)
 *         .gather(window(3))
 *         .toList();
 * // [1,2,3], [2,3,4], [3,4,5]
 * }</pre>
 *
 * <p><strong>Runtime requirement:</strong> {@link Gatherer} and {@code Stream.gather}
 * are stable from JDK 24 (JEP 485). This module is compiled against JDK 24 and requires
 * a Java 24+ runtime — no preview flags. The rest of FluentX remains Java 17 compatible.
 *
 * <p>All gatherers here are <strong>sequential</strong> and stateful, mirroring the
 * semantics of their {@code FluentStream} counterparts. Output element types reuse
 * {@link Pair} and {@link Indexed} from {@code fluentx-streams} for consistency.
 */
public final class FluentGatherers {

    private FluentGatherers() {}

    /**
     * Splits elements into fixed-size chunks (the final chunk may be smaller).
     *
     * <pre>{@code
     * Stream.of(1, 2, 3, 4, 5).gather(chunk(2))
     * // -> [1,2], [3,4], [5]
     * }</pre>
     *
     * @param <T>  the element type
     * @param size the maximum chunk size; must be positive
     * @return a chunking gatherer producing unmodifiable lists
     * @throws IllegalArgumentException if {@code size} is not positive
     */
    public static <T> Gatherer<T, ?, List<T>> chunk(int size) {
        if (size <= 0) throw new IllegalArgumentException("chunk size must be positive, got: " + size);
        return Gatherer.ofSequential(
                () -> new ArrayList<T>(size),
                Gatherer.Integrator.ofGreedy((buffer, element, downstream) -> {
                    buffer.add(element);
                    if (buffer.size() == size) {
                        List<T> out = List.copyOf(buffer);
                        buffer.clear();
                        return downstream.push(out);
                    }
                    return true;
                }),
                (buffer, downstream) -> {
                    if (!buffer.isEmpty()) {
                        downstream.push(List.copyOf(buffer));
                    }
                }
        );
    }

    /**
     * Produces sliding windows of the given width over the elements.
     *
     * <pre>{@code
     * Stream.of(1, 2, 3, 4, 5).gather(window(3))
     * // -> [1,2,3], [2,3,4], [3,4,5]
     * }</pre>
     *
     * @param <T>  the element type
     * @param size the window width; must be positive
     * @return a sliding-window gatherer producing unmodifiable lists
     * @throws IllegalArgumentException if {@code size} is not positive
     */
    public static <T> Gatherer<T, ?, List<T>> window(int size) {
        if (size <= 0) throw new IllegalArgumentException("window size must be positive, got: " + size);
        return Gatherer.ofSequential(
                () -> new ArrayDeque<T>(size),
                Gatherer.Integrator.ofGreedy((buffer, element, downstream) -> {
                    buffer.addLast(element);
                    if (buffer.size() == size) {
                        boolean cont = downstream.push(List.copyOf(buffer));
                        buffer.pollFirst();
                        return cont;
                    }
                    return true;
                })
        );
    }

    /**
     * Groups consecutive elements sharing the same key into lists.
     *
     * <pre>{@code
     * Stream.of("apple", "apricot", "banana")
     *       .gather(groupConsecutiveBy(s -> s.charAt(0)))
     * // -> [apple, apricot], [banana]
     * }</pre>
     *
     * @param <T>   the element type
     * @param <K>   the key type
     * @param keyFn the key extractor determining group boundaries
     * @return a grouping gatherer producing unmodifiable lists
     */
    public static <T, K> Gatherer<T, ?, List<T>> groupConsecutiveBy(Function<T, K> keyFn) {
        Objects.requireNonNull(keyFn, "keyFn must not be null");
        return Gatherer.ofSequential(
                GroupState<T, K>::new,
                Gatherer.Integrator.ofGreedy((state, element, downstream) -> {
                    K key = keyFn.apply(element);
                    if (!state.group.isEmpty() && !Objects.equals(key, state.currentKey)) {
                        boolean cont = downstream.push(Collections.unmodifiableList(state.group));
                        state.group = new ArrayList<>();
                        state.currentKey = key;
                        state.group.add(element);
                        return cont;
                    }
                    state.currentKey = key;
                    state.group.add(element);
                    return true;
                }),
                (state, downstream) -> {
                    if (!state.group.isEmpty()) {
                        downstream.push(Collections.unmodifiableList(state.group));
                    }
                }
        );
    }

    /**
     * Groups consecutive equal elements into lists.
     *
     * <pre>{@code
     * Stream.of(1, 1, 2, 3, 3, 1).gather(groupConsecutive())
     * // -> [1,1], [2], [3,3], [1]
     * }</pre>
     *
     * @param <T> the element type
     * @return a grouping gatherer producing unmodifiable lists
     */
    public static <T> Gatherer<T, ?, List<T>> groupConsecutive() {
        return groupConsecutiveBy(Function.identity());
    }

    /**
     * Returns a running prefix scan: emits the identity first, then each successive
     * accumulation. An empty upstream still emits the identity.
     *
     * <pre>{@code
     * Stream.of(1, 2, 3, 4).gather(scan(0, Integer::sum))
     * // -> 0, 1, 3, 6, 10
     * }</pre>
     *
     * @param <T>         the element type
     * @param <R>         the accumulation type
     * @param identity    the initial value, emitted first
     * @param accumulator combines the running result with each element
     * @return a scanning gatherer
     */
    public static <T, R> Gatherer<T, ?, R> scan(R identity, BiFunction<R, T, R> accumulator) {
        Objects.requireNonNull(accumulator, "accumulator must not be null");
        return Gatherer.ofSequential(
                () -> new ScanState<R>(identity),
                Gatherer.Integrator.ofGreedy((state, element, downstream) -> {
                    if (!state.startedEmitted) {
                        state.startedEmitted = true;
                        if (!downstream.push(state.current)) {
                            return false;
                        }
                    }
                    state.current = accumulator.apply(state.current, element);
                    return downstream.push(state.current);
                }),
                (state, downstream) -> {
                    if (!state.startedEmitted) {
                        downstream.push(state.current); // empty upstream -> emit identity
                    }
                }
        );
    }

    /**
     * Pairs each element with its successor, producing {@code n-1} pairs.
     *
     * <pre>{@code
     * Stream.of(1, 2, 3, 4).gather(zipWithNext())
     * // -> Pair(1,2), Pair(2,3), Pair(3,4)
     * }</pre>
     *
     * @param <T> the element type
     * @return a gatherer producing {@link Pair} of consecutive elements
     */
    public static <T> Gatherer<T, ?, Pair<T, T>> zipWithNext() {
        return Gatherer.ofSequential(
                PrevState<T>::new,
                Gatherer.Integrator.ofGreedy((state, element, downstream) -> {
                    boolean cont = true;
                    if (state.hasPrevious) {
                        cont = downstream.push(new Pair<>(state.previous, element));
                    }
                    state.previous = element;
                    state.hasPrevious = true;
                    return cont;
                })
        );
    }

    /**
     * Pairs each element with its zero-based index.
     *
     * <pre>{@code
     * Stream.of("a", "b", "c").gather(zipWithIndex())
     * // -> Indexed(0,"a"), Indexed(1,"b"), Indexed(2,"c")
     * }</pre>
     *
     * @param <T> the element type
     * @return a gatherer producing {@link Indexed} values
     */
    public static <T> Gatherer<T, ?, Indexed<T>> zipWithIndex() {
        return Gatherer.ofSequential(
                () -> new int[]{0},
                Gatherer.Integrator.ofGreedy((index, element, downstream) ->
                        downstream.push(new Indexed<>(index[0]++, element)))
        );
    }

    /**
     * Deduplicates elements by a key extractor, keeping the first occurrence per key.
     *
     * <pre>{@code
     * Stream.of("apple", "apricot", "banana").gather(distinctBy(s -> s.charAt(0)))
     * // -> apple, banana
     * }</pre>
     *
     * @param <T>   the element type
     * @param <K>   the key type
     * @param keyFn the key extractor used for deduplication
     * @return a deduplicating gatherer
     */
    public static <T, K> Gatherer<T, ?, T> distinctBy(Function<T, K> keyFn) {
        Objects.requireNonNull(keyFn, "keyFn must not be null");
        return Gatherer.ofSequential(
                (java.util.function.Supplier<Set<K>>) HashSet::new,
                Gatherer.Integrator.ofGreedy((seen, element, downstream) -> {
                    if (seen.add(keyFn.apply(element))) {
                        return downstream.push(element);
                    }
                    return true;
                })
        );
    }

    /**
     * Emits elements until the predicate first holds (exclusive), then stops.
     *
     * <pre>{@code
     * Stream.of(1, 2, 3, 4, 5).gather(takeUntil(n -> n >= 3))
     * // -> 1, 2
     * }</pre>
     *
     * @param <T>       the element type
     * @param predicate the stop condition; the first matching element is excluded
     * @return a short-circuiting gatherer
     */
    public static <T> Gatherer<T, ?, T> takeUntil(java.util.function.Predicate<T> predicate) {
        Objects.requireNonNull(predicate, "predicate must not be null");
        return Gatherer.ofSequential(
                () -> (Void) null,
                (state, element, downstream) -> {
                    if (predicate.test(element)) {
                        return false; // stop, excluding this element
                    }
                    return downstream.push(element);
                }
        );
    }

    // ── Mutable gatherer state ────────────────────────────────────────────────

    private static final class GroupState<T, K> {
        K currentKey;
        List<T> group = new ArrayList<>();
    }

    private static final class ScanState<R> {
        R current;
        boolean startedEmitted = false;
        ScanState(R identity) { this.current = identity; }
    }

    private static final class PrevState<T> {
        T previous;
        boolean hasPrevious = false;
    }
}
