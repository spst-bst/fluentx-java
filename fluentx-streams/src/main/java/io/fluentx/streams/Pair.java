package io.fluentx.streams;

/**
 * An immutable pair of two values.
 *
 * @param <A> the type of the first value
 * @param <B> the type of the second value
 */
public record Pair<A, B>(A first, B second) {

    /**
     * Factory method for more readable construction.
     * @param <A>    the type of the first value
     * @param <B>    the type of the second value
     * @param first  the first value
     * @param second the second value
     * @return a new Pair containing the given values
     */
    public static <A, B> Pair<A, B> of(A first, B second) {
        return new Pair<>(first, second);
    }
}
