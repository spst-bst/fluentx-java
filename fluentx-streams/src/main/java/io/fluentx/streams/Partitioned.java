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
