package io.fluentx.streams;

/**
 * An element paired with its zero-based position in the stream.
 *
 * @param <T> the type of the value
 */
public record Indexed<T>(int index, T value) {}
