package io.fluentx.examples;

import io.fluentx.streams.FluentStream;

import java.util.stream.Stream;

/**
 * Examples for {@code FluentStream.zip(Stream other)}.
 *
 * <p>Use case: combine two parallel streams element-by-element into pairs.
 * Stops at the shorter stream — no IndexOutOfBoundsException.
 */
public class ZipExample {

    public static void main(String[] args) {

        // ── Basic usage ───────────────────────────────────────────────────────
        System.out.println("=== zip: basic ===");

        var names  = Stream.of("Alice", "Bob", "Charlie");
        var scores = Stream.of(95, 82, 78);

        FluentStream.of(names)
                .zip(scores)
                .forEach(p -> System.out.printf("%s scored %d%n", p.first(), p.second()));
        // Alice scored 95
        // Bob scored 82
        // Charlie scored 78

        // ── Stops at shorter stream ───────────────────────────────────────────
        System.out.println("\n=== zip: unequal lengths ===");

        FluentStream.of("x", "y", "z")
                .zip(Stream.of(1, 2))
                .forEach(p -> System.out.println(p.first() + " → " + p.second()));
        // x → 1
        // y → 2   (z is dropped — no error)

        // ── Real-world: merge two data sources by row ────────────────────────
        System.out.println("\n=== zip: merge CSV columns ===");

        var cities      = Stream.of("New York", "London", "Tokyo");
        var populations = Stream.of("8.3M", "9.0M", "13.9M");

        FluentStream.of(cities)
                .zip(populations)
                .map(p -> String.format("%-12s population: %s", p.first(), p.second()))
                .forEach(System.out::println);
        // New York     population: 8.3M
        // London       population: 9.0M
        // Tokyo        population: 13.9M
    }
}
