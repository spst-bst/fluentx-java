package io.fluentx.examples;

import io.fluentx.streams.FluentStream;

/**
 * Examples for {@code FluentStream.takeUntil(Predicate)}.
 *
 * <p>Use case: read elements from a stream up to (but not including)
 * a sentinel value or boundary condition — cleaner than combining
 * {@code takeWhile} with a negated predicate manually.
 */
public class TakeUntilExample {

    public static void main(String[] args) {

        // ── Basic usage ───────────────────────────────────────────────────────
        System.out.println("=== takeUntil: stop before first value >= 5 ===");

        FluentStream.of(1, 2, 3, 4, 5, 6, 7)
                .takeUntil(n -> n >= 5)
                .forEach(System.out::println);
        // 1  2  3  4

        // ── Real-world: read lines until a separator ──────────────────────────
        System.out.println("\n=== takeUntil: read until blank line ===");

        FluentStream.of("Name: Alice", "Age: 30", "City: NYC", "", "Name: Bob")
                .takeUntil(String::isBlank)
                .forEach(System.out::println);
        // Name: Alice
        // Age: 30
        // City: NYC

        // ── Real-world: process queue until an error ──────────────────────────
        System.out.println("\n=== takeUntil: process until error status ===");

        record Task(String name, String status) {}

        FluentStream.of(
                new Task("Task A", "SUCCESS"),
                new Task("Task B", "SUCCESS"),
                new Task("Task C", "ERROR"),
                new Task("Task D", "SUCCESS")
        )
                .takeUntil(t -> t.status().equals("ERROR"))
                .forEach(t -> System.out.println("Processed: " + t.name()));
        // Processed: Task A
        // Processed: Task B
    }
}
