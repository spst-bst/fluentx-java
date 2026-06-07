package io.fluentx.examples;

import io.fluentx.streams.FluentStream;

import java.util.List;

/**
 * Examples for {@code FluentStream.chunk(int size)}.
 *
 * <p>Use case: batch processing — split a large list into fixed-size
 * groups for bulk API calls, pagination, or parallel processing.
 */
public class ChunkExample {

    public static void main(String[] args) {

        // ── Basic usage ───────────────────────────────────────────────────────
        System.out.println("=== chunk: basic ===");

        FluentStream.of(1, 2, 3, 4, 5, 6, 7)
                .chunk(3)
                .forEach(System.out::println);
        // [1, 2, 3]
        // [4, 5, 6]
        // [7]          <- last chunk is smaller, no error

        // ── Real-world: batch API calls ───────────────────────────────────────
        System.out.println("\n=== chunk: simulated batch API calls ===");

        var userIds = List.of(101, 102, 103, 104, 105, 106, 107, 108, 109, 110);

        FluentStream.of(userIds)
                .chunk(3)
                .forEach(batch -> System.out.println("Calling API with batch: " + batch));
        // Calling API with batch: [101, 102, 103]
        // Calling API with batch: [104, 105, 106]
        // Calling API with batch: [107, 108, 109]
        // Calling API with batch: [110]

        // ── Real-world: paginate results ──────────────────────────────────────
        System.out.println("\n=== chunk: pagination ===");

        var products = List.of("Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headset");

        FluentStream.of(products)
                .chunk(3)
                .zipWithIndex()
                .forEach(page -> System.out.printf(
                        "Page %d: %s%n", page.index() + 1, page.value()
                ));
        // Page 1: [Laptop, Phone, Tablet]
        // Page 2: [Monitor, Keyboard, Mouse]
        // Page 3: [Headset]
    }
}
