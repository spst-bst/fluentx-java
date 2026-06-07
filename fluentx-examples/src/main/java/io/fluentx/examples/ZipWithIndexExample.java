package io.fluentx.examples;

import io.fluentx.streams.FluentStream;

/**
 * Examples for {@code FluentStream.zipWithIndex()}.
 *
 * <p>Use case: iterate a stream and need the position of each element
 * without managing an external counter variable.
 */
public class ZipWithIndexExample {

    public static void main(String[] args) {

        // ── Basic usage ───────────────────────────────────────────────────────
        System.out.println("=== zipWithIndex: basic ===");

        FluentStream.of("apple", "banana", "cherry")
                .zipWithIndex()
                .forEach(e -> System.out.printf("[%d] %s%n", e.index(), e.value()));
        // [0] apple
        // [1] banana
        // [2] cherry

        // ── Print only even-indexed elements ──────────────────────────────────
        System.out.println("\n=== zipWithIndex: filter by index ===");

        FluentStream.of("a", "b", "c", "d", "e")
                .zipWithIndex()
                .filter(e -> e.index() % 2 == 0)
                .forEach(e -> System.out.println(e.index() + " → " + e.value()));
        // 0 → a
        // 2 → c
        // 4 → e

        // ── Real-world: number lines in a file ───────────────────────────────
        System.out.println("\n=== zipWithIndex: numbered lines ===");

        var lines = java.util.List.of(
                "public class Hello {",
                "    public static void main(String[] args) {",
                "        System.out.println(\"Hello, World!\");",
                "    }",
                "}"
        );

        FluentStream.of(lines)
                .zipWithIndex()
                .forEach(e -> System.out.printf("%3d | %s%n", e.index() + 1, e.value()));
        //   1 | public class Hello {
        //   2 |     public static void main(String[] args) {
        //   3 |         System.out.println("Hello, World!");
        //   4 |     }
        //   5 | }
    }
}
