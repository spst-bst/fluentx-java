package io.fluentx.examples;

import io.fluentx.streams.FluentStream;

/**
 * Examples for {@code FluentStream.scan(identity, accumulator)}.
 *
 * <p>Use case: compute running totals, cumulative averages, or any
 * progressive accumulation where you need every intermediate result,
 * not just the final value.
 */
public class ScanExample {

    public static void main(String[] args) {

        // ── Running sum ───────────────────────────────────────────────────────
        System.out.println("=== scan: running sum ===");

        FluentStream.of(1, 2, 3, 4, 5)
                .scan(0, Integer::sum)
                .forEach(System.out::println);
        // 0  1  3  6  10  15

        // ── Running maximum ───────────────────────────────────────────────────
        System.out.println("\n=== scan: running max ===");

        FluentStream.of(3, 1, 4, 1, 5, 9, 2, 6)
                .scan(Integer.MIN_VALUE, Math::max)
                .skip(1) // skip the seed
                .forEach(System.out::println);
        // 3  3  4  4  5  9  9  9

        // ── Running string concatenation ──────────────────────────────────────
        System.out.println("\n=== scan: building a sentence word-by-word ===");

        FluentStream.of("The", "quick", "brown", "fox")
                .scan("", (acc, word) -> acc.isEmpty() ? word : acc + " " + word)
                .skip(1)
                .forEach(System.out::println);
        // The
        // The quick
        // The quick brown
        // The quick brown fox

        // ── Real-world: daily balance ledger ──────────────────────────────────
        System.out.println("\n=== scan: bank balance after each transaction ===");

        var transactions = new double[]{1000.00, -250.50, -80.00, 500.00, -120.75};
        double openingBalance = 500.00;

        FluentStream.of(transactions[0], transactions[1], transactions[2], transactions[3], transactions[4])
                .scan(openingBalance, Double::sum)
                .skip(1)
                .forEach(balance -> System.out.printf("Balance: $%.2f%n", balance));
        // Balance: $1500.00
        // Balance: $1249.50
        // Balance: $1169.50
        // Balance: $1669.50
        // Balance: $1548.75
    }
}
