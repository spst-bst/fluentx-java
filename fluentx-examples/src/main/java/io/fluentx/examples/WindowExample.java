package io.fluentx.examples;

import io.fluentx.streams.FluentStream;
import java.util.List;

/**
 * Examples for {@code FluentStream.window(int size)}.
 *
 * <p>Use case: analyse overlapping sequences — moving averages,
 * trend detection, or any algorithm that needs context from
 * neighbouring elements.
 */
public class WindowExample {

    public static void main(String[] args) {

        // ── Basic usage ───────────────────────────────────────────────────────
        System.out.println("=== window: basic ===");

        FluentStream.of(1, 2, 3, 4, 5)
                .window(3)
                .forEach(System.out::println);
        // [1, 2, 3]
        // [2, 3, 4]
        // [3, 4, 5]

        // ── Real-world: 3-day moving average ──────────────────────────────────
        System.out.println("\n=== window: 3-day moving average ===");

        var dailyPrices = List.of(100.0, 102.5, 101.0, 105.0, 107.5, 106.0, 110.0);

        FluentStream.of(dailyPrices)
                .window(3)
                .map(w -> w.stream().mapToDouble(Double::doubleValue).average().orElse(0))
                .forEach(avg -> System.out.printf("Moving avg: %.2f%n", avg));
        // Moving avg: 101.17
        // Moving avg: 102.83
        // Moving avg: 104.50
        // Moving avg: 106.17
        // Moving avg: 107.83

        // ── Real-world: detect consecutive increasing values ──────────────────
        System.out.println("\n=== window: detect upward trend (3 consecutive rises) ===");

        var temps = List.of(20, 21, 19, 22, 23, 24, 20, 25);

        FluentStream.of(temps)
                .window(3)
                .filter(w -> w.get(0) < w.get(1) && w.get(1) < w.get(2))
                .forEach(w -> System.out.println("Upward trend detected: " + w));
        // Upward trend detected: [19, 22, 23]
        // Upward trend detected: [22, 23, 24]
    }
}
