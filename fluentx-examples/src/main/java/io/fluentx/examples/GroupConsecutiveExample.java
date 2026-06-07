package io.fluentx.examples;

import io.fluentx.streams.FluentStream;

import java.util.List;

/**
 * Examples for {@code FluentStream.groupConsecutive()} and
 * {@code FluentStream.groupConsecutiveBy(Function)}.
 *
 * <p>Use case: run-length encoding, log parsing, grouping events
 * by type when order matters.
 */
public class GroupConsecutiveExample {

    public static void main(String[] args) {

        // ── Basic groupConsecutive ────────────────────────────────────────────
        System.out.println("=== groupConsecutive: basic ===");

        FluentStream.of(1, 1, 2, 3, 3, 3, 1)
                .groupConsecutive()
                .forEach(System.out::println);
        // [1, 1]
        // [2]
        // [3, 3, 3]
        // [1]

        // ── Run-length encoding ───────────────────────────────────────────────
        System.out.println("\n=== groupConsecutive: run-length encoding ===");

        FluentStream.of('a', 'a', 'a', 'b', 'c', 'c')
                .groupConsecutive()
                .map(g -> g.size() + "x" + g.get(0))
                .forEach(System.out::println);
        // 3xa
        // 1xb
        // 2xc

        // ── groupConsecutiveBy: group by first letter ─────────────────────────
        System.out.println("\n=== groupConsecutiveBy: group words by first letter ===");

        FluentStream.of("apple", "avocado", "banana", "blueberry", "cherry")
                .groupConsecutiveBy(s -> s.charAt(0))
                .forEach(System.out::println);
        // [apple, avocado]
        // [banana, blueberry]
        // [cherry]

        // ── Real-world: parse log levels ──────────────────────────────────────
        System.out.println("\n=== groupConsecutiveBy: consecutive log level bursts ===");

        record LogEntry(String level, String message) {}

        var logs = List.of(
                new LogEntry("INFO",  "Server started"),
                new LogEntry("INFO",  "Request received"),
                new LogEntry("ERROR", "Database timeout"),
                new LogEntry("ERROR", "Retry attempt 1"),
                new LogEntry("ERROR", "Retry attempt 2"),
                new LogEntry("INFO",  "Connection restored")
        );

        FluentStream.of(logs)
                .groupConsecutiveBy(LogEntry::level)
                .forEach(group -> System.out.printf(
                        "[%s x%d] %s%n",
                        group.get(0).level(),
                        group.size(),
                        group.stream().map(LogEntry::message).toList()
                ));
        // [INFO x2] [Server started, Request received]
        // [ERROR x3] [Database timeout, Retry attempt 1, Retry attempt 2]
        // [INFO x1] [Connection restored]
    }
}
