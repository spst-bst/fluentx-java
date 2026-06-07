package io.fluentx.examples;

import io.fluentx.streams.FluentStream;

import java.util.List;

/**
 * Examples for {@code FluentStream.distinctBy(Function keyFn)}.
 *
 * <p>Use case: deduplicate objects by a specific field — something
 * the standard {@code Stream.distinct()} cannot do without overriding
 * {@code equals/hashCode}.
 */
public class DistinctByExample {

    public static void main(String[] args) {

        // ── Basic usage: distinct by first character ──────────────────────────
        System.out.println("=== distinctBy: by first letter ===");

        FluentStream.of("apple", "apricot", "banana", "blueberry", "cherry")
                .distinctBy(s -> s.charAt(0))
                .forEach(System.out::println);
        // apple
        // banana
        // cherry

        // ── Deduplicate objects by field ──────────────────────────────────────
        System.out.println("\n=== distinctBy: unique users by email ===");

        record User(String name, String email) {}

        var users = List.of(
                new User("Alice",   "alice@example.com"),
                new User("Bob",     "bob@example.com"),
                new User("Alice2",  "alice@example.com"),  // duplicate email
                new User("Charlie", "charlie@example.com")
        );

        FluentStream.of(users)
                .distinctBy(User::email)
                .forEach(u -> System.out.println(u.name() + " <" + u.email() + ">"));
        // Alice <alice@example.com>
        // Bob <bob@example.com>
        // Charlie <charlie@example.com>   <- Alice2 dropped (duplicate email)

        // ── Real-world: deduplicate events by type, keep first occurrence ─────
        System.out.println("\n=== distinctBy: first event per type ===");

        record Event(String type, String payload) {}

        FluentStream.of(
                new Event("LOGIN",   "user=alice, ip=1.2.3.4"),
                new Event("VIEW",    "page=/home"),
                new Event("LOGIN",   "user=alice, ip=5.6.7.8"),  // second login
                new Event("LOGOUT",  "user=alice"),
                new Event("VIEW",    "page=/about")               // second view
        )
                .distinctBy(Event::type)
                .forEach(e -> System.out.println(e.type() + " → " + e.payload()));
        // LOGIN  → user=alice, ip=1.2.3.4
        // VIEW   → page=/home
        // LOGOUT → user=alice
    }
}
