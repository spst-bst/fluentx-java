package io.fluentx.examples;

import io.fluentx.streams.FluentStream;

import java.util.List;
import java.util.stream.Stream;

/**
 * Real-world example: an e-commerce order processing pipeline.
 *
 * <p>Simulates a batch of incoming orders flowing through a backend service.
 * Each step maps to a business operation that a real order processor would perform,
 * showing how FluentX eliminates the boilerplate that would otherwise appear in
 * each step.
 *
 * <p>Run via: {@code ./gradlew :fluentx-examples:run}
 */
public class OrderProcessingPipeline {

    // ── Domain model ──────────────────────────────────────────────────────────

    enum Status { PENDING, PROCESSING, SHIPPED, DELIVERED }

    record Order(String id, String customerId, Status status, double amount, String category) {
        @Override public String toString() {
            return String.format("Order[%s cust=%s %s $%.2f (%s)]",
                    id, customerId, status, amount, category);
        }
    }

    record NumberedOrder(int seq, Order order) {}

    record Batch(int batchNum, List<Order> orders) {
        double total() { return orders.stream().mapToDouble(Order::amount).sum(); }
    }

    // ── Sample data ───────────────────────────────────────────────────────────

    static List<Order> incomingOrders() {
        return List.of(
            new Order("ORD-001", "C01", Status.PENDING,    120.00, "Electronics"),
            new Order("ORD-002", "C02", Status.PENDING,     45.50, "Books"),
            new Order("ORD-003", "C03", Status.PENDING,    310.00, "Electronics"),
            new Order("ORD-004", "C01", Status.PROCESSING, 89.99,  "Clothing"),
            new Order("ORD-005", "C04", Status.PROCESSING, 230.00, "Electronics"),
            new Order("ORD-006", "C05", Status.PROCESSING, 15.00,  "Books"),
            new Order("ORD-007", "C02", Status.SHIPPED,    540.00, "Electronics"),
            new Order("ORD-008", "C06", Status.SHIPPED,     72.50, "Clothing"),
            new Order("ORD-009", "C07", Status.SHIPPED,    190.00, "Electronics"),
            new Order("ORD-010", "C03", Status.DELIVERED,   55.00, "Books"),
            new Order("ORD-011", "C08", Status.DELIVERED,  420.00, "Electronics"),
            new Order("ORD-012", "C01", Status.DELIVERED,   33.00, "Clothing")
        );
    }

    // ── Pipeline steps ────────────────────────────────────────────────────────

    public static void main(String[] args) {

        List<Order> orders = incomingOrders();

        // ── Step 1: zipWithIndex ──────────────────────────────────────────────
        // Assign a processing sequence number to each order for audit logging.
        // Without FluentX: manual int[] counter inside a map() lambda.
        System.out.println("=== Step 1: Assign sequence numbers (zipWithIndex) ===");

        FluentStream.of(orders)
                .zipWithIndex()
                .map(e -> new NumberedOrder(e.index() + 1, e.value()))
                .forEach(n -> System.out.printf("  [%02d] %s%n", n.seq(), n.order().id()));

        // ── Step 2: groupConsecutiveBy ────────────────────────────────────────
        // Orders arrive pre-sorted by status from the upstream queue.
        // Group consecutive orders with the same status into processing batches.
        // Without FluentX: 15+ lines of stateful loop with lastKey tracking.
        System.out.println("\n=== Step 2: Group by status for staged processing (groupConsecutiveBy) ===");

        FluentStream.of(orders)
                .groupConsecutiveBy(Order::status)
                .forEach(group -> {
                    Status status = group.get(0).status();
                    double subtotal = group.stream().mapToDouble(Order::amount).sum();
                    System.out.printf("  %-11s %d orders  subtotal=$%.2f%n",
                            status, group.size(), subtotal);
                });

        // ── Step 3: scan ─────────────────────────────────────────────────────
        // Compute a running revenue total as orders are confirmed.
        // Useful for dashboards or stopping when a daily revenue cap is hit.
        // Without FluentX: accumulator variable + list append in a for loop.
        System.out.println("\n=== Step 3: Running revenue total (scan) ===");

        FluentStream.of(orders)
                .scan(0.0, (total, o) -> total + o.amount())
                .zipWithIndex()
                .filter(e -> e.index() > 0)   // skip the seed value (0.0)
                .forEach(e -> {
                    Order o = orders.get(e.index() - 1);
                    System.out.printf("  After %-7s  running total = $%7.2f%n",
                            o.id(), e.value());
                });

        // ── Step 4: takeUntil ────────────────────────────────────────────────
        // Only process orders until today's fulfilment budget ($700) is reached.
        // Stop before the order that would exceed the cap — don't process it.
        // Without FluentX: stream.takeWhile(predicate.negate()) — less readable intent.
        System.out.println("\n=== Step 4: Process orders up to $700 daily cap (takeUntil) ===");

        final double DAILY_CAP = 700.00;
        double[] running = {0.0};

        FluentStream.of(orders)
                .takeUntil(o -> {
                    running[0] += o.amount();
                    return running[0] > DAILY_CAP;
                })
                .forEach(o -> System.out.printf("  Approved  %-8s  $%.2f%n", o.id(), o.amount()));

        System.out.printf("  Cap of $%.2f reached — remaining orders deferred to next batch.%n", DAILY_CAP);

        // ── Step 5: chunk ────────────────────────────────────────────────────
        // Send orders to the payment processor in batches of 3
        // (the processor's max batch size per API call).
        // Without FluentX: manual subList loop with index arithmetic.
        System.out.println("\n=== Step 5: Batch to payment processor in groups of 3 (chunk) ===");

        FluentStream.of(orders)
                .chunk(3)
                .zipWithIndex()
                .map(e -> new Batch(e.index() + 1, e.value()))
                .forEach(batch -> System.out.printf(
                        "  Batch %d → %d orders  batch total=$%.2f  ids=%s%n",
                        batch.batchNum(),
                        batch.orders().size(),
                        batch.total(),
                        batch.orders().stream().map(Order::id).toList()));

        // ── Step 6: distinctBy ───────────────────────────────────────────────
        // Build a list of unique customers who placed at least one order today,
        // keeping only their first order as the representative record.
        // Without FluentX: Set<String> seen + stateful filter.
        System.out.println("\n=== Step 6: Unique customers (first order per customer) (distinctBy) ===");

        FluentStream.of(orders)
                .distinctBy(Order::customerId)
                .forEach(o -> System.out.printf(
                        "  Customer %-4s  first order %-8s  $%.2f%n",
                        o.customerId(), o.id(), o.amount()));

        // ── Step 7: zip ──────────────────────────────────────────────────────
        // Pair each order with its generated confirmation code for the dispatch email.
        // Without FluentX: dual-iterator boilerplate with StreamSupport.
        System.out.println("\n=== Step 7: Pair orders with confirmation codes (zip) ===");

        Stream<String> confirmationCodes = orders.stream()
                .map(o -> "CONF-" + o.id().replace("ORD-", "") + "-" + (o.hashCode() & 0xFFFF));

        FluentStream.of(orders)
                .zip(confirmationCodes)
                .forEach(pair -> System.out.printf(
                        "  %-8s  -> %s%n", pair.first().id(), pair.second()));

        // ── Step 8: window ───────────────────────────────────────────────────
        // Fraud detection: inspect every sliding window of 3 consecutive orders.
        // Flag any window where the same customer appears twice within 3 orders.
        // Without FluentX: manual index arithmetic with subList calls.
        System.out.println("\n=== Step 8: Fraud detection — flag repeated customer in 3-order window (window) ===");

        FluentStream.of(orders)
                .window(3)
                .forEach(window -> {
                    long distinctCustomers = window.stream()
                            .map(Order::customerId)
                            .distinct()
                            .count();
                    if (distinctCustomers < window.size()) {
                        List<String> ids   = window.stream().map(Order::id).toList();
                        List<String> custs = window.stream().map(Order::customerId).toList();
                        System.out.printf("  ALERT  window=%s  customers=%s  (repeat detected)%n",
                                ids, custs);
                    }
                });

        System.out.println("\n  No further anomalies detected.");
        System.out.println("\n=== Pipeline complete ===");
    }
}
