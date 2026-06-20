package io.fluentx.examples;

/**
 * Runs all FluentStream examples in sequence.
 *
 * <p>Execute with:
 * <pre>{@code
 *   ./gradlew :fluentx-examples:run
 * }</pre>
 */
public class AllExamples {

    public static void main(String[] args) throws Exception {
        System.out.println("==========================================");
        System.out.println("         FluentX -- Stream Examples       ");
        System.out.println("==========================================\n");

        section("zipWithIndex", ZipWithIndexExample::main);
        section("zip",          ZipExample::main);
        section("scan",         ScanExample::main);
        section("chunk",        ChunkExample::main);
        section("window",       WindowExample::main);
        section("groupConsecutive", GroupConsecutiveExample::main);
        section("takeUntil",    TakeUntilExample::main);
        section("distinctBy",   DistinctByExample::main);

        System.out.println("\n==========================================");
        System.out.println("   Real-World: Order Processing Pipeline  ");
        System.out.println("==========================================");
        OrderProcessingPipeline.main(args);
    }

    @FunctionalInterface
    interface Example { void run(String[] args) throws Exception; }

    private static void section(String name, Example example) throws Exception {
        System.out.println("\n------------------------------------------");
        System.out.println("  " + name);
        System.out.println("------------------------------------------\n");
        example.run(new String[]{});
    }
}
