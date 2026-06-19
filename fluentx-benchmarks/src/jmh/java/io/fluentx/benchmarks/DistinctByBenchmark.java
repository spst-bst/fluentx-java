package io.fluentx.benchmarks;

import io.fluentx.streams.FluentStream;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Benchmarks FluentStream.distinctBy() vs a manual LinkedHashSet filter.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class DistinctByBenchmark {

    @Param({"10000", "100000", "500000", "1000000"})
    private int size;

    private List<String> data;

    @Setup
    public void setup() {
        // Many duplicates by first char — only 26 distinct keys across N elements
        data = IntStream.range(0, size)
                .mapToObj(i -> (char) ('a' + (i % 26)) + "-item-" + i)
                .collect(Collectors.toList());
    }

    @Benchmark
    public void fluentX_distinctBy(Blackhole bh) {
        FluentStream.of(data)
                .distinctBy(s -> s.charAt(0))
                .forEach(v -> bh.consume(v));
    }

    @Benchmark
    public void baseline_hashSet(Blackhole bh) {
        // HashSet matches the implementation used inside FluentX distinctBy — fair comparison
        Set<Object> seen = new HashSet<>();
        data.stream()
                .filter(el -> seen.add(el.charAt(0)))
                .forEach(v -> bh.consume(v));
    }
}
