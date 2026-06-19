package io.fluentx.benchmarks;

import io.fluentx.streams.FluentStream;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Quick overview benchmark — all 9 FluentStream methods across four data sizes.
 * Run this first for a broad summary before diving into the per-method benchmarks.
 *
 * <p>Run with: {@code ./gradlew :fluentx-benchmarks:jmh}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class FluentStreamOverviewBenchmark {

    @Param({"10000", "100000", "500000", "1000000"})
    private int size;

    private List<Integer> ints;
    private List<String>  strings;

    @Setup
    public void setup() {
        ints    = IntStream.range(0, size).boxed().collect(Collectors.toList());
        strings = IntStream.range(0, size)
                .mapToObj(i -> (char) ('a' + (i % 26)) + "-item-" + i)
                .collect(Collectors.toList());
    }

    @Benchmark
    public void zipWithIndex(Blackhole bh) {
        FluentStream.of(strings).zipWithIndex().forEach(e -> bh.consume(e));
    }

    @Benchmark
    public void zip(Blackhole bh) {
        FluentStream.of(ints).zip(strings.stream()).forEach(p -> bh.consume(p));
    }

    @Benchmark
    public void scan(Blackhole bh) {
        FluentStream.of(ints).scan(0, Integer::sum).forEach(v -> bh.consume(v));
    }

    @Benchmark
    public void chunk(Blackhole bh) {
        FluentStream.of(ints).chunk(100).forEach(c -> bh.consume(c));
    }

    @Benchmark
    public void window(Blackhole bh) {
        FluentStream.of(ints).window(3).forEach(w -> bh.consume(w));
    }

    @Benchmark
    public void groupConsecutive(Blackhole bh) {
        FluentStream.of(ints).groupConsecutive().forEach(g -> bh.consume(g));
    }

    @Benchmark
    public void groupConsecutiveBy(Blackhole bh) {
        FluentStream.of(strings).groupConsecutiveBy(s -> s.charAt(0)).forEach(g -> bh.consume(g));
    }

    @Benchmark
    public void takeUntil(Blackhole bh) {
        FluentStream.of(ints).takeUntil(n -> n >= size / 2).forEach(v -> bh.consume(v));
    }

    @Benchmark
    public void distinctBy(Blackhole bh) {
        FluentStream.of(strings).distinctBy(s -> s.charAt(0)).forEach(v -> bh.consume(v));
    }
}
