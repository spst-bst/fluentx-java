package io.fluentx.benchmarks;

import io.fluentx.streams.FluentStream;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Benchmarks FluentStream.scan() vs a manual ArrayList accumulation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class ScanBenchmark {

    @Param({"10000", "100000", "500000", "1000000"})
    private int size;

    private List<Integer> data;

    @Setup
    public void setup() {
        data = IntStream.range(0, size).boxed().collect(Collectors.toList());
    }

    @Benchmark
    public void fluentX_scan(Blackhole bh) {
        FluentStream.of(data)
                .scan(0, Integer::sum)
                .forEach(v -> bh.consume(v));
    }

    @Benchmark
    public void baseline_manualAccumulation(Blackhole bh) {
        List<Integer> results = new ArrayList<>();
        results.add(0);
        data.forEach(el -> results.add(results.get(results.size() - 1) + el));
        results.forEach(v -> bh.consume(v));
    }
}
