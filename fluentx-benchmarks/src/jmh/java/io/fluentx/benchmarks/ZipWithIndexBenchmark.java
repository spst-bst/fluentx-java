package io.fluentx.benchmarks;

import io.fluentx.streams.FluentStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks FluentStream.zipWithIndex() vs the standard int[] counter workaround.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class ZipWithIndexBenchmark {

    @Param({"10000", "100000", "500000", "1000000"})
    private int size;

    private List<String> data;

    @Setup
    public void setup() {
        data = IntStream.range(0, size)
                .mapToObj(i -> "item-" + i)
                .collect(Collectors.toList());
    }

    @Benchmark
    public void fluentX_zipWithIndex(Blackhole bh) {
        FluentStream.of(data)
                .zipWithIndex()
                .forEach(e -> bh.consume(e));
    }

    @Benchmark
    public void baseline_manualCounter(Blackhole bh) {
        int[] i = {0};
        data.stream()
                .map(v -> Map.entry(i[0]++, v))
                .forEach(e -> bh.consume(e));
    }
}
