package io.fluentx.benchmarks;

import io.fluentx.streams.FluentStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks FluentStream.window() vs a manual sliding index loop.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class WindowBenchmark {

    @Param({"10000", "100000", "500000", "1000000"})
    private int size;

    private static final int WINDOW_SIZE = 3;

    private List<Integer> data;

    @Setup
    public void setup() {
        data = IntStream.range(0, size).boxed().collect(Collectors.toList());
    }

    @Benchmark
    public void fluentX_window(Blackhole bh) {
        FluentStream.of(data)
                .window(WINDOW_SIZE)
                .forEach(w -> bh.consume(w));
    }

    @Benchmark
    public void baseline_slidingIndexLoop(Blackhole bh) {
        List<List<Integer>> windows = new ArrayList<>();
        for (int i = 0; i <= data.size() - WINDOW_SIZE; i++) {
            windows.add(Collections.unmodifiableList(data.subList(i, i + WINDOW_SIZE)));
        }
        windows.forEach(w -> bh.consume(w));
    }
}
