package io.fluentx.benchmarks;

import io.fluentx.streams.FluentStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks FluentStream.takeUntil() vs stream.takeWhile(predicate.negate()).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class TakeUntilBenchmark {

    @Param({"10000", "100000", "500000", "1000000"})
    private int size;

    private List<Integer> data;

    /** Stop halfway through */
    private final Predicate<Integer> stopCondition = n -> n >= size / 2;

    @Setup
    public void setup() {
        data = IntStream.range(0, size).boxed().collect(Collectors.toList());
    }

    @Benchmark
    public void fluentX_takeUntil(Blackhole bh) {
        FluentStream.of(data)
                .takeUntil(stopCondition)
                .forEach(v -> bh.consume(v));
    }

    @Benchmark
    public void baseline_takeWhileNegate(Blackhole bh) {
        data.stream()
                .takeWhile(stopCondition.negate())
                .forEach(v -> bh.consume(v));
    }
}
