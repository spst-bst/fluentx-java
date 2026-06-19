package io.fluentx.benchmarks;

import io.fluentx.streams.FluentStream;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Benchmarks FluentStream.chunk() vs a manual subList loop.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class ChunkBenchmark {

    @Param({"10000", "100000", "500000", "1000000"})
    private int size;

    private static final int CHUNK_SIZE = 100;

    private List<Integer> data;

    @Setup
    public void setup() {
        data = IntStream.range(0, size).boxed().collect(Collectors.toList());
    }

    @Benchmark
    public void fluentX_chunk(Blackhole bh) {
        FluentStream.of(data)
                .chunk(CHUNK_SIZE)
                .forEach(chunk -> bh.consume(chunk));
    }

    @Benchmark
    public void baseline_subListLoop(Blackhole bh) {
        List<List<Integer>> chunks = new ArrayList<>();
        for (int i = 0; i < data.size(); i += CHUNK_SIZE) {
            chunks.add(Collections.unmodifiableList(
                    data.subList(i, Math.min(i + CHUNK_SIZE, data.size()))));
        }
        chunks.forEach(chunk -> bh.consume(chunk));
    }
}
