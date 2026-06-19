package io.fluentx.benchmarks;

import io.fluentx.streams.FluentStream;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Benchmarks FluentStream.groupConsecutive() and groupConsecutiveBy()
 * vs a manual consecutive-grouping loop.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class GroupConsecutiveBenchmark {

    @Param({"10000", "100000", "500000", "1000000"})
    private int size;

    private List<Integer> data;
    private List<String>  words;

    @Setup
    public void setup() {
        // Alternating groups: 0,0,0, 1,1,1, 2,2,2, ...
        data = IntStream.range(0, size)
                .mapToObj(i -> i / 3)
                .collect(Collectors.toList());

        // Words grouped by first letter
        String[] letters = {"apple", "avocado", "banana", "blueberry", "cherry", "citrus"};
        words = IntStream.range(0, size)
                .mapToObj(i -> letters[i % letters.length])
                .collect(Collectors.toList());
    }

    @Benchmark
    public void fluentX_groupConsecutive(Blackhole bh) {
        FluentStream.of(data)
                .groupConsecutive()
                .forEach(g -> bh.consume(g));
    }

    @Benchmark
    public void fluentX_groupConsecutiveBy(Blackhole bh) {
        FluentStream.of(words)
                .groupConsecutiveBy(s -> s.charAt(0))
                .forEach(g -> bh.consume(g));
    }

    @Benchmark
    public void baseline_manualGrouping(Blackhole bh) {
        List<List<Integer>> groups = new ArrayList<>();
        List<Integer>       current = new ArrayList<>();
        Object lastKey = null;
        boolean first = true;

        for (Integer el : data) {
            if (first || !Objects.equals(el, lastKey)) {
                if (!current.isEmpty()) { groups.add(List.copyOf(current)); current.clear(); }
                first   = false;
                lastKey = el;
            }
            current.add(el);
        }
        if (!current.isEmpty()) groups.add(List.copyOf(current));
        groups.forEach(g -> bh.consume(g));
    }
}
