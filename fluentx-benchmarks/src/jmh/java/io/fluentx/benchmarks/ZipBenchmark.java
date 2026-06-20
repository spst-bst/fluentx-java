package io.fluentx.benchmarks;

import io.fluentx.streams.FluentStream;
import io.fluentx.streams.Pair;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks FluentStream.zip() vs a manual dual-iterator approach.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class ZipBenchmark {

    @Param({"10000", "100000", "500000", "1000000"})
    private int size;

    private List<Integer> numbersA;
    private List<String>  labelsB;

    @Setup
    public void setup() {
        numbersA = IntStream.range(0, size).boxed().collect(Collectors.toList());
        labelsB  = IntStream.range(0, size).mapToObj(i -> "label-" + i).collect(Collectors.toList());
    }

    @Benchmark
    public void fluentX_zip(Blackhole bh) {
        FluentStream.of(numbersA)
                .zip(labelsB.stream())
                .forEach(p -> bh.consume(p));
    }

    @Benchmark
    public void baseline_dualIterator(Blackhole bh) {
        Iterator<Integer> iterA = numbersA.iterator();
        Iterator<String>  iterB = labelsB.iterator();
        Iterable<Pair<Integer, String>> iterable = () -> new Iterator<>() {
            @Override public boolean hasNext() { return iterA.hasNext() && iterB.hasNext(); }
            @Override public Pair<Integer, String> next() { return new Pair<>(iterA.next(), iterB.next()); }
        };
        StreamSupport.stream(iterable.spliterator(), false)
                .forEach(p -> bh.consume(p));
    }
}
