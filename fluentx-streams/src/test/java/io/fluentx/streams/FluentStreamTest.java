package io.fluentx.streams;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FluentStreamTest {

    // ── zipWithIndex ──────────────────────────────────────────────────────────

    @Nested
    class ZipWithIndexTests {

        @Test
        void assignsCorrectIndices() {
            var result = FluentStream.of("a", "b", "c").zipWithIndex().toList();
            assertEquals(List.of(
                    new Indexed<>(0, "a"),
                    new Indexed<>(1, "b"),
                    new Indexed<>(2, "c")), result);
        }

        @Test
        void emptyStream_returnsEmpty() {
            assertEquals(0, FluentStream.empty().zipWithIndex().count());
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1, 2, 3).parallelStream()).zipWithIndex());
        }
    }

    // ── zip ───────────────────────────────────────────────────────────────────

    @Nested
    class ZipTests {

        @Test
        void pairsElementsByPosition() {
            var result = FluentStream.of(1, 2, 3).zip(Stream.of("a", "b", "c")).toList();
            assertEquals(List.of(
                    new Pair<>(1, "a"),
                    new Pair<>(2, "b"),
                    new Pair<>(3, "c")), result);
        }

        @Test
        void stopsAtShorterStream() {
            var result = FluentStream.of(1, 2, 3).zip(Stream.of("a", "b")).toList();
            assertEquals(2, result.size());
        }

        @Test
        void emptyStreams() {
            var result = FluentStream.<Integer>empty().zip(Stream.empty()).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).zip(Stream.of(2)));
        }
    }

    // ── zipWithNext ───────────────────────────────────────────────────────────

    @Nested
    class ZipWithNextTests {

        @Test
        void pairsConsecutiveElements() {
            var result = FluentStream.of(1, 2, 3, 4).zipWithNext().toList();
            assertEquals(List.of(
                    new Pair<>(1, 2),
                    new Pair<>(2, 3),
                    new Pair<>(3, 4)), result);
        }

        @Test
        void singleElement_returnsEmpty() {
            var result = FluentStream.of(1).zipWithNext().toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void emptyStream_returnsEmpty() {
            var result = FluentStream.<Integer>empty().zipWithNext().toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void twoElements_returnsOnePair() {
            var result = FluentStream.of("a", "b").zipWithNext().toList();
            assertEquals(List.of(new Pair<>("a", "b")), result);
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1, 2).parallelStream()).zipWithNext());
        }
    }

    // ── scan ──────────────────────────────────────────────────────────────────

    @Nested
    class ScanTests {

        @Test
        void producesRunningAccumulation() {
            var result = FluentStream.of(1, 2, 3, 4).scan(0, Integer::sum).toList();
            assertEquals(List.of(0, 1, 3, 6, 10), result);
        }

        @Test
        void emptyStream_returnsIdentityOnly() {
            var result = FluentStream.<Integer>empty().scan(0, Integer::sum).toList();
            assertEquals(List.of(0), result);
        }

        @Test
        void stringConcatenation() {
            var result = FluentStream.of("a", "b", "c").scan("", String::concat).toList();
            assertEquals(List.of("", "a", "ab", "abc"), result);
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).scan(0, Integer::sum));
        }
    }

    // ── chunk ─────────────────────────────────────────────────────────────────

    @Nested
    class ChunkTests {

        @Test
        void splitsIntoFixedSizeGroups() {
            var result = FluentStream.of(1, 2, 3, 4, 5).chunk(2).toList();
            assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5)), result);
        }

        @Test
        void exactMultiple_noRemainder() {
            var result = FluentStream.of(1, 2, 3, 4).chunk(2).toList();
            assertEquals(List.of(List.of(1, 2), List.of(3, 4)), result);
        }

        @Test
        void chunkSizeOfOne() {
            var result = FluentStream.of(1, 2, 3).chunk(1).toList();
            assertEquals(List.of(List.of(1), List.of(2), List.of(3)), result);
        }

        @Test
        void chunkLargerThanStream() {
            var result = FluentStream.of(1, 2).chunk(10).toList();
            assertEquals(List.of(List.of(1, 2)), result);
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().chunk(3).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void throwsOnNonPositiveSize() {
            assertThrows(IllegalArgumentException.class, () -> FluentStream.of(1).chunk(0));
            assertThrows(IllegalArgumentException.class, () -> FluentStream.of(1).chunk(-1));
        }

        @Test
        void chunksAreUnmodifiable() {
            var chunks = FluentStream.of(1, 2, 3).chunk(2).toList();
            assertThrows(UnsupportedOperationException.class, () -> chunks.get(0).add(99));
        }

        @Test
        void lazyEvaluation_worksWithLargeStream() {
            var first = FluentStream.of(Stream.iterate(0, i -> i + 1).limit(1_000_000))
                    .chunk(100)
                    .findFirst();
            assertTrue(first.isPresent());
            assertEquals(100, first.get().size());
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).chunk(1));
        }
    }

    // ── window ────────────────────────────────────────────────────────────────

    @Nested
    class WindowTests {

        @Test
        void producesSlidingWindows() {
            var result = FluentStream.of(1, 2, 3, 4, 5).window(3).toList();
            assertEquals(List.of(
                    List.of(1, 2, 3),
                    List.of(2, 3, 4),
                    List.of(3, 4, 5)), result);
        }

        @Test
        void windowOfOne_eachElementAlone() {
            var result = FluentStream.of(1, 2, 3).window(1).toList();
            assertEquals(List.of(List.of(1), List.of(2), List.of(3)), result);
        }

        @Test
        void windowEqualsStreamSize() {
            var result = FluentStream.of(1, 2, 3).window(3).toList();
            assertEquals(List.of(List.of(1, 2, 3)), result);
        }

        @Test
        void largerThanStream_returnsEmpty() {
            var result = FluentStream.of(1, 2).window(5).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void throwsOnNonPositiveSize() {
            assertThrows(IllegalArgumentException.class, () -> FluentStream.of(1).window(0));
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().window(3).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).window(1));
        }
    }

    // ── groupConsecutive ──────────────────────────────────────────────────────

    @Nested
    class GroupConsecutiveTests {

        @Test
        void groupsAdjacentEqualElements() {
            var result = FluentStream.of(1, 1, 2, 3, 3, 3, 1).groupConsecutive().toList();
            assertEquals(List.of(
                    List.of(1, 1),
                    List.of(2),
                    List.of(3, 3, 3),
                    List.of(1)), result);
        }

        @Test
        void groupsByKeyFunction() {
            var result = FluentStream.of("apple", "apricot", "banana", "blueberry", "cherry")
                    .groupConsecutiveBy(s -> s.charAt(0))
                    .toList();
            assertEquals(List.of(
                    List.of("apple", "apricot"),
                    List.of("banana", "blueberry"),
                    List.of("cherry")), result);
        }

        @Test
        void singleElement() {
            var result = FluentStream.of(42).groupConsecutive().toList();
            assertEquals(List.of(List.of(42)), result);
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().groupConsecutive().toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void allSameKey() {
            var result = FluentStream.of(1, 1, 1).groupConsecutive().toList();
            assertEquals(List.of(List.of(1, 1, 1)), result);
        }

        @Test
        void allDifferentKeys() {
            var result = FluentStream.of(1, 2, 3).groupConsecutive().toList();
            assertEquals(List.of(List.of(1), List.of(2), List.of(3)), result);
        }

        @Test
        void groupsAreUnmodifiable() {
            var groups = FluentStream.of(1, 1, 2).groupConsecutive().toList();
            assertThrows(UnsupportedOperationException.class, () -> groups.get(0).add(99));
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream())
                            .groupConsecutiveBy(Function.identity()));
        }
    }

    // ── takeUntil ─────────────────────────────────────────────────────────────

    @Nested
    class TakeUntilTests {

        @Test
        void stopsBeforeMatchingElement() {
            var result = FluentStream.of(1, 2, 3, 4, 5).takeUntil(n -> n >= 3).toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        void neverMatches_returnsAll() {
            var result = FluentStream.of(1, 2, 3).takeUntil(n -> n > 100).toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        void firstElementMatches_returnsEmpty() {
            var result = FluentStream.of(1, 2, 3).takeUntil(n -> n >= 1).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().takeUntil(n -> true).toList();
            assertTrue(result.isEmpty());
        }
    }

    // ── distinctBy ────────────────────────────────────────────────────────────

    @Nested
    class DistinctByTests {

        @Test
        void keepsFirstOccurrencePerKey() {
            var result = FluentStream.of("apple", "apricot", "banana", "blueberry")
                    .distinctBy(s -> s.charAt(0))
                    .toList();
            assertEquals(List.of("apple", "banana"), result);
        }

        @Test
        void allUnique_returnsAll() {
            var result = FluentStream.of(1, 2, 3).distinctBy(Function.identity()).toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<String>empty().distinctBy(s -> s).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream())
                            .distinctBy(Function.identity()));
        }
    }

    // ── interleave ────────────────────────────────────────────────────────────

    @Nested
    class InterleaveTests {

        @Test
        void roundRobinEqualLength() {
            var result = FluentStream.of(1, 3, 5).interleave(Stream.of(2, 4, 6)).toList();
            assertEquals(List.of(1, 2, 3, 4, 5, 6), result);
        }

        @Test
        void firstLonger_appendsRemaining() {
            var result = FluentStream.of(1, 3, 5, 7).interleave(Stream.of(2, 4)).toList();
            assertEquals(List.of(1, 2, 3, 4, 5, 7), result);
        }

        @Test
        void secondLonger_appendsRemaining() {
            var result = FluentStream.of(1).interleave(Stream.of(2, 4, 6)).toList();
            assertEquals(List.of(1, 2, 4, 6), result);
        }

        @Test
        void firstEmpty() {
            var result = FluentStream.<Integer>empty().interleave(Stream.of(1, 2)).toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        void secondEmpty() {
            var result = FluentStream.of(1, 2).interleave(Stream.empty()).toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        void bothEmpty() {
            var result = FluentStream.<Integer>empty().interleave(Stream.empty()).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void rejectsParallelStream() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream())
                            .interleave(Stream.of(2)));
        }
    }

    // ── crossProduct ──────────────────────────────────────────────────────────

    @Nested
    class CrossProductTests {

        @Test
        void producesAllPairings() {
            var result = FluentStream.of(1, 2).crossProduct(Stream.of("a", "b")).toList();
            assertEquals(List.of(
                    new Pair<>(1, "a"), new Pair<>(1, "b"),
                    new Pair<>(2, "a"), new Pair<>(2, "b")), result);
        }

        @Test
        void firstEmpty_returnsEmpty() {
            var result = FluentStream.<Integer>empty().crossProduct(Stream.of("a")).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void secondEmpty_returnsEmpty() {
            var result = FluentStream.of(1).crossProduct(Stream.empty()).toList();
            assertTrue(result.isEmpty());
        }

        @Test
        void singleElements() {
            var result = FluentStream.of(1).crossProduct(Stream.of("x")).toList();
            assertEquals(List.of(new Pair<>(1, "x")), result);
        }
    }

    // ── partition ─────────────────────────────────────────────────────────────

    @Nested
    class PartitionTests {

        @Test
        void splitsIntoMatchingAndNotMatching() {
            var result = FluentStream.of(1, 2, 3, 4, 5).partition(n -> n % 2 == 0);
            assertEquals(List.of(2, 4), result.matching());
            assertEquals(List.of(1, 3, 5), result.notMatching());
        }

        @Test
        void allMatch() {
            var result = FluentStream.of(2, 4, 6).partition(n -> n % 2 == 0);
            assertEquals(List.of(2, 4, 6), result.matching());
            assertTrue(result.notMatching().isEmpty());
        }

        @Test
        void noneMatch() {
            var result = FluentStream.of(1, 3, 5).partition(n -> n % 2 == 0);
            assertTrue(result.matching().isEmpty());
            assertEquals(List.of(1, 3, 5), result.notMatching());
        }

        @Test
        void emptyStream() {
            var result = FluentStream.<Integer>empty().partition(n -> true);
            assertTrue(result.matching().isEmpty());
            assertTrue(result.notMatching().isEmpty());
        }

        @Test
        void resultsAreUnmodifiable() {
            var result = FluentStream.of(1, 2).partition(n -> n > 1);
            assertThrows(UnsupportedOperationException.class, () -> result.matching().add(99));
            assertThrows(UnsupportedOperationException.class, () -> result.notMatching().add(99));
        }
    }

    // ── frequencies ───────────────────────────────────────────────────────────

    @Nested
    class FrequencyTests {

        @Test
        void countsOccurrences() {
            var result = FluentStream.of("a", "b", "a", "c", "b", "a").frequencies();
            assertEquals(Map.of("a", 3L, "b", 2L, "c", 1L), result);
        }

        @Test
        void singleElement() {
            assertEquals(Map.of("x", 1L), FluentStream.of("x").frequencies());
        }

        @Test
        void emptyStream() {
            assertTrue(FluentStream.empty().frequencies().isEmpty());
        }

        @Test
        void frequenciesByKey() {
            var result = FluentStream.of("apple", "apricot", "banana")
                    .frequenciesBy(s -> s.charAt(0));
            assertEquals(Map.of('a', 2L, 'b', 1L), result);
        }

        @Test
        void resultIsUnmodifiable() {
            var result = FluentStream.of("a").frequencies();
            assertThrows(UnsupportedOperationException.class, () -> result.put("b", 1L));
        }
    }

    // ── parallel stream rejection ─────────────────────────────────────────────

    @Nested
    class ParallelStreamTests {

        @Test
        void zipWithIndex_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).zipWithIndex());
        }

        @Test
        void scan_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).scan(0, Integer::sum));
        }

        @Test
        void chunk_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).chunk(1));
        }

        @Test
        void window_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).window(1));
        }

        @Test
        void groupConsecutiveBy_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream())
                            .groupConsecutiveBy(Function.identity()));
        }

        @Test
        void distinctBy_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream())
                            .distinctBy(Function.identity()));
        }

        @Test
        void interleave_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream())
                            .interleave(Stream.of(2)));
        }

        @Test
        void zipWithNext_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1, 2).parallelStream()).zipWithNext());
        }

        @Test
        void zip_rejectsParallel() {
            assertThrows(IllegalStateException.class, () ->
                    FluentStream.of(List.of(1).parallelStream()).zip(Stream.of(2)));
        }
    }

    // ── resource safety (onClose propagation) ─────────────────────────────────

    @Nested
    class ResourceSafetyTests {

        /** A stream that flips {@code closed} when its close() handler fires. */
        private Stream<Integer> tracked(AtomicBoolean closed, Integer... values) {
            return Stream.of(values).onClose(() -> closed.set(true));
        }

        @Test
        void scan_closesSource() {
            var closed = new AtomicBoolean(false);
            try (var s = FluentStream.of(tracked(closed, 1, 2, 3)).scan(0, Integer::sum).toStream()) {
                s.forEach(v -> {});
            }
            assertTrue(closed.get(), "scan must propagate close() to the source stream");
        }

        @Test
        void chunk_closesSource() {
            var closed = new AtomicBoolean(false);
            try (var s = FluentStream.of(tracked(closed, 1, 2, 3)).chunk(2).toStream()) {
                s.forEach(v -> {});
            }
            assertTrue(closed.get());
        }

        @Test
        void window_closesSource() {
            var closed = new AtomicBoolean(false);
            try (var s = FluentStream.of(tracked(closed, 1, 2, 3)).window(2).toStream()) {
                s.forEach(v -> {});
            }
            assertTrue(closed.get());
        }

        @Test
        void window_emptyResult_stillClosesSource() {
            // window larger than the stream takes the early-return path
            var closed = new AtomicBoolean(false);
            try (var s = FluentStream.of(tracked(closed, 1, 2)).window(5).toStream()) {
                s.forEach(v -> {});
            }
            assertTrue(closed.get(), "early-return empty window must still close the source");
        }

        @Test
        void zipWithNext_emptyResult_stillClosesSource() {
            var closed = new AtomicBoolean(false);
            try (var s = FluentStream.of(tracked(closed)).zipWithNext().toStream()) {
                s.forEach(v -> {});
            }
            assertTrue(closed.get(), "early-return empty zipWithNext must still close the source");
        }

        @Test
        void groupConsecutiveBy_closesSource() {
            var closed = new AtomicBoolean(false);
            try (var s = FluentStream.of(tracked(closed, 1, 1, 2)).groupConsecutive().toStream()) {
                s.forEach(v -> {});
            }
            assertTrue(closed.get());
        }

        @Test
        void zip_closesBothSources() {
            var closedA = new AtomicBoolean(false);
            var closedB = new AtomicBoolean(false);
            try (var s = FluentStream.of(tracked(closedA, 1, 2))
                    .zip(tracked(closedB, 9, 8)).toStream()) {
                s.forEach(v -> {});
            }
            assertTrue(closedA.get(), "zip must close the left source");
            assertTrue(closedB.get(), "zip must close the right source");
        }

        @Test
        void interleave_closesBothSources() {
            var closedA = new AtomicBoolean(false);
            var closedB = new AtomicBoolean(false);
            try (var s = FluentStream.of(tracked(closedA, 1, 3))
                    .interleave(tracked(closedB, 2, 4)).toStream()) {
                s.forEach(v -> {});
            }
            assertTrue(closedA.get());
            assertTrue(closedB.get());
        }
    }

    // ── laziness on unbounded streams ─────────────────────────────────────────

    @Nested
    class LazinessTests {

        @Test
        void scan_isLazy_onInfiniteStream() {
            var pulls = new AtomicInteger(0);
            var result = FluentStream.of(Stream.iterate(0, i -> i + 1).peek(i -> pulls.incrementAndGet()))
                    .scan(0, Integer::sum)
                    .limit(5)
                    .toList();
            assertEquals(List.of(0, 0, 1, 3, 6), result);
            // identity + 4 elements pulled — must NOT have consumed the whole (infinite) source
            assertTrue(pulls.get() <= 5, "scan over-pulled from an infinite source: " + pulls.get());
        }

        @Test
        void chunk_isLazy_onInfiniteStream() {
            var first = FluentStream.of(Stream.iterate(0, i -> i + 1))
                    .chunk(10)
                    .findFirst();
            assertTrue(first.isPresent());
            assertEquals(10, first.get().size());
        }

        @Test
        void window_isLazy_onInfiniteStream() {
            var firstTwo = FluentStream.of(Stream.iterate(0, i -> i + 1))
                    .window(3)
                    .limit(2)
                    .toList();
            assertEquals(List.of(List.of(0, 1, 2), List.of(1, 2, 3)), firstTwo);
        }

        @Test
        void groupConsecutiveBy_isLazy_onInfiniteStream() {
            // infinite stream of run-lengths: 0,0,1,1,2,2,... grouped by value
            var firstTwo = FluentStream.of(Stream.iterate(0, i -> i + 1).map(i -> i / 2))
                    .groupConsecutive()
                    .limit(2)
                    .toList();
            assertEquals(List.of(List.of(0, 0), List.of(1, 1)), firstTwo);
        }

        @Test
        void zipWithNext_isLazy_onInfiniteStream() {
            var firstTwo = FluentStream.of(Stream.iterate(0, i -> i + 1))
                    .zipWithNext()
                    .limit(2)
                    .toList();
            assertEquals(List.of(new Pair<>(0, 1), new Pair<>(1, 2)), firstTwo);
        }
    }

    // ── null-argument validation ──────────────────────────────────────────────

    @Nested
    class NullArgumentTests {

        @Test
        void zip_rejectsNull() {
            assertThrows(NullPointerException.class, () -> FluentStream.of(1).zip(null));
        }

        @Test
        void interleave_rejectsNull() {
            assertThrows(NullPointerException.class, () -> FluentStream.of(1).interleave(null));
        }

        @Test
        void crossProduct_rejectsNull() {
            assertThrows(NullPointerException.class, () -> FluentStream.of(1).crossProduct(null));
        }

        @Test
        void scan_rejectsNullAccumulator() {
            assertThrows(NullPointerException.class, () -> FluentStream.of(1).scan(0, null));
        }

        @Test
        void groupConsecutiveBy_rejectsNull() {
            assertThrows(NullPointerException.class, () -> FluentStream.of(1).groupConsecutiveBy(null));
        }

        @Test
        void distinctBy_rejectsNull() {
            assertThrows(NullPointerException.class, () -> FluentStream.of(1).distinctBy(null));
        }

        @Test
        void takeUntil_rejectsNull() {
            assertThrows(NullPointerException.class, () -> FluentStream.of(1).takeUntil(null));
        }

        @Test
        void partition_rejectsNull() {
            assertThrows(NullPointerException.class, () -> FluentStream.of(1).partition(null));
        }

        @Test
        void frequenciesBy_rejectsNull() {
            assertThrows(NullPointerException.class, () -> FluentStream.of(1).frequenciesBy(null));
        }
    }

    // ── pass-throughs ─────────────────────────────────────────────────────────

    @Nested
    class PassThroughTests {

        @Test
        void filter() {
            assertEquals(List.of(2, 4),
                    FluentStream.of(1, 2, 3, 4, 5).filter(n -> n % 2 == 0).toList());
        }

        @Test
        void map() {
            assertEquals(List.of(2, 4, 6),
                    FluentStream.of(1, 2, 3).map(n -> n * 2).toList());
        }

        @Test
        void flatMap() {
            assertEquals(List.of(1, 1, 2, 2),
                    FluentStream.of(1, 2).flatMap(n -> Stream.of(n, n)).toList());
        }

        @Test
        void sorted() {
            assertEquals(List.of(1, 2, 3),
                    FluentStream.of(3, 1, 2).sorted().toList());
        }

        @Test
        void limit() {
            assertEquals(List.of(1, 2),
                    FluentStream.of(1, 2, 3).limit(2).toList());
        }

        @Test
        void skip() {
            assertEquals(List.of(3),
                    FluentStream.of(1, 2, 3).skip(2).toList());
        }

        @Test
        void count() {
            assertEquals(3, FluentStream.of(1, 2, 3).count());
        }

        @Test
        void findFirst() {
            assertEquals(1, FluentStream.of(1, 2, 3).findFirst().orElseThrow());
        }

        @Test
        void reduce() {
            assertEquals(6, FluentStream.of(1, 2, 3).reduce(0, Integer::sum));
        }

        @Test
        void anyMatch() {
            assertTrue(FluentStream.of(1, 2, 3).anyMatch(n -> n == 2));
            assertFalse(FluentStream.of(1, 2, 3).anyMatch(n -> n == 5));
        }

        @Test
        void allMatch() {
            assertTrue(FluentStream.of(2, 4, 6).allMatch(n -> n % 2 == 0));
            assertFalse(FluentStream.of(1, 2, 3).allMatch(n -> n % 2 == 0));
        }

        @Test
        void noneMatch() {
            assertTrue(FluentStream.of(1, 3, 5).noneMatch(n -> n % 2 == 0));
        }

        @Test
        void toStream_returnsUnderlyingStream() {
            var stream = Stream.of(1, 2, 3);
            assertSame(stream, FluentStream.of(stream).toStream());
        }
    }
}
