package io.fluentx.gatherers;

import static io.fluentx.gatherers.FluentGatherers.*;
import static org.junit.jupiter.api.Assertions.*;

import io.fluentx.streams.Indexed;
import io.fluentx.streams.Pair;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FluentGatherersTest {

    @Nested
    class ChunkTests {

        @Test
        void splitsIntoFixedSizeGroups() {
            var result = Stream.of(1, 2, 3, 4, 5).gather(chunk(2)).toList();
            assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5)), result);
        }

        @Test
        void exactMultiple() {
            var result = Stream.of(1, 2, 3, 4).gather(chunk(2)).toList();
            assertEquals(List.of(List.of(1, 2), List.of(3, 4)), result);
        }

        @Test
        void emptyStream() {
            assertTrue(Stream.<Integer>of().gather(chunk(3)).toList().isEmpty());
        }

        @Test
        void throwsOnNonPositiveSize() {
            assertThrows(IllegalArgumentException.class, () -> chunk(0));
        }

        @Test
        void isLazyOnInfiniteStream() {
            var first = Stream.iterate(0, i -> i + 1).gather(chunk(10)).findFirst();
            assertTrue(first.isPresent());
            assertEquals(10, first.get().size());
        }

        @Test
        void chunksAreUnmodifiable() {
            var chunks = Stream.of(1, 2, 3).gather(chunk(2)).toList();
            assertThrows(UnsupportedOperationException.class, () -> chunks.get(0).add(99));
        }
    }

    @Nested
    class WindowTests {

        @Test
        void producesSlidingWindows() {
            var result = Stream.of(1, 2, 3, 4, 5).gather(window(3)).toList();
            assertEquals(List.of(List.of(1, 2, 3), List.of(2, 3, 4), List.of(3, 4, 5)), result);
        }

        @Test
        void largerThanStream_returnsEmpty() {
            assertTrue(Stream.of(1, 2).gather(window(5)).toList().isEmpty());
        }

        @Test
        void throwsOnNonPositiveSize() {
            assertThrows(IllegalArgumentException.class, () -> window(0));
        }

        @Test
        void isLazyOnInfiniteStream() {
            var firstTwo = Stream.iterate(0, i -> i + 1).gather(window(3)).limit(2).toList();
            assertEquals(List.of(List.of(0, 1, 2), List.of(1, 2, 3)), firstTwo);
        }
    }

    @Nested
    class GroupConsecutiveTests {

        @Test
        void groupsAdjacentEqualElements() {
            var result = Stream.of(1, 1, 2, 3, 3, 3, 1).gather(groupConsecutive()).toList();
            assertEquals(List.of(List.of(1, 1), List.of(2), List.of(3, 3, 3), List.of(1)), result);
        }

        @Test
        void groupsByKeyFunction() {
            var result = Stream.of("apple", "apricot", "banana", "blueberry", "cherry")
                    .gather(groupConsecutiveBy(s -> s.charAt(0)))
                    .toList();
            assertEquals(List.of(
                    List.of("apple", "apricot"),
                    List.of("banana", "blueberry"),
                    List.of("cherry")), result);
        }

        @Test
        void emptyStream() {
            assertTrue(Stream.<Integer>of().gather(groupConsecutive()).toList().isEmpty());
        }

        @Test
        void singleElement() {
            assertEquals(List.of(List.of(42)), Stream.of(42).gather(groupConsecutive()).toList());
        }

        @Test
        void isLazyOnInfiniteStream() {
            var firstTwo = Stream.iterate(0, i -> i + 1).map(i -> i / 2)
                    .gather(groupConsecutive()).limit(2).toList();
            assertEquals(List.of(List.of(0, 0), List.of(1, 1)), firstTwo);
        }

        @Test
        void groupsAreUnmodifiable() {
            var groups = Stream.of(1, 1, 2).gather(groupConsecutive()).toList();
            assertThrows(UnsupportedOperationException.class, () -> groups.get(0).add(99));
        }

        @Test
        void rejectsNullKeyFn() {
            assertThrows(NullPointerException.class, () -> groupConsecutiveBy(null));
        }
    }

    @Nested
    class ScanTests {

        @Test
        void producesRunningAccumulation() {
            var result = Stream.of(1, 2, 3, 4).gather(scan(0, Integer::sum)).toList();
            assertEquals(List.of(0, 1, 3, 6, 10), result);
        }

        @Test
        void emptyStream_emitsIdentityOnly() {
            var result = Stream.<Integer>of().gather(scan(0, Integer::sum)).toList();
            assertEquals(List.of(0), result);
        }

        @Test
        void stringConcatenation() {
            var result = Stream.of("a", "b", "c").gather(scan("", String::concat)).toList();
            assertEquals(List.of("", "a", "ab", "abc"), result);
        }

        @Test
        void isLazyOnInfiniteStream() {
            var result = Stream.iterate(0, i -> i + 1).gather(scan(0, Integer::sum)).limit(5).toList();
            assertEquals(List.of(0, 0, 1, 3, 6), result);
        }

        @Test
        void rejectsNullAccumulator() {
            assertThrows(NullPointerException.class, () -> scan(0, null));
        }
    }

    @Nested
    class ZipWithNextTests {

        @Test
        void pairsConsecutiveElements() {
            var result = Stream.of(1, 2, 3, 4).gather(zipWithNext()).toList();
            assertEquals(List.of(new Pair<>(1, 2), new Pair<>(2, 3), new Pair<>(3, 4)), result);
        }

        @Test
        void singleElement_returnsEmpty() {
            assertTrue(Stream.of(1).gather(zipWithNext()).toList().isEmpty());
        }

        @Test
        void emptyStream_returnsEmpty() {
            assertTrue(Stream.<Integer>of().gather(zipWithNext()).toList().isEmpty());
        }

        @Test
        void isLazyOnInfiniteStream() {
            var firstTwo = Stream.iterate(0, i -> i + 1).gather(zipWithNext()).limit(2).toList();
            assertEquals(List.of(new Pair<>(0, 1), new Pair<>(1, 2)), firstTwo);
        }
    }

    @Nested
    class ZipWithIndexTests {

        @Test
        void assignsCorrectIndices() {
            var result = Stream.of("a", "b", "c").gather(zipWithIndex()).toList();
            assertEquals(List.of(
                    new Indexed<>(0, "a"),
                    new Indexed<>(1, "b"),
                    new Indexed<>(2, "c")), result);
        }

        @Test
        void emptyStream() {
            assertTrue(Stream.of().gather(zipWithIndex()).toList().isEmpty());
        }
    }

    @Nested
    class DistinctByTests {

        @Test
        void keepsFirstOccurrencePerKey() {
            var result = Stream.of("apple", "apricot", "banana", "blueberry")
                    .gather(distinctBy(s -> s.charAt(0))).toList();
            assertEquals(List.of("apple", "banana"), result);
        }

        @Test
        void allUnique() {
            var result = Stream.of(1, 2, 3).gather(distinctBy(Function.identity())).toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        void rejectsNullKeyFn() {
            assertThrows(NullPointerException.class, () -> distinctBy(null));
        }
    }

    @Nested
    class TakeUntilTests {

        @Test
        void stopsBeforeMatchingElement() {
            var result = Stream.of(1, 2, 3, 4, 5).gather(takeUntil(n -> n >= 3)).toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        void neverMatches_returnsAll() {
            var result = Stream.of(1, 2, 3).gather(takeUntil(n -> n > 100)).toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        void shortCircuitsInfiniteStream() {
            var result = Stream.iterate(0, i -> i + 1).gather(takeUntil(n -> n >= 4)).toList();
            assertEquals(List.of(0, 1, 2, 3), result);
        }

        @Test
        void rejectsNullPredicate() {
            assertThrows(NullPointerException.class, () -> takeUntil(null));
        }
    }

    @Nested
    class CompositionTests {

        @Test
        void gatherersComposeInOnePipeline() {
            // chunk into pairs, then index each chunk
            var result = Stream.of(10, 20, 30, 40, 50)
                    .gather(chunk(2))
                    .gather(zipWithIndex())
                    .toList();
            assertEquals(List.of(
                    new Indexed<>(0, List.of(10, 20)),
                    new Indexed<>(1, List.of(30, 40)),
                    new Indexed<>(2, List.of(50))), result);
        }

        @Test
        void interleavesWithNativeStreamOps() {
            var result = Stream.of(1, 2, 3, 4, 5, 6)
                    .filter(n -> n % 2 == 0)
                    .gather(scan(0, Integer::sum))
                    .toList();
            assertEquals(List.of(0, 2, 6, 12), result);
        }
    }
}
