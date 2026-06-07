package io.fluentx.streams;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class FluentStreamTest {

    // ── zipWithIndex ──────────────────────────────────────────────────────────

    @Test
    void zipWithIndex_assignsCorrectIndices() {
        var result = FluentStream.of("a", "b", "c").zipWithIndex().toList();
        assertEquals(List.of(new Indexed<>(0, "a"), new Indexed<>(1, "b"), new Indexed<>(2, "c")), result);
    }

    @Test
    void zipWithIndex_emptyStream_returnsEmpty() {
        assertEquals(0, FluentStream.empty().zipWithIndex().count());
    }

    // ── zip ───────────────────────────────────────────────────────────────────

    @Test
    void zip_pairsElementsByPosition() {
        var result = FluentStream.of(1, 2, 3).zip(Stream.of("a", "b", "c")).toList();
        assertEquals(List.of(new Pair<>(1, "a"), new Pair<>(2, "b"), new Pair<>(3, "c")), result);
    }

    @Test
    void zip_stopsAtShorterStream() {
        var result = FluentStream.of(1, 2, 3).zip(Stream.of("a", "b")).toList();
        assertEquals(2, result.size());
    }

    // ── scan ──────────────────────────────────────────────────────────────────

    @Test
    void scan_producesRunningAccumulation() {
        var result = FluentStream.of(1, 2, 3, 4).scan(0, Integer::sum).toList();
        assertEquals(List.of(0, 1, 3, 6, 10), result);
    }

    @Test
    void scan_emptyStream_returnsIdentityOnly() {
        var result = FluentStream.<Integer>empty().scan(0, Integer::sum).toList();
        assertEquals(List.of(0), result);
    }

    // ── chunk ─────────────────────────────────────────────────────────────────

    @Test
    void chunk_splitsIntoFixedSizeGroups() {
        var result = FluentStream.of(1, 2, 3, 4, 5).chunk(2).toList();
        assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5)), result);
    }

    @Test
    void chunk_exactMultiple_noRemainder() {
        var result = FluentStream.of(1, 2, 3, 4).chunk(2).toList();
        assertEquals(List.of(List.of(1, 2), List.of(3, 4)), result);
    }

    @Test
    void chunk_throwsOnNonPositiveSize() {
        assertThrows(IllegalArgumentException.class, () -> FluentStream.of(1, 2).chunk(0));
        assertThrows(IllegalArgumentException.class, () -> FluentStream.of(1, 2).chunk(-1));
    }

    // ── window ────────────────────────────────────────────────────────────────

    @Test
    void window_producesSlidingWindows() {
        var result = FluentStream.of(1, 2, 3, 4, 5).window(3).toList();
        assertEquals(List.of(List.of(1, 2, 3), List.of(2, 3, 4), List.of(3, 4, 5)), result);
    }

    @Test
    void window_largerThanStream_returnsEmpty() {
        var result = FluentStream.of(1, 2).window(5).toList();
        assertTrue(result.isEmpty());
    }

    @Test
    void window_throwsOnNonPositiveSize() {
        assertThrows(IllegalArgumentException.class, () -> FluentStream.of(1, 2).window(0));
    }

    // ── groupConsecutive ──────────────────────────────────────────────────────

    @Test
    void groupConsecutive_groupsAdjacentEqualElements() {
        var result = FluentStream.of(1, 1, 2, 3, 3, 3, 1).groupConsecutive().toList();
        assertEquals(List.of(List.of(1, 1), List.of(2), List.of(3, 3, 3), List.of(1)), result);
    }

    @Test
    void groupConsecutiveBy_groupsByKeyFunction() {
        var result = FluentStream.of("apple", "apricot", "banana", "blueberry", "cherry")
                .groupConsecutiveBy(s -> s.charAt(0))
                .toList();
        assertEquals(List.of(
                List.of("apple", "apricot"),
                List.of("banana", "blueberry"),
                List.of("cherry")
        ), result);
    }

    // ── takeUntil ─────────────────────────────────────────────────────────────

    @Test
    void takeUntil_stopsBeforeMatchingElement() {
        var result = FluentStream.of(1, 2, 3, 4, 5).takeUntil(n -> n >= 3).toList();
        assertEquals(List.of(1, 2), result);
    }

    @Test
    void takeUntil_neverMatches_returnsAll() {
        var result = FluentStream.of(1, 2, 3).takeUntil(n -> n > 100).toList();
        assertEquals(List.of(1, 2, 3), result);
    }

    // ── distinctBy ────────────────────────────────────────────────────────────

    @Test
    void distinctBy_keepsFirstOccurrencePerKey() {
        var result = FluentStream.of("apple", "apricot", "banana", "blueberry")
                .distinctBy(s -> s.charAt(0))
                .toList();
        assertEquals(List.of("apple", "banana"), result);
    }

    // ── pass-throughs ─────────────────────────────────────────────────────────

    @Test
    void filter_delegatesToUnderlyingStream() {
        assertEquals(List.of(2, 4), FluentStream.of(1, 2, 3, 4, 5).filter(n -> n % 2 == 0).toList());
    }

    @Test
    void map_transformsElements() {
        assertEquals(List.of(2, 4, 6), FluentStream.of(1, 2, 3).map(n -> n * 2).toList());
    }

    @Test
    void count_returnsCorrectSize() {
        assertEquals(3, FluentStream.of(1, 2, 3).count());
    }
}
