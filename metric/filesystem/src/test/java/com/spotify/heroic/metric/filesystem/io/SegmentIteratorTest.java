package com.spotify.heroic.metric.filesystem.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.function.ThrowingRunnable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class SegmentIteratorTest {
    @Test
    public void testMerged() throws Exception {
        final Set<Integer> closed = new HashSet<>();

        final List<SegmentIterator<Integer>> iterators = new ArrayList<>();
        iterators.add(SegmentIterator
            .fromIterator(ImmutableList.of(1, 3, 5).iterator())
            .closing(() -> closed.add(1)));
        iterators.add(SegmentIterator
            .fromIterator(ImmutableList.of(2, 4, 6).iterator())
            .closing(() -> closed.add(2)));
        iterators.add(SegmentIterator
            .fromIterator(ImmutableList.of(3, 5, 7).iterator())
            .closing(() -> closed.add(3)));

        final List<Integer> result;

        try (final SegmentIterator<Integer> iterator = SegmentIterator.mergeSorted(iterators)) {
            result = ImmutableList.copyOf(iterator.toIterator());
        }

        assertEquals(ImmutableList.of(1, 2, 3, 4, 5, 6, 7), result);
        assertEquals(ImmutableSet.of(1, 2, 3), closed);
    }

    @Test
    public void testClosing() throws Exception {
        final ImmutableList<Integer> source = ImmutableList.of(1, 3, 5);
        final AtomicBoolean closed = new AtomicBoolean();
        final List<Integer> result;

        final ThrowingRunnable closing = () -> {
            closed.set(true);
        };

        try (SegmentIterator<Integer> iterator = SegmentIterator
            .fromIterator(source.iterator())
            .closing(closing)) {
            result = ImmutableList.copyOf(iterator.toIterator());
        }

        assertEquals(ImmutableList.of(1, 3, 5), result);
        assertTrue(closed.get());
    }

    @Test
    public void testFiltering() throws Exception {
        final ImmutableList<Integer> source = ImmutableList.of(1, 3, 5, 7, 9);
        final AtomicBoolean closed = new AtomicBoolean();
        final List<Integer> result;

        final ThrowingRunnable closing = () -> {
            closed.set(true);
        };

        try (SegmentIterator<Integer> iterator = SegmentIterator
            .fromIterator(source.iterator())
            .closing(closing)
            .filtering(v -> v < 5 || v > 7)) {
            result = ImmutableList.copyOf(iterator.toIterator());
        }

        assertEquals(ImmutableList.of(1, 3, 9), result);
        assertTrue(closed.get());
    }
}
