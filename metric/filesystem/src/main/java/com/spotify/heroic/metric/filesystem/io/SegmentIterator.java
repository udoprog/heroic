package com.spotify.heroic.metric.filesystem.io;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.function.ThrowingRunnable;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

/**
 * A special kind of iterator over sorted values.
 *
 * Always guarantees uniqueness with respect to how the values compare with each other.
 */
public interface SegmentIterator<T extends Comparable<T>> extends AutoCloseable {
    /**
     * Check if there is another value.
     */
    boolean hasNext() throws Exception;

    /**
     * Get the next value.
     */
    T next() throws Exception;

    default SegmentIterator<T> filtering(final Predicate<T> predicate) {
        return new FilteringSegmentIterator<>(this, predicate);
    }

    @Override
    default void close() throws Exception {
    }

    /**
     * Register an additional closer.
     */
    default SegmentIterator<T> closing(ThrowingRunnable closer) {
        return new ClosingSegmentIterator<>(this, closer);
    }

    /**
     * Converts this into a regular iterator.
     *
     * Should mainly be used for testing since this will discard the ability to call
     * {@link #close()}.
     */
    default Iterator<T> toIterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                try {
                    return SegmentIterator.this.hasNext();
                } catch (Exception e) {
                    throw new RuntimeException();
                }
            }

            @Override
            public T next() {
                try {
                    return SegmentIterator.this.next();
                } catch (Exception e) {
                    throw new RuntimeException();
                }
            }
        };
    }

    /**
     * Merge a sorted collection of iterators.
     */
    static <T extends Comparable<T>> SegmentIterator<T> mergeSorted(
        Collection<? extends SegmentIterator<T>> iterators
    ) throws Exception {
        if (iterators.size() == 1) {
            return iterators.iterator().next();
        }

        return new MergeSortedSegmentIterator<>(ImmutableList.copyOf(iterators));
    }

    static <T extends Comparable<T>> SegmentIterator<T> fromIterator(final Iterator<T> iterator) {
        return new SegmentIterator<T>() {
            @Override
            public boolean hasNext() throws Exception {
                return iterator.hasNext();
            }

            @Override
            public T next() throws Exception {
                return iterator.next();
            }
        };
    }
}
