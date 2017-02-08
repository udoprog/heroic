package com.spotify.heroic.metric.filesystem.io;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import lombok.RequiredArgsConstructor;

public class MergeSortedSegmentIterator<T extends Comparable<T>> implements SegmentIterator<T> {
    private final Collection<? extends SegmentIterator<T>> iterators;

    private final PriorityQueue<Entry<T>> heap = new PriorityQueue<>();

    private T prevValue = null;
    private T nextValue = null;

    public MergeSortedSegmentIterator(
        final Collection<SegmentIterator<T>> iterators
    ) throws Exception {
        this.iterators = iterators;

        for (final SegmentIterator<T> iterator : iterators) {
            if (iterator.hasNext()) {
                heap.add(new Entry<>(iterator, iterator.next()));
            }
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        flushNext();

        return nextValue != null;
    }

    @Override
    public T next() throws Exception {
        flushNext();

        final T next = this.nextValue;

        if (next == null) {
            throw new NoSuchElementException();
        }

        this.nextValue = null;
        this.prevValue = next;
        return next;
    }

    @Override
    public void close() throws Exception {
        Exception e = null;

        for (final SegmentIterator<T> it : iterators) {
            try {
                it.close();
            } catch (final Exception inner) {
                if (e != null) {
                    inner.addSuppressed(e);
                }

                e = inner;
            }
        }

        if (e != null) {
            throw e;
        }
    }

    @RequiredArgsConstructor
    public static class Entry<T extends Comparable<T>> implements Comparable<Entry<T>> {
        private final SegmentIterator<T> iterator;
        private final T value;

        @Override
        public int compareTo(final Entry<T> o) {
            final int v = value.compareTo(o.value);

            if (v != 0) {
                return v;
            }

            return Long.compare(iterator.slot(), o.iterator.slot());
        }
    }

    private void flushNext() throws Exception {
        while (!heap.isEmpty() && nextValue == null) {
            nextValue = pollNext();

            // skip identical values
            if (prevValue != null) {
                if (nextValue.compareTo(prevValue) == 0) {
                    nextValue = null;
                    continue;
                }
            }

            break;
        }
    }

    private T pollNext() throws Exception {
        final Entry<T> next = heap.poll();

        if (next.iterator.hasNext()) {
            heap.add(new Entry<>(next.iterator, next.iterator.next()));
        }

        return next.value;
    }
}
