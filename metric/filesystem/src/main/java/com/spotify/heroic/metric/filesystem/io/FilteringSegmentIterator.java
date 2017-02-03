package com.spotify.heroic.metric.filesystem.io;

import java.util.NoSuchElementException;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FilteringSegmentIterator<T extends Comparable<T>> implements SegmentIterator<T> {
    private final SegmentIterator<T> parent;
    private final Predicate<T> predicate;

    private T nextValue = null;

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
        return next;
    }

    @Override
    public void close() throws Exception {
        parent.close();
    }

    private void flushNext() throws Exception {
        while (nextValue == null && parent.hasNext()) {
            final T candidate = parent.next();

            if (!predicate.test(candidate)) {
                continue;
            }

            nextValue = candidate;
        }
    }
}
