package com.spotify.heroic.metric.filesystem.io;

import com.spotify.heroic.function.ThrowingRunnable;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ClosingSegmentIterator<T extends Comparable<T>> implements SegmentIterator<T> {
    private final SegmentIterator<T> parent;
    private final ThrowingRunnable closer;

    @Override
    public boolean hasNext() throws Exception {
        return parent.hasNext();
    }

    @Override
    public T next() throws Exception {
        return parent.next();
    }

    @Override
    public void close() throws Exception {
        Exception e = null;

        try {
            parent.close();
        } catch (final Exception inner) {
            e = inner;
        }

        try {
            closer.run();
        } catch (final Exception inner) {
            if (e != null) {
                inner.addSuppressed(e);
            }

            e = inner;
        }

        if (e != null) {
            throw e;
        }
    }
}
