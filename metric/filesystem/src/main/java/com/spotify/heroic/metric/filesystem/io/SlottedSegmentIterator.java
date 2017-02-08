package com.spotify.heroic.metric.filesystem.io;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SlottedSegmentIterator<T extends Comparable<T>> implements SegmentIterator<T> {
    private final SegmentIterator<T> parent;
    private final long slot;

    @Override
    public boolean hasNext() throws Exception {
        return parent.hasNext();
    }

    @Override
    public T next() throws Exception {
        return parent.next();
    }

    @Override
    public long slot() {
        return slot;
    }

    @Override
    public void close() throws Exception {
        parent.close();
    }
}
