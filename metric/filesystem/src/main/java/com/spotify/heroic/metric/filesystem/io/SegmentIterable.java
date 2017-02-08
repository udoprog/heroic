package com.spotify.heroic.metric.filesystem.io;

public interface SegmentIterable<T extends Comparable<T>> {
    SegmentIterator<T> iterator() throws Exception;

    static <T extends Comparable<T>> SegmentIterable<T> fromIterable(final Iterable<T> values) {
        return () -> SegmentIterator.fromIterator(values.iterator());
    }
}
