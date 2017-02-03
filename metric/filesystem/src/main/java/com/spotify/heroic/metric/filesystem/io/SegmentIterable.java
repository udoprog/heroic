package com.spotify.heroic.metric.filesystem.io;

public interface SegmentIterable<T extends Comparable<T>> {
    SegmentIterator<T> iterator() throws Exception;
}
