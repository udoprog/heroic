package com.spotify.heroic.metric.filesystem.io;

public interface SegmentValue<T> {
    long timestamp();

    T value();
}
