package com.spotify.heroic.metric.filesystem.transaction;

import java.util.concurrent.ConcurrentSkipListMap;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SegmentInMemory<T> {
    /**
     * Cached values for a segment.
     */
    final ConcurrentSkipListMap<Long, T> data;
}
