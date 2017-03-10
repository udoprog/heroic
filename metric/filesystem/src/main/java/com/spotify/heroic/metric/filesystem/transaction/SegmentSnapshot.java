package com.spotify.heroic.metric.filesystem.transaction;

import com.google.common.cache.LoadingCache;
import com.spotify.heroic.metric.filesystem.Segment;
import com.spotify.heroic.metric.filesystem.SegmentKey;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SegmentSnapshot<T extends Comparable<T>, V> {
    /**
     * Filesystem cache for loading segments.
     */
    final LoadingCache<SegmentKey, Optional<Segment<T>>> segments;

    /**
     * Snapshot of data that should be written out to the segment.
     */
    public final Map<SegmentKey, NavigableMap<Long, V>> snapshot = new HashMap<>();

    /**
     * Indicated that this write has changed.
     */
    public boolean changed = false;

    /**
     * Reset the snapshot, clearing all memory used.
     */
    public void reset() {
        if (changed) {
            snapshot.clear();
            this.changed = false;
        }
    }
}
