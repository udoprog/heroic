package com.spotify.heroic.metric.filesystem.transaction;

import com.spotify.heroic.metric.filesystem.Segment;
import com.spotify.heroic.metric.filesystem.SegmentKey;
import com.spotify.heroic.metric.filesystem.Transaction;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

public abstract class WriteTransaction implements Transaction {
    protected abstract SegmentKey key();

    protected <T extends Comparable<T>, V> NavigableMap<Long, V> getOrCreateWriteMap(
        final SegmentSnapshot<T, V> write, final long txId
    ) throws Exception {
        final SegmentKey key = key();
        final Optional<Segment<T>> segment = write.segments.get(key);

        if (segment.map(s -> s.isNewer(txId)).orElse(false)) {
            return null;
        }

        NavigableMap<Long, V> map = write.snapshot.get(key);

        if (map == null) {
            map = new TreeMap<>();
            write.snapshot.put(key, map);
        }

        write.changed = true;
        return map;
    }
}
