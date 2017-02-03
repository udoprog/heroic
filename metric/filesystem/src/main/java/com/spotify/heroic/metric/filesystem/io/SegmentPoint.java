package com.spotify.heroic.metric.filesystem.io;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;
import lombok.EqualsAndHashCode;

@AutoSerialize
@Data
@EqualsAndHashCode(of = "timestamp")
public class SegmentPoint implements Comparable<SegmentPoint> {
    private final long timestamp;
    private final double value;

    @Override
    public int compareTo(final SegmentPoint o) {
        return Long.compare(timestamp, o.timestamp);
    }
}
