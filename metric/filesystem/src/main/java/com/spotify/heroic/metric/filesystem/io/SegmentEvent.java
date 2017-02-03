package com.spotify.heroic.metric.filesystem.io;

import eu.toolchain.serializer.AutoSerialize;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;

@AutoSerialize
@Data
@EqualsAndHashCode(of = "timestamp")
public class SegmentEvent implements Comparable<SegmentEvent> {
    private final long timestamp;
    private final Map<String, String> payload;

    @Override
    public int compareTo(final SegmentEvent o) {
        return Long.compare(timestamp, o.timestamp);
    }
}
