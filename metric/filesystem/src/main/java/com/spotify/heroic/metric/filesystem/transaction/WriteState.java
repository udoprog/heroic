package com.spotify.heroic.metric.filesystem.transaction;

import com.spotify.heroic.metric.filesystem.io.SegmentEvent;
import com.spotify.heroic.metric.filesystem.io.SegmentPoint;
import com.spotify.heroic.metric.filesystem.wal.Wal;
import java.util.Map;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class WriteState {
    public final SegmentSnapshot<SegmentPoint, Double> points;
    public final SegmentSnapshot<SegmentEvent, Map<String, String>> events;
    public long lastTxId = Wal.DISABLED_TXID;
}
