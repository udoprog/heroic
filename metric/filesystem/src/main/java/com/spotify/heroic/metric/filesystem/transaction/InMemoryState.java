package com.spotify.heroic.metric.filesystem.transaction;

import java.util.Map;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class InMemoryState {
    public final SegmentInMemory<Double> points;
    public final SegmentInMemory<Map<String, String>> events;
}
