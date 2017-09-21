package com.spotify.heroic.aggregation.simple;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.Metric;
import java.util.Map;
import org.junit.Test;

public class CountBucketTest {
    @Test
    public void testCount() {
        final Map<String, String> tags = ImmutableMap.of();
        final CountBucket b = new CountBucket(0);
        final Metric m = mock(Metric.class);
        assertEquals(0D, b.value(), 0.1D);
        b.update(tags, m);
        assertEquals(1D, b.value(), 0.1D);
    }
}
