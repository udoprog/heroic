package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.metric.Point;
import org.junit.Assert;
import org.junit.Test;

public class MaxBucketTest {
    @Test
    public void testInitialValue() {
        final StripedMaxBucket b = new StripedMaxBucket(0);
        Assert.assertEquals(Double.NaN, b.value(), 0.0);
    }

    @Test
    public void testMinValues() {
        final StripedMaxBucket b = new StripedMaxBucket(0);
        b.collectPoint(new Point(0, 20.0));
        b.collectPoint(new Point(0, 10.0));
        Assert.assertEquals(20.0, b.value(), 0.0);
    }
}
