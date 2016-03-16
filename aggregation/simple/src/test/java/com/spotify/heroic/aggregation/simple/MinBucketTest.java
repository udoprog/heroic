package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.metric.Point;
import org.junit.Assert;
import org.junit.Test;

public class MinBucketTest {
    @Test
    public void testInitialValue() {
        final StripedMinBucket b = new StripedMinBucket(0);
        Assert.assertEquals(Double.NaN, b.value(), 0.0);
    }

    @Test
    public void testMinValues() {
        final StripedMinBucket b = new StripedMinBucket(0);
        b.collectPoint(new Point(0, 10.0));
        b.collectPoint(new Point(0, 20.0));
        Assert.assertEquals(10.0, b.value(), 0.0);
    }
}
