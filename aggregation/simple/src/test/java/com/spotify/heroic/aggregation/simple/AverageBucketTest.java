package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AverageBucketTest {
    public Collection<? extends DoubleBucket> buckets() {
        return ImmutableList.<DoubleBucket>of(new AverageBucket(0L), new StripedAverageBucket(0L));
    }

    @Test
    public void testZeroValue() {
        for (final DoubleBucket bucket : buckets()) {
            assertTrue(Double.isNaN(bucket.value()));
        }
    }

    @Test
    public void testAddSome() {
        for (final DoubleBucket bucket : buckets()) {
            bucket.collectPoint(new Point(0, 10.0));
            bucket.collectPoint(new Point(0, 20.0));
            assertEquals(bucket.getClass().getSimpleName(), 15.0, bucket.value(), 0.0);
        }
    }
}
