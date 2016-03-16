package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;
import org.junit.Test;

import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StdDevBucketTest {
    public Collection<? extends DoubleBucket> buckets() {
        return ImmutableList.<DoubleBucket>of(new StdDevBucket(0L), new StripedStdDevBucket(0L));
    }

    @Test
    public void testExpectedValues() {
        final Random rnd = new Random();

        for (final DoubleBucket bucket : buckets()) {
            for (int i = 0; i < 1000; i++) {
                bucket.collectPoint(new Point(0L, rnd.nextDouble()));
            }

            final double value = bucket.value();
            assertTrue(bucket.getClass().getSimpleName(), value <= 1.0d && value >= 0.0d);
        }
    }

    @Test
    public void testNaNOnZero() {
        for (final DoubleBucket bucket : buckets()) {
            assertTrue(Double.isNaN(bucket.value()));
        }

        for (final DoubleBucket bucket : buckets()) {
            bucket.collectPoint(new Point(0L, 0.0d));
            bucket.collectPoint(new Point(0L, 0.0d));
            assertFalse(bucket.getClass().getSimpleName(), Double.isNaN(bucket.value()));
        }
    }
}
