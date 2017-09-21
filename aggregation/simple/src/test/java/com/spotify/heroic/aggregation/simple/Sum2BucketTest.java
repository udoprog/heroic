package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.Point;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Sum2BucketTest {
    private static final Map<String, String> tags = ImmutableMap.of();

    public Collection<? extends PointBucket> buckets() {
        return ImmutableList.<PointBucket>of(new Sum2Bucket(0L), new StripedSum2Bucket(0L));
    }

    @Test
    public void testZeroValue() {
        for (final PointBucket bucket : buckets()) {
            assertTrue(bucket.getClass().getSimpleName(), Double.isNaN(bucket.value()));
        }
    }

    @Test
    public void testAddSome() {
        for (final PointBucket bucket : buckets()) {
            bucket.updatePoint(tags, new Point(0, 10.0));
            bucket.updatePoint(tags, new Point(0, 20.0));
            assertEquals(bucket.getClass().getSimpleName(), 500.0, bucket.value(), 0.0);
        }
    }
}
