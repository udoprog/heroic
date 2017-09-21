package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.function.DoubleBinaryOperator;

public class MinBucketIntegrationTest extends ValueBucketIntegrationTest {
    public MinBucketIntegrationTest() {
        super(Double.POSITIVE_INFINITY, new DoubleBinaryOperator() {
            @Override
            public double applyAsDouble(double left, double right) {
                return Math.min(left, right);
            }
        });
    }

    @Override
    public Collection<PointBucket> buckets() {
        return ImmutableList.<PointBucket>of(new MinBucket(0L), new StripedMinBucket(0L));
    }
}
