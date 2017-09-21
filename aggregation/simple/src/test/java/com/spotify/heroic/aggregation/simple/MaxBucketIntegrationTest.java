package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.function.DoubleBinaryOperator;

public class MaxBucketIntegrationTest extends ValueBucketIntegrationTest {
    public MaxBucketIntegrationTest() {
        super(Double.NEGATIVE_INFINITY, new DoubleBinaryOperator() {
            @Override
            public double applyAsDouble(double left, double right) {
                return Math.max(left, right);
            }
        });
    }

    @Override
    public Collection<PointBucket> buckets() {
        return ImmutableList.<PointBucket>of(new MaxBucket(0L), new StripedMaxBucket(0L));
    }
}
