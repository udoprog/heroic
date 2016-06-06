package com.spotify.heroic.aggregation.cardinality;

public class HyperLogLogCardinalityBucketTest extends AbstractCardinalityBucketTest {
    @Override
    protected double allowedError() {
        return 0.02D;
    }

    @Override
    protected CardinalityBucket setupBucket(long timestamp) {
        return new HyperLogLogCardinalityBucket(42, 0.01D);
    }
}
