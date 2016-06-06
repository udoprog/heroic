package com.spotify.heroic.aggregation.cardinality;

public class ExactCardinalityBucketTest extends AbstractCardinalityBucketTest {
    @Override
    protected double allowedError() {
        return 0D;
    }

    @Override
    protected CardinalityBucket setupBucket(long timestamp) {
        return new ExactCardinalityBucket(timestamp);
    }
}
