package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BucketAggregationTest {
    public final class IterableBuilder {
        final ArrayList<Point> datapoints = new ArrayList<Point>();

        public IterableBuilder add(long timestamp, double value) {
            datapoints.add(new Point(timestamp, value));
            return this;
        }

        public List<Point> result() {
            return datapoints;
        }
    }

    public IterableBuilder build() {
        return new IterableBuilder();
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class TestBucket extends AbstractBucket {
        private final long timestamp;
        private double sum;

        public void updatePoint(Map<String, String> key, Point d) {
            sum += d.getValue();
        }

        @Override
        public long timestamp() {
            return timestamp;
        }
    }

    public BucketAggregationInstance<TestBucket> setup(long sampling, long extent) {
        return new BucketAggregationInstance<TestBucket>(sampling, extent,
            ImmutableSet.of(MetricType.POINT), MetricType.POINT) {
            @Override
            protected TestBucket buildBucket(long timestamp) {
                return new TestBucket(timestamp);
            }

            @Override
            protected Point build(TestBucket bucket) {
                return new Point(bucket.timestamp, bucket.sum);
            }

            @Override
            public AggregationInstance distributed() {
                return this;
            }

            @Override
            public AggregationInstance reducer() {
                return Mockito.mock(AggregationInstance.class);
            }
        };
    }

    final Map<String, String> group = ImmutableMap.of();
    final Set<Series> series = ImmutableSet.of();

    @Test
    public void testSameSampling() {
        List<Point> input = build().add(999, 1.0).add(1000, 1.0).add(2000, 1.0).result();
        List<Point> expected = build().add(1000, 2.0).add(2000, 1.0).add(3000, 0.0).result();
        checkBucketAggregation(input, expected, 1000);
    }

    @Test
    public void testLongerExtent() {
        List<Point> input =
            build().add(0, 1.0).add(1000, 1.0).add(1000, 1.0).add(2000, 1.0).result();
        List<Point> expected = build().add(1000, 3.0).add(2000, 3.0).add(3000, 1.0).result();
        checkBucketAggregation(input, expected, 2000);
    }

    @Test
    public void testShorterExtent() {
        final List<Point> input =
            build().add(1500, 1.0).add(1501, 1.0).add(2000, 1.0).add(2001, 1.0).result();
        final List<Point> expected = build().add(1000, 0.0).add(2000, 2.0).add(3000, 0.0).result();
        checkBucketAggregation(input, expected, 500);
    }

    private void checkBucketAggregation(
        List<Point> input, List<Point> expected, final long extent
    ) {
        final BucketAggregationInstance<TestBucket> a = setup(1000, extent);
        final AggregationSession session = a.session(new DateRange(1000, 3000));
        session.updatePoints(group, series, input);

        final AggregationResult result = session.result();

        Assert.assertEquals(expected, result.getResult().get(0).getMetrics().data());
    }

    @Test
    public void testUnevenSampling() {
        final BucketAggregationInstance<TestBucket> a = setup(999, 499);
        final AggregationSession session = a.session(new DateRange(1000, 2998));
        session.updatePoints(group, series,
            build().add(501, 1.0).add(502, 1.0).add(1000, 1.0).add(1001, 1.0).result());

        final AggregationResult result = session.result();

        Assert.assertEquals(build().add(1000, 2.0).add(1999, 0.0).add(2998, 0.0).result(),
            result.getResult().get(0).getMetrics().data());
    }
}
