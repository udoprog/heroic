package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

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
        final BucketAggregationInstance<TestBucket> a = setup(10, 10);
        final AggregationSession session = a.session(new DateRange(10, 30));

        session.updatePoints(group, series, build()
            .add(9, 1000.0)
            .add(10, 1.0)
            .add(19, 2.0)
            .add(20, 3.0)
            .add(29, 4.0)
            .add(30, 1000.0)
            .result());

        final AggregationResult result = session.result();

        Assert.assertEquals(build().add(10, 3.0).add(20, 7.0).result(),
            result.getResult().get(0).getMetrics().getData());
    }

    @Test
    public void testLongerExtent() {
        final BucketAggregationInstance<TestBucket> a = setup(10, 20);
        final AggregationSession session = a.session(new DateRange(10, 30));

        session.updatePoints(group, series, build()
            .add(9, 1000.0)
            .add(10, 1.0)
            .add(20, 2.0)
            .add(29, 3.0)
            .add(30, 4.0)
            .add(39, 5.0)
            .add(40, 1000.0)
            .result());

        final AggregationResult result = session.result();

        Assert.assertEquals(build().add(10, 6.0).add(20, 14.0).result(),
            result.getResult().get(0).getMetrics().getData());
    }

    @Test
    public void testShorterExtent() {
        final BucketAggregationInstance<TestBucket> a = setup(10, 5);
        final AggregationSession session = a.session(new DateRange(10, 40));

        session.updatePoints(group, series, build()
            .add(9, 1000.0)
            .add(10, 1.0)
            .add(14, 1.0)
            .add(15, 1000.0)
            .add(19, 1000.0)
            .add(20, 1.0)
            .add(21, 1.0)
            .add(24, 1.0)
            .add(25, 1000.0)
            .add(29, 1000.0)
            .add(30, 1.0)
            .add(31, 1.0)
            .add(32, 1.0)
            .add(34, 1.0)
            .add(35, 1000.0)
            .result());

        final AggregationResult result = session.result();

        Assert.assertEquals(build().add(10, 2.0).add(20, 3.0).add(30, 4.0).result(),
            result.getResult().get(0).getMetrics().getData());
    }

    @Test
    public void testUnevenSampling() {
        final BucketAggregationInstance<TestBucket> a = setup(10, 15);
        final AggregationSession session = a.session(new DateRange(10, 40));

        session.updatePoints(group, series, build()
            .add(9, 1000.0)
            .add(10, 1.0)
            .add(20, 2.0)
            .add(24, 3.0)
            .add(30, 4.0)
            .add(34, 5.0)
            .add(44, 6.0)
            .add(45, 1000.0)
            .result());

        final AggregationResult result = session.result();

        Assert.assertEquals(build().add(10, 6.0).add(20, 14.0).add(30, 15.0).result(),
            result.getResult().get(0).getMetrics().getData());
    }
}
