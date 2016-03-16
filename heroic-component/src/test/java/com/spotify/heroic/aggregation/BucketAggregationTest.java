package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

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

        @Override
        public void collectPoint(Point d) {
            sum += d.getValue();
        }

        @Override
        public long timestamp() {
            return timestamp;
        }
    }

    public BucketAggregation<TestBucket> setup(final long size, final long extent) {
        return new BucketAggregation<TestBucket>(
            Optional.of(Duration.of(size, TimeUnit.MILLISECONDS)),
            Optional.of(Duration.of(extent, TimeUnit.MILLISECONDS)), Optional.empty(),
            ImmutableSet.of(MetricType.POINT), MetricType.POINT) {
            @Override
            protected TestBucket buildBucket(long timestamp) {
                return new TestBucket(timestamp);
            }

            @Override
            protected Point build(TestBucket bucket) {
                return new Point(bucket.timestamp, bucket.sum);
            }
        };
    }

    final Map<String, String> group = ImmutableMap.of();
    final List<Series> series = ImmutableList.of();

    @Test
    public void testSameSampling() throws Exception {
        final DateRange range = new DateRange(1000, 3000);
        final List<Point> input =
            build().add(0, 1.0).add(1, 1.0).add(1000, 1.0).add(2000, 1.0).result();
        final List<Point> expected = build().add(1000, 2.0).add(2000, 1.0).add(3000, 0.0).result();
        checkBucketAggregation(input, expected, range, 1000, 1000);
    }

    @Test
    public void testLongerExtent() throws Exception {
        final DateRange range = new DateRange(1000, 3000);
        final List<Point> input =
            build().add(0, 1.0).add(1000, 1.0).add(1000, 1.0).add(2000, 1.0).result();
        final List<Point> expected = build().add(1000, 3.0).add(2000, 3.0).add(3000, 1.0).result();
        checkBucketAggregation(input, expected, range, 1000, 2000);
    }

    @Test
    public void testShorterExtent() throws Exception {
        final DateRange range = new DateRange(1000, 3000);
        final List<Point> input =
            build().add(1500, 1.0).add(1501, 1.0).add(2000, 1.0).add(2001, 1.0).result();
        final List<Point> expected = build().add(1000, 0.0).add(2000, 2.0).add(3000, 0.0).result();
        checkBucketAggregation(input, expected, range, 1000, 500);
    }

    @Test
    public void testUnevenSampling() throws Exception {
        final DateRange range = new DateRange(1000, 3000);
        final List<Point> input =
            build().add(501, 1.0).add(502, 1.0).add(1000, 1.0).add(1001, 1.0).result();
        final List<Point> expected = build().add(1000, 2.0).add(1999, 0.0).add(2998, 0.0).result();
        checkBucketAggregation(input, expected, range, 999, 499);
    }

    private void checkBucketAggregation(
        List<Point> input, List<Point> expected, final DateRange range, final long size,
        final long extent
    ) throws Exception {
        final BucketAggregation<TestBucket> a = setup(size, extent);

        final AggregationState state = new AggregationState(group, series,
            Observable.ofValues(MetricCollection.points(input)));

        AggregationUtils.RunResult r = AggregationUtils.run(a, ImmutableList.of(state), range);

        r.getError().ifPresent(e -> {
            throw new RuntimeException("aggregation failed", e);
        });

        assertEquals(1, r.getResults().size());
        assertEquals(expected, r.getResults().get(0).getData());
    }
}
