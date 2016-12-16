/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.CompositeCollection;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

/**
 * A base aggregation that collects data in 'buckets', one for each sampled data point.
 * <p>
 * A bucket aggregation is used to down-sample a lot of data into distinct buckets over time, making
 * them useful for presentation purposes. Buckets have to be thread safe.
 *
 * @param <B> The bucket type.
 * @author udoprog
 * @see Bucket
 */
@Data
@EqualsAndHashCode(of = {"size", "extent"})
public abstract class BucketAggregationInstance<B extends Bucket> implements AggregationInstance {
    public static final Map<String, String> EMPTY_KEY = ImmutableMap.of();
    public static final long MAX_BUCKET_COUNT = 100000L;

    private static final Map<String, String> EMPTY = ImmutableMap.of();

    public static final Set<MetricType> ALL_TYPES =
        ImmutableSet.of(MetricType.POINT, MetricType.EVENT, MetricType.SPREAD, MetricType.GROUP,
            MetricType.CARDINALITY);

    protected final long size;
    protected final long extent;

    @Getter(AccessLevel.NONE)
    private final Set<MetricType> input;

    @Getter(AccessLevel.NONE)
    protected final MetricType out;

    @Data
    private final class Session implements AggregationSession {
        private final ConcurrentLinkedQueue<Set<Series>> series = new ConcurrentLinkedQueue<>();
        private final LongAdder sampleSize = new LongAdder();

        private final List<B> buckets;
        private final long offset;

        @Override
        public void updatePoints(
            Map<String, String> key, Set<Series> s, Iterable<Point> values, long size
        ) {
            series.add(s);
            feed(MetricType.POINT, values, (bucket, m) -> bucket.updatePoint(key, m));
        }

        @Override
        public void updateEvents(
            Map<String, String> key, Set<Series> s, Iterable<Event> values, long size
        ) {
            series.add(s);
            feed(MetricType.EVENT, values, (bucket, m) -> bucket.updateEvent(key, m));
        }

        @Override
        public void updateSpreads(
            Map<String, String> key, Set<Series> s, Iterable<Spread> values, long size
        ) {
            series.add(s);
            feed(MetricType.SPREAD, values, (bucket, m) -> bucket.updateSpread(key, m));
        }

        @Override
        public void updateGroup(
            Map<String, String> key, Set<Series> s, Iterable<MetricGroup> values, long size
        ) {
            series.add(s);
            feed(MetricType.GROUP, values, (bucket, m) -> bucket.updateGroup(key, m));
        }

        @Override
        public void updatePayload(
            Map<String, String> key, Set<Series> s, Iterable<Payload> values, long size
        ) {
            series.add(s);
            feed(MetricType.CARDINALITY, values, (bucket, m) -> bucket.updatePayload(key, m));
        }

        private <T extends Metric> void feed(
            final MetricType type, Iterable<T> values, final BucketConsumer<B, T> consumer
        ) {
            if (!input.contains(type)) {
                return;
            }

            int sampleSize = 0;

            for (final T m : values) {
                if (!m.valid()) {
                    continue;
                }

                final Iterator<B> buckets = matching(m);

                while (buckets.hasNext()) {
                    consumer.apply(buckets.next(), m);
                }

                sampleSize += 1;
            }

            this.sampleSize.add(sampleSize);
        }

        private Iterator<B> matching(final Metric m) {
            final long ts = m.getTimestamp() - offset - 1;
            final long te = ts + extent;

            if (te < 0) {
                return Collections.emptyIterator();
            }

            // iterator that iterates from the largest to the smallest matching bucket for _this_
            // metric.
            return new Iterator<B>() {
                long current = te;

                @Override
                public boolean hasNext() {
                    while ((current / size) >= buckets.size()) {
                        current -= size;
                    }

                    final long m = current % size;
                    return (current >= 0 && current > ts) && (m >= 0 && m < extent);
                }

                @Override
                public B next() {
                    final int index = (int) (current / size);
                    current -= size;
                    return buckets.get(index);
                }
            };
        }

        @Override
        public AggregationResult result() {
            final List<Metric> result = new ArrayList<>(buckets.size());

            for (final B bucket : buckets) {
                final Metric d = build(bucket);

                if (!d.valid()) {
                    continue;
                }

                result.add(d);
            }

            final Set<Series> series = ImmutableSet.copyOf(Iterables.concat(this.series));
            final CompositeCollection metrics = CompositeCollection.build(out, result);

            final Statistics statistics =
                new Statistics(ImmutableMap.of(AggregationInstance.SAMPLE_SIZE, sampleSize.sum()));

            final AggregationOutput d = new AggregationOutput(EMPTY_KEY, series, metrics);
            return new AggregationResult(ImmutableList.of(d), statistics);
        }
    }

    @Override
    public long estimate(DateRange original) {
        if (size == 0) {
            return 0;
        }

        return original.rounded(size).diff() / size;
    }

    @Override
    public AggregationSession session(DateRange range) {
        final List<B> buckets = buildBuckets(range, size);
        return new Session(buckets, range.start());
    }

    @Override
    public AggregationInstance distributed() {
        return this;
    }

    @Override
    public long cadence() {
        return size;
    }

    @Override
    public String toString() {
        return String.format("%s(size=%d, extent=%d)", getClass().getSimpleName(), size, extent);
    }

    private List<B> buildBuckets(final DateRange range, long size) {
        final long start = range.start();
        final long count = (range.diff() + size) / size;

        if (count < 1 || count > MAX_BUCKET_COUNT) {
            throw new IllegalArgumentException(String.format("range %s, size %d", range, size));
        }

        final List<B> buckets = new ArrayList<>((int) count);

        for (int i = 0; i < count; i++) {
            buckets.add(buildBucket(start + size * i));
        }

        return buckets;
    }

    protected abstract B buildBucket(long timestamp);

    protected abstract Metric build(B bucket);

    private interface BucketConsumer<B extends Bucket, M extends Metric> {
        void apply(B bucket, M metric);
    }
}
