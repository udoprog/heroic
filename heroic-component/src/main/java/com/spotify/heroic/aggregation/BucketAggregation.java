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
import com.spotify.heroic.async.Observable;
import com.spotify.heroic.async.Observer;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import eu.toolchain.async.AsyncFuture;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
@Slf4j
@Data
@EqualsAndHashCode(of = {"size", "extent"})
public abstract class BucketAggregation<B extends Bucket> implements Aggregation {
    public static final Map<String, String> EMPTY_GROUP = ImmutableMap.of();
    public static final long MAX_BUCKET_COUNT = 100000L;

    private static final Map<String, String> EMPTY = ImmutableMap.of();

    public static final Set<MetricType> ALL_TYPES =
        ImmutableSet.of(MetricType.POINT, MetricType.EVENT, MetricType.SPREAD, MetricType.GROUP);

    protected final Optional<Duration> size;
    protected final Optional<Duration> extent;
    protected final Optional<Expression> reference;

    @Getter(AccessLevel.NONE)
    private final Set<MetricType> input;

    @Getter(AccessLevel.NONE)
    protected final MetricType out;

    @Override
    public boolean referential() {
        return reference.isPresent();
    }

    @Override
    public AsyncFuture<AggregationContext> setup(final AggregationContext input) {
        return input.lookupContext(reference).directTransform(context -> {
            final DateRange range = context.range();

            final Duration size = Optionals
                .firstPresent(this.size, context.size())
                .orElseThrow(() -> new IllegalStateException("No size available"));

            final long sizeMillis = size.toMilliseconds();
            final long extentMillis = this.extent.map(Duration::toMilliseconds).orElse(sizeMillis);

            final List<Iterable<Series>> series = new ArrayList<>();
            final List<Observable<MetricCollection>> observables = new ArrayList<>();

            for (final AggregationState s : context.states()) {
                series.add(s.getSeries());
                observables.add(s.getObservable());
            }

            final AggregationState state =
                new AggregationState(EMPTY, Iterables.concat(series), observer -> {
                    final List<B> buckets = buildBuckets(range, sizeMillis);
                    final Session s =
                        new Session(buckets, range.start(), Iterables.concat(series), sizeMillis,
                            extentMillis);

                    Observable.concurrently(observables).observe(new Observer<MetricCollection>() {
                        @Override
                        public void observe(final MetricCollection value) throws Exception {
                            value.update(s);
                        }

                        @Override
                        public void fail(final Throwable cause) throws Exception {
                            observer.fail(cause);
                        }

                        @Override
                        public void end() throws Exception {
                            observer.observe(s.result());
                            observer.end();
                        }
                    });
                });

            final List<AggregationState> states = ImmutableList.of(state);

            final Optional<Long> estimate = sizeMillis <= 0 ? Optional.empty()
                : Optional.of(range.rounded(sizeMillis).diff() / sizeMillis);

            return context
                .withStep(getClass().getSimpleName(), ImmutableList.of(context.step()),
                    context.step().keys())
                .withSize(size)
                .withStates(states)
                .withEstimate(estimate);
        });
    }

    protected abstract B buildBucket(long timestamp);

    protected abstract Metric build(B bucket);

    private List<B> buildBuckets(final DateRange range, long sizeMillis) {
        final long start = range.start();
        final long count = (range.diff() + sizeMillis) / sizeMillis;

        if (count < 1 || count > MAX_BUCKET_COUNT) {
            throw new IllegalArgumentException(
                String.format("range %s, size %d", range, sizeMillis));
        }

        final List<B> buckets = new ArrayList<>((int) count);

        for (int i = 0; i < count; i++) {
            buckets.add(buildBucket(start + sizeMillis * i));
        }

        return buckets;
    }

    private interface BucketConsumer<B extends Bucket, M extends Metric> {
        void apply(B bucket, M metric);
    }

    @Data
    private final class Session implements MetricsCollector {
        private final LongAdder sampleSize = new LongAdder();
        private final List<B> buckets;
        private final long offset;
        private final Iterable<Series> series;

        protected final long sizeMillis;
        protected final long extentMillis;

        @Override
        public void collectPoints(List<Point> values) {
            feed(MetricType.POINT, values, (bucket, m) -> bucket.collectPoint(m));
        }

        @Override
        public void collectEvents(List<Event> values) {
            feed(MetricType.EVENT, values, (bucket, m) -> bucket.collectEvent(m));
        }

        @Override
        public void collectSpreads(List<Spread> values) {
            feed(MetricType.SPREAD, values, (bucket, m) -> bucket.collectSpread(m));
        }

        @Override
        public void collectGroups(List<MetricGroup> values) {
            feed(MetricType.GROUP, values, (bucket, m) -> bucket.collectGroup(m));
        }

        private <T extends Metric> void feed(
            final MetricType type, List<T> values, final BucketConsumer<B, T> consumer
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
            final long te = ts + extentMillis;

            if (te < 0) {
                return Collections.emptyIterator();
            }

            // iterator that iterates from the largest to the smallest matching bucket for _this_
            // metric.
            return new Iterator<B>() {
                long current = te;

                @Override
                public boolean hasNext() {
                    while ((current / sizeMillis) >= buckets.size()) {
                        current -= sizeMillis;
                    }

                    final long m = current % sizeMillis;
                    return (current >= 0 && current > ts) && (m >= 0 && m < extentMillis);
                }

                @Override
                public B next() {
                    final int index = (int) (current / sizeMillis);
                    current -= sizeMillis;
                    return buckets.get(index);
                }
            };
        }

        public MetricCollection result() {
            final List<Metric> result = new ArrayList<>(buckets.size());

            for (final B bucket : buckets) {
                final Metric d = build(bucket);

                if (!d.valid()) {
                    continue;
                }

                result.add(d);
            }

            return MetricCollection.build(out, result);
        }
    }
}
