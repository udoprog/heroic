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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.spotify.heroic.aggregation.MetricCollector;
import com.spotify.heroic.aggregation.MetricsCollector;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A collection of metrics.
 * <p>
 * Metrics are constrained to the implemented types below, so far these are {@link Point}, {@link
 * Event}, {@link Spread} , and {@link MetricGroup}.
 * <p>
 * There is a JSON serialization which correctly preserves the type information of these
 * collections.
 * <p>
 * This class is a carrier for _any_ of these metrics, the canonical way for accessing the
 * underlying data is to first check it's type using {@link #getType()}, and then access the data
 * with the appropriate cast using {@link #getDataAs(Class)}.
 * <p>
 * The following is an example for how you may access data from the collection.
 * <p>
 * <pre>
 * final MetricCollection collection = ...;
 *
 * if (collection.getType() == MetricType.POINT) {
 *     final List<Point> points = collection.getDataAs(Point.class);
 *     ...
 * }
 * </pre>
 *
 * @author udoprog
 * @see Point
 * @see Spread
 * @see Event
 * @see MetricGroup
 */
@Data
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public abstract class MetricCollection {
    final MetricType type;
    final List<? extends Metric> data;

    /**
     * Helper method to fetch a collection of the given type, if applicable.
     *
     * @param expected The expected type to read.
     * @return A list of the expected type.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getDataAs(Class<T> expected) {
        if (!expected.isAssignableFrom(type.type())) {
            throw new IllegalArgumentException(
                String.format("Cannot assign type (%s) to expected (%s)", type, expected));
        }

        return (List<T>) data;
    }

    /**
     * Update the given aggregation with the content of this collection.
     */
    public abstract void update(final MetricsCollector collector);

    public abstract void update(final MetricCollector collector);

    public abstract MetricCollection apply(
        final MetricCollectionBiFunction function, final MetricCollection right
    );

    public abstract <T> T apply(final MetricCollectionFunction<T> function);

    public int size() {
        return data.size();
    }

    @JsonIgnore
    public boolean isEmpty() {
        return data.isEmpty();
    }

    private static final MetricCollection empty = new EmptyMetricCollection();

    public static MetricCollection empty() {
        return empty;
    }

    // @formatter:off
    private static final
    Map<MetricType, Function<List<? extends Metric>, MetricCollection>> adapters = ImmutableMap.of(
        MetricType.GROUP, GroupCollection::new,
        MetricType.POINT, PointCollection::new,
        MetricType.EVENT, EventCollection::new,
        MetricType.SPREAD, SpreadCollection::new
    );
    // @formatter:on

    public static MetricCollection groups(List<MetricGroup> metrics) {
        return new GroupCollection(metrics);
    }

    public static MetricCollection points(List<Point> metrics) {
        return new PointCollection(metrics);
    }

    public static MetricCollection events(List<Event> metrics) {
        return new EventCollection(metrics);
    }

    public static MetricCollection spreads(List<Spread> metrics) {
        return new SpreadCollection(metrics);
    }

    public static MetricCollection build(
        final MetricType key, final List<? extends Metric> metrics
    ) {
        final Function<List<? extends Metric>, MetricCollection> adapter =
            checkNotNull(adapters.get(key), "applier does not exist for type");
        return adapter.apply(metrics);
    }

    public static MetricCollection mergeSorted(
        final MetricType type, final List<List<? extends Metric>> values
    ) {
        final List<Metric> data = ImmutableList.copyOf(Iterators.mergeSorted(
            ImmutableList.copyOf(values.stream().map(Iterable::iterator).iterator()),
            type.comparator()));
        return build(type, data);
    }

    @SuppressWarnings("unchecked")
    private static class PointCollection extends MetricCollection {
        PointCollection(List<? extends Metric> points) {
            super(MetricType.POINT, points);
        }

        @Override
        public void update(MetricsCollector collector) {
            collector.collectPoints(adapt());
        }

        @Override
        public void update(MetricCollector collector) {
            adapt().forEach(collector::collectPoint);
        }

        @Override
        public MetricCollection apply(
            final MetricCollectionBiFunction function, final MetricCollection right
        ) {
            return new PointCollection(function.applyPoints(adapt(), right.getDataAs(Point.class)));
        }

        @Override
        public <T> T apply(final MetricCollectionFunction<T> function) {
            return function.applyPoints(adapt());
        }

        private List<Point> adapt() {
            return (List<Point>) data;
        }
    }

    @SuppressWarnings("unchecked")
    private static class EventCollection extends MetricCollection {
        EventCollection(List<? extends Metric> events) {
            super(MetricType.EVENT, events);
        }

        @Override
        public void update(MetricsCollector collector) {
            collector.collectEvents(adapt());
        }

        @Override
        public void update(MetricCollector collector) {
            adapt().forEach(collector::collectEvent);
        }

        @Override
        public MetricCollection apply(
            final MetricCollectionBiFunction function, final MetricCollection right
        ) {
            return new EventCollection(function.applyEvents(adapt(), right.getDataAs(Event.class)));
        }

        @Override
        public <T> T apply(final MetricCollectionFunction<T> function) {
            return function.applyEvents(adapt());
        }

        private List<Event> adapt() {
            return (List<Event>) data;
        }
    }

    @SuppressWarnings("unchecked")
    private static class SpreadCollection extends MetricCollection {
        SpreadCollection(List<? extends Metric> spread) {
            super(MetricType.SPREAD, spread);
        }

        @Override
        public void update(MetricsCollector collector) {
            collector.collectSpreads(adapt());
        }

        @Override
        public void update(MetricCollector collector) {
            adapt().forEach(collector::collectSpread);
        }

        @Override
        public MetricCollection apply(
            final MetricCollectionBiFunction function, final MetricCollection right
        ) {
            return new SpreadCollection(
                function.applySpreads(adapt(), right.getDataAs(Spread.class)));
        }

        @Override
        public <T> T apply(final MetricCollectionFunction<T> function) {
            return function.applySpreads(adapt());
        }

        private List<Spread> adapt() {
            return (List<Spread>) data;
        }
    }

    @SuppressWarnings("unchecked")
    private static class GroupCollection extends MetricCollection {
        GroupCollection(List<? extends Metric> groups) {
            super(MetricType.GROUP, groups);
        }

        @Override
        public void update(MetricsCollector collector) {
            collector.collectGroups(adapt());
        }

        @Override
        public void update(MetricCollector collector) {
            adapt().forEach(collector::collectGroup);
        }

        @Override
        public MetricCollection apply(
            final MetricCollectionBiFunction function, final MetricCollection right
        ) {
            return new GroupCollection(
                function.applyGroups(adapt(), right.getDataAs(MetricGroup.class)));
        }

        @Override
        public <T> T apply(final MetricCollectionFunction<T> function) {
            return function.applyGroups(adapt());
        }

        private List<MetricGroup> adapt() {
            return (List<MetricGroup>) data;
        }
    }
}
