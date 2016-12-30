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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.Series;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A collection of metrics.
 * <p>
 * Metrics are constrained to the implemented types below, so far these are {@link Point}, {@link
 * Event}, {@link Spread} , and {@link MetricGroup}.
 * <p>
 * There is a JSON serialization available in {@link com.spotify.heroic.metric.MetricCollection}
 * which correctly preserves the type information of these collections.
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
public interface MetricCollection {
    /**
     * Helper method to fetch a collection of the given type, if applicable.
     *
     * @param expected the expected type
     * @return a list of the expected type or {@link java.util.Optional#empty()}
     * @deprecated use {@link #getDataUnsafe()} (for pre-checked)
     */
    default <T> List<T> getDataAs(final Class<T> expected) {
        return getDataOptional(expected).orElseThrow(() -> new IllegalArgumentException(
            "expected (" + expected.getCanonicalName() + ") but is (" + this.getType() + ")"));
    }

    /**
     * Get the type of the collection.
     *
     * @return type of the collection
     */
    MetricType getType();

    /**
     * Get size of the underlying collection.
     *
     * @return a long
     */
    default long size() {
        return getData().size();
    }

    /**
     * Get the underlying data of the collection.
     * <p>
     * This is an unsafe operation and must always be preceded by a type check.
     *
     * @return data of the collection
     */
    @SuppressWarnings("unchecked")
    default <T> List<T> getDataUnsafe() {
        return (List<T>) getData();
    }

    /**
     * Safely access the underlying data of the collection.
     *
     * @param type class of type to expect
     * @param <T> type to expect
     * @return an optional containing the data if the underlying type matches, {@link
     * java.util.Optional#empty()} otherwise
     */
    <T> Optional<List<T>> getDataOptional(Class<T> type);

    /**
     * Get the underlying data of the collection.
     *
     * @return data of the collection
     */
    List<? extends Metric> getData();

    /**
     * Check if the collection is empty.
     *
     * @return {@code true} if the collection is empty
     */
    boolean isEmpty();

    /**
     * Update the given aggregation with the content of this collection.
     */
    void updateAggregation(
        AggregationSession session, Map<String, String> tags, Set<Series> series
    );

    static MetricCollection empty() {
        return points(ImmutableList.of());
    }

    static MetricCollection groups(List<MetricGroup> metrics) {
        return new MetricGroups(metrics);
    }

    static MetricCollection points(List<Point> metrics) {
        return new Points(metrics);
    }

    static MetricCollection events(List<Event> metrics) {
        return new Events(metrics);
    }

    static MetricCollection spreads(List<Spread> metrics) {
        return new Spreads(metrics);
    }

    static MetricCollection cardinality(List<Payload> metrics) {
        return new Cardinalities(metrics);
    }

    static MetricCollection mergeSorted(
        final MetricType type, final List<List<? extends Metric>> values
    ) {
        final List<Metric> data = ImmutableList.copyOf(Iterators.mergeSorted(
            ImmutableList.copyOf(values.stream().map(Iterable::iterator).iterator()),
            Metric.comparator()));
        return build(type, data);
    }

    @SuppressWarnings("unchecked")
    static MetricCollection build(final MetricType type, List<? extends Metric> data) {
        switch (type) {
            case POINT:
                return new Points((List<Point>) data);
            case EVENT:
                return new Events((List<Event>) data);
            case SPREAD:
                return new Spreads((List<Spread>) data);
            case GROUP:
                return new MetricGroups((List<MetricGroup>) data);
            case CARDINALITY:
                return new Cardinalities((List<Payload>) data);
            default:
                throw new IllegalArgumentException("type");
        }
    }

    @Data
    class Points implements MetricCollection {
        final List<Point> data;

        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<List<T>> getDataOptional(final Class<T> expected) {
            if (expected != Point.class) {
                return Optional.empty();
            }

            return Optional.of((List<T>) data);
        }

        @Override
        public MetricType getType() {
            return MetricType.POINT;
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public void updateAggregation(
            final AggregationSession session, final Map<String, String> tags,
            final Set<Series> series
        ) {
            session.updatePoints(tags, series, data);
        }
    }

    @Data
    class Events implements MetricCollection {
        final List<Event> data;

        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<List<T>> getDataOptional(final Class<T> expected) {
            if (expected != Event.class) {
                return Optional.empty();
            }

            return Optional.of((List<T>) data);
        }

        @Override
        public MetricType getType() {
            return MetricType.EVENT;
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public void updateAggregation(
            final AggregationSession session, final Map<String, String> tags,
            final Set<Series> series
        ) {
            session.updateEvents(tags, series, data);
        }
    }

    @Data
    class Spreads implements MetricCollection {
        final List<Spread> data;

        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<List<T>> getDataOptional(final Class<T> expected) {
            if (expected != Spread.class) {
                return Optional.empty();
            }

            return Optional.of((List<T>) data);
        }

        @Override
        public MetricType getType() {
            return MetricType.SPREAD;
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public void updateAggregation(
            final AggregationSession session, final Map<String, String> tags,
            final Set<Series> series
        ) {
            session.updateSpreads(tags, series, data);
        }
    }

    @Data
    class MetricGroups implements MetricCollection {
        final List<MetricGroup> data;

        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<List<T>> getDataOptional(final Class<T> expected) {
            if (expected != MetricGroup.class) {
                return Optional.empty();
            }

            return Optional.of((List<T>) data);
        }

        @Override
        public MetricType getType() {
            return MetricType.GROUP;
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public void updateAggregation(
            final AggregationSession session, final Map<String, String> tags,
            final Set<Series> series
        ) {
            session.updateGroup(tags, series, data);
        }
    }

    @Data
    class Cardinalities implements MetricCollection {
        final List<Payload> data;

        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<List<T>> getDataOptional(final Class<T> expected) {
            if (expected != Payload.class) {
                return Optional.empty();
            }

            return Optional.of((List<T>) data);
        }

        @Override
        public MetricType getType() {
            return MetricType.CARDINALITY;
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public void updateAggregation(
            final AggregationSession session, final Map<String, String> tags,
            final Set<Series> series
        ) {
            session.updatePayload(tags, series, data);
        }
    }
}
