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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.Series;
import lombok.RequiredArgsConstructor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * A collection of metrics that is a composition of multiple sources.
 *
 * These are not guaranteed to be stored in any particular order.
 *
 * <p>These collections to not generate additional internal copies for the data they are being
 * presented with. It is important that the collections being provided are not modified while this
 * is in scope.
 *
 * @author udoprog
 * @see Point
 * @see Spread
 * @see Event
 * @see MetricGroup
 */
public interface CompositeCollection extends BaseCollection {
    Ordering<Metric> ORDERING = Ordering.from(Metric.comparator);

    Iterable<? extends Metric> data();

    SortedCollection sorted();

    static CompositeCollection build(final MetricType type, final List<? extends Metric> metrics) {
        return build(type, metrics, metrics.size());
    }

    @SuppressWarnings("unchecked")
    static CompositeCollection build(
        final MetricType type, final Iterable<? extends Metric> metrics, final long size
    ) {
        switch (type) {
            case POINT:
                return new Points((Iterable<Point>) metrics, size);
            case EVENT:
                return new Events((Iterable<Event>) metrics, size);
            case SPREAD:
                return new Spreads((Iterable<Spread>) metrics, size);
            case GROUP:
                return new Groups((Iterable<MetricGroup>) metrics, size);
            case CARDINALITY:
                return new Cardinality((Iterable<Payload>) metrics, size);
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    static <T extends Metric> List<T> mergeSorted(final List<Iterable<T>> points) {
        final List<Iterator<T>> iterators =
            points.stream().map(Iterable::iterator).collect(toList());
        return ImmutableList.copyOf(Iterators.mergeSorted(iterators, Metric.comparator));
    }

    @RequiredArgsConstructor
    class Points implements CompositeCollection {
        private final Iterable<Point> points;
        private final long size;

        public Points(final List<Point> points) {
            this(points, points.size());
        }

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Point.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) points;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updatePoints(tags, series, points, size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.POINT;
        }

        @Override
        public Iterable<Point> data() {
            return points;
        }

        @Override
        public SortedCollection.Points sorted() {
            return new SortedCollection.Points(ORDERING.sortedCopy(points));
        }
    }

    @RequiredArgsConstructor
    class Events implements CompositeCollection {
        private final Iterable<Event> events;
        private final long size;

        public Events(final List<Event> events) {
            this(events, events.size());
        }

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Event.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) events;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateEvents(tags, series, events, size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.EVENT;
        }

        @Override
        public Iterable<Event> data() {
            return events;
        }

        @Override
        public SortedCollection.Events sorted() {
            return new SortedCollection.Events(ORDERING.sortedCopy(events));
        }
    }

    @RequiredArgsConstructor
    class Spreads implements CompositeCollection {
        private final Iterable<Spread> spreads;
        private final long size;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Spread.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) spreads;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateSpreads(tags, series, spreads, size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.SPREAD;
        }

        @Override
        public Iterable<Spread> data() {
            return spreads;
        }

        @Override
        public SortedCollection.Spreads sorted() {
            return new SortedCollection.Spreads(ORDERING.sortedCopy(spreads));
        }
    }

    @RequiredArgsConstructor
    class Groups implements CompositeCollection {
        private final Iterable<MetricGroup> groups;
        private final long size;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != MetricGroup.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) groups;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateGroup(tags, series, groups, size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.GROUP;
        }

        @Override
        public Iterable<MetricGroup> data() {
            return groups;
        }

        @Override
        public SortedCollection.Groups sorted() {
            return new SortedCollection.Groups(ORDERING.sortedCopy(groups));
        }
    }

    @RequiredArgsConstructor
    class Cardinality implements CompositeCollection {
        private final Iterable<Payload> payloads;
        private final long size;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Payload.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) payloads;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updatePayload(tags, series, payloads, size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.CARDINALITY;
        }

        @Override
        public Iterable<Payload> data() {
            return payloads;
        }

        @Override
        public SortedCollection.Cardinality sorted() {
            return new SortedCollection.Cardinality(ORDERING.sortedCopy(payloads));
        }
    }

    @RequiredArgsConstructor
    class PointsList implements CompositeCollection {
        private final List<Iterable<Point>> points;
        private final long size;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Point.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data();
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updatePoints(tags, series, Iterables.concat(points), size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.POINT;
        }

        @Override
        public Iterable<Point> data() {
            return Iterables.concat(points);
        }

        @Override
        public SortedCollection.Points sorted() {
            return new SortedCollection.Points(mergeSorted(points));
        }
    }

    @RequiredArgsConstructor
    class EventsList implements CompositeCollection {
        private final List<Iterable<Event>> events;
        private final long size;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Event.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data();
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateEvents(tags, series, Iterables.concat(events), size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.EVENT;
        }

        @Override
        public Iterable<Event> data() {
            return Iterables.concat(events);
        }

        @Override
        public SortedCollection.Events sorted() {
            return new SortedCollection.Events(mergeSorted(events));
        }
    }

    @RequiredArgsConstructor
    class SpreadsList implements CompositeCollection {
        private final List<Iterable<Spread>> spreads;
        private final long size;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Spread.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data();
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateSpreads(tags, series, Iterables.concat(spreads), size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.SPREAD;
        }

        @Override
        public Iterable<Spread> data() {
            return Iterables.concat(spreads);
        }

        @Override
        public SortedCollection.Spreads sorted() {
            return new SortedCollection.Spreads(mergeSorted(spreads));
        }
    }

    @RequiredArgsConstructor
    class GroupsList implements CompositeCollection {
        private final List<Iterable<MetricGroup>> groups;
        private final long size;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != MetricGroup.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data();
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateGroup(tags, series, Iterables.concat(groups), size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.GROUP;
        }

        @Override
        public Iterable<MetricGroup> data() {
            return Iterables.concat(groups);
        }

        @Override
        public SortedCollection.Groups sorted() {
            return new SortedCollection.Groups(mergeSorted(groups));
        }
    }

    @RequiredArgsConstructor
    class CardinalityList implements CompositeCollection {
        private final List<Iterable<Payload>> payloads;
        private final long size;

        @Override
        public <T> Iterable<T> dataAs(final Class<T> expected) {
            if (expected != Payload.class) {
                throw new IllegalArgumentException("expected");
            }

            return (Iterable<T>) data();
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updatePayload(tags, series, Iterables.concat(payloads), size);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public MetricType type() {
            return MetricType.CARDINALITY;
        }

        @Override
        public Iterable<Payload> data() {
            return Iterables.concat(payloads);
        }

        @Override
        public SortedCollection.Cardinality sorted() {
            return new SortedCollection.Cardinality(mergeSorted(payloads));
        }
    }
}
